//! SQL text normalization for the plan cache.
//!
//! `normalize(sql, Mode::Literals)` replaces every numeric and string literal
//! with a numbered placeholder `?N` and returns the literals, so that
//! `SELECT c FROM t WHERE id = 42` and `... id = 7` share one cached plan
//! (`... id = ?1`). `Mode::Placeholders` (COM_STMT_PREPARE) keeps literals
//! and only numbers the client's `?` markers in textual order.
//!
//! Soundness requirement for caching: re-substituting the extracted literals
//! into the template must give back the original statement's meaning. The
//! normalizer therefore leaves literals alone wherever a literal is not a
//! value: positional `ORDER BY 1` / `GROUP BY 2`, and after keywords that take
//! a literal as syntax (`INTERVAL`, `COLLATE`, `CHARACTER SET` ...).
//!
//! A hexadecimal literal `X'4142'` becomes a binary parameter, as does a
//! string with the `_binary` introducer (`_binary 'AB'`, `_binary X'4142'`);
//! one with a UTF-8 introducer (`_utf8mb4 'ab'`), or a national string
//! (`N'ab'`), a text parameter. Plans are cached by the kinds of their
//! parameters too (`Session::plan_cached`): a binary value compares and is
//! typed as a binary string, a text one under the connection's collation.
//! The literals whose text MariaDB names a column by (`SELECT X'41'` is a
//! column `X'41'`) keep it in `Normalized::names`.
//!
//! It also rewrites MySQL's older `LOCK IN SHARE MODE` to `FOR SHARE`, which
//! the parser knows.

use bytes::Bytes;
use tarn_common::{SqlError, SqlResult, Value};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    /// COM_QUERY: turn literals into parameters.
    Literals,
    /// COM_STMT_PREPARE: number the client's `?` placeholders only.
    Placeholders,
}

#[derive(Debug, Clone)]
pub struct Normalized {
    pub template: String,
    pub params: Vec<Value>,
    /// Number of placeholders in the template.
    pub n_params: usize,
    /// The parameters made from literals whose text names a column.
    pub names: Vec<LitName>,
}

/// A parameter made from a hexadecimal literal or a string with an
/// introducer: MariaDB names a column of it by its text (`X'41'`,
/// `HEX(_binary'ab')`), or, for a string alone in a column, by its value
/// (`_binary'ab'` is a column `ab`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LitName {
    /// The parameter (0 for `?1`).
    pub param: usize,
    /// The literal as written, with its introducer.
    pub text: String,
    /// A column of the literal alone is named by `text`, not by its value.
    pub bare_text: bool,
}

fn is_ident_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80
}

/// Keywords after which a literal must stay literal.
const KEEP_AFTER: &[&str] = &["INTERVAL", "COLLATE", "SET", "CHARSET", "BINARY"];

/// A character set introducer of a string literal: whether its strings are
/// binary.
fn introducer(word: &str) -> Option<bool> {
    match word {
        "_BINARY" => Some(true),
        "_UTF8MB4" | "_UTF8MB3" | "_UTF8" => Some(false),
        _ => None,
    }
}

/// The quoted string whose opening quote is at `b[i]`: its value, with
/// MySQL's escapes, and where its closing quote is.
fn scan_string(b: &[u8], i: usize) -> SqlResult<(Vec<u8>, usize)> {
    let q = b[i];
    let n = b.len();
    let mut j = i + 1;
    let mut val: Vec<u8> = Vec::new();
    loop {
        if j >= n {
            return Err(SqlError::parse("unterminated string literal"));
        }
        let d = b[j];
        if d == b'\\' && j + 1 < n {
            let e = b[j + 1];
            val.push(match e {
                b'n' => b'\n',
                b't' => b'\t',
                b'r' => b'\r',
                b'0' => 0,
                b'b' => 8,
                b'Z' => 26,
                other => other,
            });
            j += 2;
            continue;
        }
        if d == q {
            if j + 1 < n && b[j + 1] == q {
                val.push(q);
                j += 2;
                continue;
            }
            return Ok((val, j));
        }
        val.push(d);
        j += 1;
    }
}

/// The hexadecimal literal `X'...'` that starts at `sql[start]`, whose
/// quote is at `i`: its bytes, and where its closing quote is. As in
/// MariaDB, an odd number of digits, or anything else than digits, is a
/// syntax error.
fn scan_hex(sql: &str, start: usize, i: usize) -> SqlResult<(Vec<u8>, usize)> {
    let b = sql.as_bytes();
    let end = match b[i + 1..].iter().position(|&c| c == b'\'') {
        Some(k) => i + 1 + k,
        None => return Err(SqlError::parse("unterminated string literal")),
    };
    let digits = &b[i + 1..end];
    if !digits.len().is_multiple_of(2) || !digits.iter().all(u8::is_ascii_hexdigit) {
        return Err(SqlError::parse(format!(
            "invalid hexadecimal literal near '{}'",
            &sql[start..=end]
        )));
    }
    let v = |c: u8| (c as char).to_digit(16).unwrap_or(0) as u8;
    Ok((digits.chunks(2).map(|p| (v(p[0]) << 4) | v(p[1])).collect(), end))
}

pub fn normalize(sql: &str, mode: Mode) -> SqlResult<Normalized> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = String::with_capacity(n + 8);
    let mut params = Vec::new();
    let mut n_params = 0usize;
    let mut i = 0usize;
    // Context tracking for ORDER BY / GROUP BY positional literals.
    let mut last_word = String::new();
    let mut prev_word = String::new();
    let mut in_by = false;
    let mut paren_depth: i32 = 0;
    let mut by_depth: i32 = 0;
    // The keywords just emitted (with their offsets in `out`) since the last
    // other token, for the LOCK IN SHARE MODE rewrite.
    let mut recent: Vec<(String, usize)> = Vec::new();
    // Where `last_word` starts in `sql`: an introducer's text names a column.
    let mut last_src = 0usize;
    let mut names: Vec<LitName> = Vec::new();
    // A literal that stays literal: in a prepared statement, a position in
    // ORDER BY / GROUP BY, or after a keyword that takes one as syntax.
    let keeps = |in_by: bool, last_word: &str, prev_word: &str| {
        mode == Mode::Placeholders
            || in_by
            || KEEP_AFTER.contains(&last_word)
            || (last_word == "SET" && prev_word == "CHARACTER")
    };
    // The introducer just before a literal (`_binary 'x'`): whether it makes
    // a binary string, and where it starts in `out` (it goes with the value).
    let intro_of = |recent: &[(String, usize)], last_word: &str| {
        let bin = introducer(last_word)?;
        recent.last().filter(|(w, _)| w == last_word).map(|&(_, at)| (bin, at))
    };
    let push_param = |out: &mut String, n_params: &mut usize| {
        *n_params += 1;
        out.push('?');
        out.push_str(&n_params.to_string());
    };
    while i < n {
        let c = b[i];
        // comments
        if c == b'#'
            || (c == b'-'
                && i + 2 <= n
                && b.get(i + 1) == Some(&b'-')
                && b.get(i + 2).is_none_or(|x| x.is_ascii_whitespace()))
        {
            while i < n && b[i] != b'\n' {
                i += 1;
            }
            out.push(' ');
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            // MySQL executable comments /*! ... */ are kept verbatim.
            let start = i;
            i += 2;
            while i + 1 < n && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(n);
            if b.get(start + 2) == Some(&b'!') {
                out.push_str(&sql[start..i]);
            } else {
                out.push(' ');
            }
            continue;
        }
        // quoted strings, with an introducer (`_binary 'x'`) or not
        if c == b'\'' || c == b'"' {
            let (val, j) = scan_string(b, i)?;
            let keep = keeps(in_by, &last_word, &prev_word);
            let intro = if keep { None } else { intro_of(&recent, &last_word) };
            recent.clear();
            if keep {
                out.push_str(&sql[i..=j]);
            } else if let Some((bin, at)) = intro {
                out.truncate(at);
                push_param(&mut out, &mut n_params);
                names.push(LitName {
                    param: params.len(),
                    text: sql[last_src..=j].to_string(),
                    bare_text: false,
                });
                params.push(if bin {
                    Value::Bin(Bytes::from(val))
                } else {
                    Value::Str(Bytes::from(val))
                });
            } else {
                push_param(&mut out, &mut n_params);
                params.push(Value::Str(Bytes::from(val)));
            }
            i = j + 1;
            last_word.clear();
            continue;
        }
        // backquoted identifiers
        if c == b'`' {
            recent.clear();
            let mut j = i + 1;
            while j < n && b[j] != b'`' {
                j += 1;
            }
            out.push_str(&sql[i..(j + 1).min(n)]);
            i = j + 1;
            prev_word = std::mem::take(&mut last_word);
            last_word = "`".into();
            continue;
        }
        // placeholders
        if c == b'?' {
            recent.clear();
            if mode == Mode::Literals {
                return Err(SqlError::parse(
                    "'?' placeholders are only allowed in prepared statements",
                ));
            }
            push_param(&mut out, &mut n_params);
            i += 1;
            continue;
        }
        // numbers (not part of an identifier)
        let starts_number = c.is_ascii_digit() || (c == b'.' && b.get(i + 1).is_some_and(|d| d.is_ascii_digit()));
        // Identifier characters are consumed whole by the identifier branch, so a
        // digit seen here always starts a token.
        if starts_number {
            recent.clear();
            let mut j = i;
            let hex = c == b'0' && matches!(b.get(i + 1), Some(b'x') | Some(b'X'));
            if hex {
                j += 2;
                while j < n && b[j].is_ascii_hexdigit() {
                    j += 1;
                }
            } else {
                while j < n && b[j].is_ascii_digit() {
                    j += 1;
                }
                if j < n && b[j] == b'.' {
                    j += 1;
                    while j < n && b[j].is_ascii_digit() {
                        j += 1;
                    }
                }
                if j < n && (b[j] == b'e' || b[j] == b'E') {
                    let mut k = j + 1;
                    if k < n && (b[k] == b'+' || b[k] == b'-') {
                        k += 1;
                    }
                    if k < n && b[k].is_ascii_digit() {
                        while k < n && b[k].is_ascii_digit() {
                            k += 1;
                        }
                        j = k;
                    }
                }
            }
            // a number glued to identifier characters is an identifier (e.g. 1abc)
            if j < n && is_ident_char(b[j]) && !hex {
                let mut k = j;
                while k < n && is_ident_char(b[k]) {
                    k += 1;
                }
                out.push_str(&sql[i..k]);
                prev_word = std::mem::take(&mut last_word);
                last_word = sql[i..k].to_ascii_uppercase();
                i = k;
                continue;
            }
            let text = &sql[i..j];
            let keep = mode == Mode::Placeholders || in_by || KEEP_AFTER.contains(&last_word.as_str()) || hex;
            if keep {
                out.push_str(text);
            } else {
                push_param(&mut out, &mut n_params);
                params.push(number_value(text));
            }
            i = j;
            last_word.clear();
            continue;
        }
        // identifiers / keywords
        if is_ident_char(c) {
            let mut j = i;
            while j < n && is_ident_char(b[j]) {
                j += 1;
            }
            let word = sql[i..j].to_ascii_uppercase();
            // a literal glued to its prefix: X'4142' (hexadecimal, binary)
            // or N'ab' (national)
            if b.get(j) == Some(&b'\'') && (word == "X" || word == "N") {
                let hex = word == "X";
                let (val, end) = if hex { scan_hex(sql, i, j)? } else { scan_string(b, j)? };
                let keep = keeps(in_by, &last_word, &prev_word);
                // (a hexadecimal one may have an introducer: `_binary X'41'`)
                let intro = if keep || !hex {
                    None
                } else {
                    intro_of(&recent, &last_word)
                };
                recent.clear();
                if keep {
                    out.push_str(&sql[i..=end]);
                } else {
                    let start = match intro {
                        Some((_, at)) => {
                            out.truncate(at);
                            last_src
                        }
                        None => i,
                    };
                    push_param(&mut out, &mut n_params);
                    names.push(LitName {
                        param: params.len(),
                        text: sql[start..=end].to_string(),
                        bare_text: hex,
                    });
                    let text = !hex || intro.is_some_and(|(bin, _)| !bin);
                    params.push(if text {
                        Value::Str(Bytes::from(val))
                    } else {
                        Value::Bin(Bytes::from(val))
                    });
                }
                i = end + 1;
                last_word.clear();
                continue;
            }
            if word == "BY" && (last_word == "ORDER" || last_word == "GROUP") {
                in_by = true;
                by_depth = paren_depth;
            } else if in_by
                && matches!(
                    word.as_str(),
                    "LIMIT" | "HAVING" | "WINDOW" | "UNION" | "FOR" | "LOCK" | "INTO" | "OFFSET" | "PROCEDURE"
                )
            {
                in_by = false;
            }
            if word == "MODE"
                && recent.len() >= 3
                && recent[recent.len() - 3..]
                    .iter()
                    .map(|(w, _)| w.as_str())
                    .eq(["LOCK", "IN", "SHARE"])
            {
                out.truncate(recent[recent.len() - 3].1);
                out.push_str("FOR SHARE");
                recent.clear();
                prev_word = "FOR".into();
                last_word = "SHARE".into();
                i = j;
                continue;
            }
            recent.push((word.clone(), out.len()));
            out.push_str(&sql[i..j]);
            prev_word = std::mem::take(&mut last_word);
            last_word = word;
            last_src = i;
            i = j;
            continue;
        }
        match c {
            b'(' => paren_depth += 1,
            b')' => {
                paren_depth -= 1;
                if in_by && paren_depth < by_depth {
                    in_by = false;
                }
            }
            b';' => in_by = false,
            _ => {}
        }
        if !c.is_ascii_whitespace() {
            last_word.clear();
            recent.clear();
        }
        // copy one UTF-8 character
        let len = utf8_len(c);
        out.push_str(&sql[i..(i + len).min(n)]);
        i += len;
    }
    Ok(Normalized {
        template: out.trim().to_string(),
        params,
        n_params,
        names,
    })
}

fn utf8_len(c: u8) -> usize {
    match c {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}

fn number_value(text: &str) -> Value {
    if text.contains(['.', 'e', 'E']) {
        if text.contains(['e', 'E']) {
            return Value::Float(text.parse().unwrap_or(0.0));
        }
        let t = if text.starts_with('.') {
            format!("0{}", text)
        } else {
            text.to_string()
        };
        let t = if t.ends_with('.') { format!("{}0", t) } else { t };
        return Value::Decimal(t);
    }
    if let Ok(i) = text.parse::<i64>() {
        return Value::Int(i);
    }
    if let Ok(u) = text.parse::<u64>() {
        return Value::UInt(u);
    }
    Value::Decimal(text.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm(s: &str) -> (String, Vec<Value>) {
        let n = normalize(s, Mode::Literals).unwrap();
        (n.template, n.params)
    }

    #[test]
    fn literals_become_params() {
        let (t, p) = norm("SELECT c FROM sbtest1 WHERE id=42");
        assert_eq!(t, "SELECT c FROM sbtest1 WHERE id=?1");
        assert_eq!(p, vec![Value::Int(42)]);
        let (t, p) = norm("INSERT INTO t VALUES (1, 'it''s', -2.50, \"x\\ny\")");
        assert_eq!(t, "INSERT INTO t VALUES (?1, ?2, -?3, ?4)");
        assert_eq!(
            p,
            vec![
                Value::Int(1),
                Value::str("it's"),
                Value::Decimal("2.50".into()),
                Value::str("x\ny")
            ]
        );
    }

    #[test]
    fn identifiers_with_digits_untouched() {
        let (t, _) = norm("SELECT t2.c1 FROM sbtest10 t2 WHERE t2.k1 = 5");
        assert_eq!(t, "SELECT t2.c1 FROM sbtest10 t2 WHERE t2.k1 = ?1");
    }

    #[test]
    fn positional_order_by_kept() {
        let (t, p) = norm("SELECT a, b FROM t WHERE a > 3 GROUP BY 1 ORDER BY 2 DESC LIMIT 10");
        assert_eq!(t, "SELECT a, b FROM t WHERE a > ?1 GROUP BY 1 ORDER BY 2 DESC LIMIT ?2");
        assert_eq!(p, vec![Value::Int(3), Value::Int(10)]);
    }

    #[test]
    fn same_template_for_different_literals() {
        assert_eq!(
            norm("SELECT * FROM t WHERE id = 1").0,
            norm("select * from t where id = 99")
                .0
                .replace("select", "SELECT")
                .replace("from", "FROM")
                .replace("where", "WHERE")
        );
    }

    #[test]
    fn placeholders_numbered_in_prepare_mode() {
        let n = normalize(
            "SELECT c FROM t WHERE id BETWEEN ? AND ? AND c <> 'a?b'",
            Mode::Placeholders,
        )
        .unwrap();
        assert_eq!(n.template, "SELECT c FROM t WHERE id BETWEEN ?1 AND ?2 AND c <> 'a?b'");
        assert_eq!(n.n_params, 2);
        assert!(normalize("SELECT ?", Mode::Literals).is_err());
        let n = normalize(
            "SELECT a FROM t WHERE b = 'lock in share mode' LOCK  IN\nSHARE MODE",
            Mode::Literals,
        )
        .unwrap();
        assert_eq!(n.template, "SELECT a FROM t WHERE b = ?1 FOR SHARE");
        let n = normalize(
            "SELECT a FROM t WHERE b = 'LOCK IN SHARE MODE' lock in share mode",
            Mode::Placeholders,
        )
        .unwrap();
        assert_eq!(n.template, "SELECT a FROM t WHERE b = 'LOCK IN SHARE MODE' FOR SHARE");
    }

    fn bin(b: &[u8]) -> Value {
        Value::Bin(Bytes::copy_from_slice(b))
    }

    fn lit(param: usize, text: &str, bare_text: bool) -> LitName {
        LitName {
            param,
            text: text.into(),
            bare_text,
        }
    }

    #[test]
    fn hexadecimal_literals_become_binary_params() {
        // PyMySQL 1.2.3 sends bytes as X'...' (#122)
        let n = normalize("INSERT INTO t VALUES (1, X'51554a44', x'00FF', X'')", Mode::Literals).unwrap();
        assert_eq!(n.template, "INSERT INTO t VALUES (?1, ?2, ?3, ?4)");
        assert_eq!(n.params, vec![Value::Int(1), bin(b"QUJD"), bin(&[0, 0xff]), bin(b"")]);
        assert_eq!(
            n.names,
            vec![
                lit(1, "X'51554a44'", true),
                lit(2, "x'00FF'", true),
                lit(3, "X''", true)
            ]
        );
        // as MariaDB: an odd number of digits, or another character, is a
        // syntax error; and so in a prepared statement
        for bad in ["SELECT X'4'", "SELECT X'4G'", "SELECT x'0 1'", "SELECT X'41"] {
            for mode in [Mode::Literals, Mode::Placeholders] {
                assert_eq!(normalize(bad, mode).unwrap_err().code, 1064, "{} {:?}", bad, mode);
            }
        }
        // a prepared statement keeps it; so does a word ending in x, or an x
        // apart from its string
        let n = normalize("SELECT X'41' FROM t WHERE c = ?", Mode::Placeholders).unwrap();
        assert_eq!(n.template, "SELECT X'41' FROM t WHERE c = ?1");
        let (t, p) = norm("SELECT x, ax FROM t WHERE ax = 'X''41' AND x = 'b'");
        assert_eq!(t, "SELECT x, ax FROM t WHERE ax = ?1 AND x = ?2");
        assert_eq!(p, vec![Value::str("X'41"), Value::str("b")]);
    }

    #[test]
    fn introducers_and_national_strings() {
        let n = normalize(
            "SELECT _binary 'q''r', _binary X'0102', _utf8mb4'ab', N'cd', HEX(_binary'xy') FROM t",
            Mode::Literals,
        )
        .unwrap();
        assert_eq!(n.template, "SELECT ?1, ?2, ?3, ?4, HEX(?5) FROM t");
        assert_eq!(
            n.params,
            vec![
                bin(b"q'r"),
                bin(&[1, 2]),
                Value::str("ab"),
                Value::str("cd"),
                bin(b"xy")
            ]
        );
        assert_eq!(
            n.names,
            vec![
                lit(0, "_binary 'q''r'", false),
                lit(1, "_binary X'0102'", true),
                lit(2, "_utf8mb4'ab'", false),
                lit(3, "N'cd'", false),
                lit(4, "_binary'xy'", false),
            ]
        );
        // kept where literals are syntax, and in prepared statements
        let n = normalize("SELECT _binary'ab', N'cd' FROM t WHERE c = ?", Mode::Placeholders).unwrap();
        assert_eq!(n.template, "SELECT _binary'ab', N'cd' FROM t WHERE c = ?1");
        assert!(n.names.is_empty());
    }

    #[test]
    fn comments_and_interval() {
        let (t, p) = norm("SELECT /* hi 5 */ NOW() - INTERVAL 1 DAY -- 7\n FROM dual");
        assert!(
            t.contains("INTERVAL 1 DAY") && !t.contains("hi 5") && !t.contains('7'),
            "{}",
            t
        );
        assert!(p.is_empty());
    }
}
