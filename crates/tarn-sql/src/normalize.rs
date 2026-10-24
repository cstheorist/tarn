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
}

fn is_ident_char(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80
}

/// Keywords after which a literal must stay literal.
const KEEP_AFTER: &[&str] = &["INTERVAL", "COLLATE", "SET", "CHARSET", "_UTF8MB4", "_BINARY", "BINARY"];

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
    let push_param = |out: &mut String, n_params: &mut usize| {
        *n_params += 1;
        out.push('?');
        out.push_str(&n_params.to_string());
    };
    while i < n {
        let c = b[i];
        // comments
        if c == b'#' || (c == b'-' && i + 2 <= n && b.get(i + 1) == Some(&b'-') && b.get(i + 2).map_or(true, |x| x.is_ascii_whitespace())) {
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
        // quoted strings
        if c == b'\'' || c == b'"' {
            let q = c;
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
                    break;
                }
                val.push(d);
                j += 1;
            }
            let keep = mode == Mode::Placeholders
                || in_by
                || KEEP_AFTER.contains(&last_word.as_str())
                || (last_word == "SET" && prev_word == "CHARACTER");
            if keep {
                out.push_str(&sql[i..=j]);
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
            if mode == Mode::Literals {
                return Err(SqlError::parse("'?' placeholders are only allowed in prepared statements"));
            }
            push_param(&mut out, &mut n_params);
            i += 1;
            continue;
        }
        // numbers (not part of an identifier)
        let starts_number = c.is_ascii_digit() || (c == b'.' && b.get(i + 1).map_or(false, |d| d.is_ascii_digit()));
        // Identifier characters are consumed whole by the identifier branch, so a
        // digit seen here always starts a token.
        if starts_number {
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
            out.push_str(&sql[i..j]);
            prev_word = std::mem::take(&mut last_word);
            last_word = word;
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
        }
        // copy one UTF-8 character
        let len = utf8_len(c);
        out.push_str(&sql[i..(i + len).min(n)]);
        i += len;
    }
    Ok(Normalized { template: out.trim().to_string(), params, n_params })
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
        let t = if text.starts_with('.') { format!("0{}", text) } else { text.to_string() };
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
        assert_eq!(p, vec![Value::Int(1), Value::str("it's"), Value::Decimal("2.50".into()), Value::str("x\ny")]);
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
        assert_eq!(norm("SELECT * FROM t WHERE id = 1").0, norm("select * from t where id = 99").0.replace("select", "SELECT").replace("from", "FROM").replace("where", "WHERE"));
    }

    #[test]
    fn placeholders_numbered_in_prepare_mode() {
        let n = normalize("SELECT c FROM t WHERE id BETWEEN ? AND ? AND c <> 'a?b'", Mode::Placeholders).unwrap();
        assert_eq!(n.template, "SELECT c FROM t WHERE id BETWEEN ?1 AND ?2 AND c <> 'a?b'");
        assert_eq!(n.n_params, 2);
        assert!(normalize("SELECT ?", Mode::Literals).is_err());
    }

    #[test]
    fn comments_and_interval() {
        let (t, p) = norm("SELECT /* hi 5 */ NOW() - INTERVAL 1 DAY -- 7\n FROM dual");
        assert!(t.contains("INTERVAL 1 DAY") && !t.contains("hi 5") && !t.contains('7'), "{}", t);
        assert!(p.is_empty());
    }
}
