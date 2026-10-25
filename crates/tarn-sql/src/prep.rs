//! Text-level preparation of client SQL, before a statement is recognized,
//! normalized or parsed: MySQL executable comments and variable references.
//!
//! * `/*! ... */` and `/*!NNNNN ... */` are MySQL's executable comments:
//!   their content is SQL for servers whose version is at least NNNNN
//!   (mysqldump wraps `SET` statements, `ALTER TABLE ... DISABLE KEYS`,
//!   index options and more in them). Tarn reports MySQL 8.0.40, so it
//!   expands those up to 80040 and drops later ones, and drops MariaDB's
//!   `/*M! ... */`, as MySQL does.
//! * `@name` (user-defined variables) and `@@name` (system variables) are
//!   replaced by their values written as literals, so that the rest of the
//!   pipeline sees ordinary constants. Assignment targets of `SET` stay.

use bytes::Bytes;
use std::borrow::Cow;
use tarn_common::Value;

/// The version executable comments are compared with: MySQL 8.0.40.
pub const COMMENT_VERSION: u32 = 80040;

fn is_ident_byte(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80
}

/// End (exclusive) of the quoted token starting at `i` (`'...'`, `"..."`
/// or `` `...` ``), honouring doubled quotes and, except in backquotes,
/// backslash escapes. An unterminated token ends with the text.
fn quoted_end(b: &[u8], i: usize) -> usize {
    let q = b[i];
    let mut j = i + 1;
    while j < b.len() {
        if b[j] == b'\\' && q != b'`' {
            j += 2;
            continue;
        }
        if b[j] == q {
            if j + 1 < b.len() && b[j + 1] == q {
                j += 2;
                continue;
            }
            return j + 1;
        }
        j += 1;
    }
    b.len()
}

/// End (exclusive) of a `--` or `#` comment starting at `i`, if one starts
/// there (`--` must be followed by whitespace, a control character or the
/// end of the text, as in MySQL).
fn line_comment_end(b: &[u8], i: usize) -> Option<usize> {
    let starts =
        b[i] == b'#' || (b[i] == b'-' && b.get(i + 1) == Some(&b'-') && b.get(i + 2).is_none_or(|c| *c <= b' '));
    if !starts {
        return None;
    }
    let mut j = i;
    while j < b.len() && b[j] != b'\n' {
        j += 1;
    }
    Some(j)
}

/// Expand MySQL executable comments (see the module documentation).
pub fn expand_comments(sql: &str) -> Cow<'_, str> {
    if !sql.contains("/*!") && !sql.contains("/*M!") {
        return Cow::Borrowed(sql);
    }
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = String::with_capacity(n);
    let mut i = 0;
    let mut copied = 0;
    while i < n {
        let c = b[i];
        if c == b'\'' || c == b'"' || c == b'`' {
            i = quoted_end(b, i);
            continue;
        }
        if let Some(e) = line_comment_end(b, i) {
            i = e;
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let executable = b.get(i + 2) == Some(&b'!');
            let mariadb = b.get(i + 2) == Some(&b'M') && b.get(i + 3) == Some(&b'!');
            // The content: quoted text inside it may contain "*/".
            let body = i + if mariadb { 4 } else { 3 };
            let mut j = if executable || mariadb { body } else { i + 2 };
            let mut end = n;
            let mut close = n;
            while j < n {
                if (executable || mariadb) && (b[j] == b'\'' || b[j] == b'"' || b[j] == b'`') {
                    j = quoted_end(b, j);
                    continue;
                }
                if b[j] == b'*' && b.get(j + 1) == Some(&b'/') {
                    end = j;
                    close = j + 2;
                    break;
                }
                j += 1;
            }
            if !(executable || mariadb) {
                i = close;
                continue;
            }
            out.push_str(&sql[copied..i]);
            if executable {
                let digits = b[body..end].iter().take_while(|d| d.is_ascii_digit()).count();
                let (version, content) = if digits == 5 || digits == 6 {
                    (sql[body..body + digits].parse::<u32>().unwrap_or(0), body + digits)
                } else {
                    (0, body)
                };
                if version <= COMMENT_VERSION {
                    out.push(' ');
                    out.push_str(&sql[content..end]);
                }
            }
            out.push(' ');
            i = close;
            copied = i;
            continue;
        }
        i += 1;
    }
    out.push_str(&sql[copied.min(n)..]);
    Cow::Owned(out)
}

/// A value written as an SQL literal of the same type.
pub fn literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        Value::Float(f) => format!("{:e}", f),
        Value::Decimal(d) => d.clone(),
        Value::Str(s) => quote(s),
        Value::Bin(s) => {
            if s.is_empty() {
                "''".into()
            } else {
                let mut o = String::from("0x");
                for byte in s.iter() {
                    o.push_str(&format!("{:02X}", byte));
                }
                o
            }
        }
    }
}

fn quote(s: &Bytes) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    o.push('\'');
    for ch in String::from_utf8_lossy(s).chars() {
        match ch {
            '\'' => o.push_str("''"),
            '\\' => o.push_str("\\\\"),
            '\0' => o.push_str("\\0"),
            c => o.push(c),
        }
    }
    o.push('\'');
    o
}

/// The value of a quoted string literal `'...'` or `"..."` (the whole of
/// `tok`), with doubled quotes and backslash escapes resolved. None for
/// anything else.
pub fn string_literal(tok: &str) -> Option<String> {
    let b = tok.as_bytes();
    let q = *b.first()?;
    if (q != b'\'' && q != b'"') || quoted_end(b, 0) != b.len() || b.len() < 2 || b[b.len() - 1] != q {
        return None;
    }
    let mut out: Vec<u8> = Vec::with_capacity(b.len());
    let mut j = 1;
    while j < b.len() - 1 {
        match b[j] {
            b'\\' => {
                out.push(match b[j + 1] {
                    b'n' => b'\n',
                    b't' => b'\t',
                    b'r' => b'\r',
                    b'0' => 0,
                    b'b' => 8,
                    b'Z' => 26,
                    other => other,
                });
                j += 2;
            }
            c if c == q => {
                out.push(q);
                j += 2;
            }
            c => {
                out.push(c);
                j += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

/// The `?` parameter markers of a statement's text (outside literals and
/// comments).
pub fn count_markers(sql: &str) -> usize {
    let b = sql.as_bytes();
    let mut i = 0;
    let mut n = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'\'' || c == b'"' || c == b'`' {
            i = quoted_end(b, i);
            continue;
        }
        if let Some(e) = line_comment_end(b, i) {
            i = e;
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let mut j = i + 2;
            while j + 1 < b.len() && !(b[j] == b'*' && b[j + 1] == b'/') {
                j += 1;
            }
            i = (j + 2).min(b.len());
            continue;
        }
        n += (c == b'?') as usize;
        i += 1;
    }
    n
}

/// Replace the `?` markers, in order, by `args` (SQL literal text).
pub fn bind_markers(sql: &str, args: &[String]) -> String {
    let b = sql.as_bytes();
    let mut out = String::with_capacity(sql.len() + 16);
    let mut i = 0;
    let mut copied = 0;
    let mut k = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'\'' || c == b'"' || c == b'`' {
            i = quoted_end(b, i);
            continue;
        }
        if let Some(e) = line_comment_end(b, i) {
            i = e;
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let mut j = i + 2;
            while j + 1 < b.len() && !(b[j] == b'*' && b[j + 1] == b'/') {
                j += 1;
            }
            i = (j + 2).min(b.len());
            continue;
        }
        if c == b'?' && k < args.len() {
            out.push_str(&sql[copied..i]);
            out.push_str(&args[k]);
            k += 1;
            copied = i + 1;
        }
        i += 1;
    }
    out.push_str(&sql[copied..]);
    out
}

/// A variable reference found in a statement.
#[derive(Debug, Clone, PartialEq)]
pub enum VarRef {
    /// `@name` (lower-cased)
    User(String),
    /// `@@[scope.]name` as written, without the `@@`
    System(String),
}

/// Do variable references (outside literals and comments) occur?
pub fn has_var_refs(sql: &str) -> bool {
    if !sql.contains('@') {
        return false;
    }
    let mut found = false;
    scan_vars(sql, false, &mut |_| {
        found = true;
        None
    });
    found
}

/// Replace variable references by `lookup`'s literal text; references
/// `lookup` declines (None) stay. With `keep_targets`, the variables a SET
/// statement assigns (`SET @a = ..., @@x = ...`) stay. A select-list item
/// that is only a variable keeps its name as the column name (`SELECT @a`
/// returns a column named `@a`). Returns None when nothing was replaced.
pub fn substitute_vars(
    sql: &str,
    keep_targets: bool,
    lookup: &mut dyn FnMut(&VarRef) -> Option<String>,
) -> Option<String> {
    let (out, changed) = scan_vars(sql, keep_targets, lookup);
    changed.then_some(out)
}

fn scan_vars(sql: &str, keep_targets: bool, lookup: &mut dyn FnMut(&VarRef) -> Option<String>) -> (String, bool) {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = String::with_capacity(n + 16);
    let mut changed = false;
    let mut i = 0;
    let mut copied = 0;
    let mut depth = 0i32;
    // Last significant token at depth 0 (upper-cased word, or punctuation).
    let mut prev = String::new();
    let mut first_word: Option<String> = None;
    // In the select list of a top-level SELECT (between SELECT and FROM).
    let mut in_select_list = false;
    while i < n {
        let c = b[i];
        if c == b'\'' || c == b'"' || c == b'`' {
            i = quoted_end(b, i);
            if depth == 0 {
                prev = "lit".into();
            }
            continue;
        }
        if let Some(e) = line_comment_end(b, i) {
            i = e;
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let mut j = i + 2;
            while j + 1 < n && !(b[j] == b'*' && b[j + 1] == b'/') {
                j += 1;
            }
            i = (j + 2).min(n);
            continue;
        }
        if c == b'@' {
            let start = i;
            let system = b.get(i + 1) == Some(&b'@');
            let mut j = i + if system { 2 } else { 1 };
            let name: String;
            if !system && j < n && (b[j] == b'\'' || b[j] == b'"' || b[j] == b'`') {
                let e = quoted_end(b, j);
                let inner = &sql[j + 1..e.saturating_sub(1).max(j + 1)];
                let q = b[j] as char;
                name = inner.replace(&format!("{q}{q}"), &q.to_string());
                j = e;
            } else {
                let s = j;
                while j < n && (is_ident_byte(b[j]) || b[j] == b'.') {
                    j += 1;
                }
                name = sql[s..j].to_string();
            }
            if name.is_empty() {
                i = j.max(i + 1);
                continue;
            }
            // `SET @a = ...`: an assignment target when followed by = or :=
            // and preceded by SET, a scope keyword or a comma.
            let mut k = j;
            while k < n && b[k].is_ascii_whitespace() {
                k += 1;
            }
            let assigns = b.get(k) == Some(&b'=') || (b.get(k) == Some(&b':') && b.get(k + 1) == Some(&b'='));
            let target = keep_targets
                && depth == 0
                && assigns
                && matches!(prev.as_str(), "SET" | "," | "GLOBAL" | "SESSION" | "LOCAL" | "PERSIST");
            if !target {
                let r = if system {
                    VarRef::System(name.clone())
                } else {
                    VarRef::User(name.to_ascii_lowercase())
                };
                if let Some(lit) = lookup(&r) {
                    out.push_str(&sql[copied..start]);
                    // A bare select-list item keeps the variable as its name.
                    let mut m = j;
                    while m < n && b[m].is_ascii_whitespace() {
                        m += 1;
                    }
                    let next_word: String = b[m..]
                        .iter()
                        .take_while(|x| is_ident_byte(**x))
                        .map(|x| (*x as char).to_ascii_uppercase())
                        .collect();
                    let item_ends = m >= n || b[m] == b',' || b[m] == b';' || next_word == "FROM";
                    if in_select_list && depth == 0 && matches!(prev.as_str(), "SELECT" | ",") && item_ends {
                        let shown = &sql[start..j];
                        out.push_str(&format!("{} AS `{}`", lit, shown.replace('`', "``")));
                    } else {
                        out.push_str(&lit);
                    }
                    copied = j;
                    changed = true;
                }
            }
            if depth == 0 {
                prev = "var".into();
            }
            i = j;
            continue;
        }
        if is_ident_byte(c) {
            let s = i;
            while i < n && is_ident_byte(b[i]) {
                i += 1;
            }
            let w = sql[s..i].to_ascii_uppercase();
            if first_word.is_none() {
                first_word = Some(w.clone());
            }
            if depth == 0 {
                if w == "SELECT" {
                    in_select_list = true;
                } else if matches!(w.as_str(), "FROM" | "INTO" | "WHERE" | "UNION") {
                    in_select_list = false;
                }
                prev = w;
            }
            continue;
        }
        match c {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        if depth == 0 && !c.is_ascii_whitespace() && c != b'(' && c != b')' {
            prev = (c as char).to_string();
        }
        i += 1;
    }
    out.push_str(&sql[copied.min(n)..]);
    let _ = first_word;
    (out, changed)
}

/// `SELECT ... INTO @a, @b ...` (before FROM or at the end): the statement
/// without its INTO clause, and the variables (lower-cased) it assigns.
/// None for any other statement, including `INTO OUTFILE` and `INSERT INTO`.
pub fn split_select_into(sql: &str) -> Option<(String, Vec<String>)> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut i = 0;
    let mut depth = 0i32;
    let mut first = true;
    while i < n {
        let c = b[i];
        if c == b'\'' || c == b'"' || c == b'`' {
            i = quoted_end(b, i);
            continue;
        }
        if let Some(e) = line_comment_end(b, i) {
            i = e;
            continue;
        }
        if c == b'/' && b.get(i + 1) == Some(&b'*') {
            let mut j = i + 2;
            while j + 1 < n && !(b[j] == b'*' && b[j + 1] == b'/') {
                j += 1;
            }
            i = (j + 2).min(n);
            continue;
        }
        if is_ident_byte(c) {
            let s = i;
            while i < n && is_ident_byte(b[i]) {
                i += 1;
            }
            let w = &sql[s..i];
            if first {
                if !w.eq_ignore_ascii_case("SELECT") {
                    return None;
                }
                first = false;
            } else if depth == 0 && w.eq_ignore_ascii_case("INTO") {
                // @var [, @var]...
                let mut vars = Vec::new();
                let mut j = i;
                loop {
                    while j < n && b[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    if b.get(j) != Some(&b'@') || b.get(j + 1) == Some(&b'@') {
                        return None;
                    }
                    j += 1;
                    let name = if j < n && (b[j] == b'\'' || b[j] == b'"' || b[j] == b'`') {
                        let e = quoted_end(b, j);
                        let v = sql[j + 1..e.saturating_sub(1).max(j + 1)].to_string();
                        j = e;
                        v
                    } else {
                        let s = j;
                        while j < n && (is_ident_byte(b[j]) || b[j] == b'.') {
                            j += 1;
                        }
                        sql[s..j].to_string()
                    };
                    if name.is_empty() {
                        return None;
                    }
                    vars.push(name.to_ascii_lowercase());
                    let mut k = j;
                    while k < n && b[k].is_ascii_whitespace() {
                        k += 1;
                    }
                    if b.get(k) == Some(&b',') {
                        j = k + 1;
                        continue;
                    }
                    break;
                }
                let rest = format!("{} {}", &sql[..s], &sql[j..]);
                return Some((rest, vars));
            }
            continue;
        }
        match c {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_into_variables() {
        let s =
            |sql: &str| split_select_into(sql).map(|(q, v)| (q.split_whitespace().collect::<Vec<_>>().join(" "), v));
        assert_eq!(
            s("SELECT COUNT(*) INTO @Has FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'x'"),
            Some((
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'x'".into(),
                vec!["has".into()]
            ))
        );
        assert_eq!(
            s("SELECT a, b FROM t WHERE id = 1 INTO @a, @`b c`"),
            Some(("SELECT a, b FROM t WHERE id = 1".into(), vec!["a".into(), "b c".into()]))
        );
        assert_eq!(s("SELECT 'INTO @x' AS v"), None);
        assert_eq!(s("SELECT (SELECT 1 INTO @x)"), None);
        assert_eq!(s("INSERT INTO t VALUES (@x)"), None);
        assert_eq!(s("SELECT * INTO OUTFILE '/tmp/x' FROM t"), None);
        assert_eq!(s("SELECT 1 INTO @@x"), None);
    }

    #[test]
    fn statement_text() {
        assert_eq!(
            string_literal("'SELECT ''a'', \\'b\\''").as_deref(),
            Some("SELECT 'a', 'b'")
        );
        assert_eq!(string_literal("\"x\"").as_deref(), Some("x"));
        assert_eq!(string_literal("'open"), None);
        assert_eq!(string_literal("x"), None);
        let s = "SELECT ? + 1, '?', `?` /* ? */ FROM t WHERE a = ? -- ?\n";
        assert_eq!(count_markers(s), 2);
        assert_eq!(
            bind_markers(s, &["5".into(), "'x'".into()]),
            "SELECT 5 + 1, '?', `?` /* ? */ FROM t WHERE a = 'x' -- ?\n"
        );
    }

    #[test]
    fn executable_comments() {
        assert_eq!(expand_comments("SELECT 1"), "SELECT 1");
        let e = expand_comments("/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE */");
        assert_eq!(e.trim(), "SET @OLD_SQL_MODE=@@SQL_MODE");
        let e = expand_comments("CREATE TABLE t (a INT, KEY k (a) /*!80000 INVISIBLE */)");
        assert_eq!(
            e.split_whitespace().collect::<Vec<_>>().join(" "),
            "CREATE TABLE t (a INT, KEY k (a) INVISIBLE )"
        );
        // later versions and MariaDB-only comments disappear
        let e = expand_comments("SELECT 1 /*!90000 + 1 */ /*M!100100 + 2 */ /*!100500 + 3 */");
        assert_eq!(e.split_whitespace().collect::<Vec<_>>().join(" "), "SELECT 1");
        // no version: always; 6-digit versions
        let e = expand_comments("SELECT /*! 2 */, /*!080040 3 */, /*!080041 4 */");
        assert_eq!(e.split_whitespace().collect::<Vec<_>>().join(" "), "SELECT 2 , 3 ,");
        // not inside strings, backquotes or line comments; quotes inside
        let s = "SELECT '/*!40101 x */', `/*!1*/` -- /*!40101 y */\n";
        assert_eq!(expand_comments(s), s);
        let e = expand_comments("SELECT /*!40101 '*/' */ AS c");
        assert_eq!(e.split_whitespace().collect::<Vec<_>>().join(" "), "SELECT '*/' AS c");
        // ordinary comments are left alone
        let e = expand_comments("SELECT /* a */ 1 /*!50001 , 2 */");
        assert_eq!(
            e.split_whitespace().collect::<Vec<_>>().join(" "),
            "SELECT /* a */ 1 , 2"
        );
    }

    fn subst(sql: &str, keep: bool) -> String {
        substitute_vars(sql, keep, &mut |r| match r {
            VarRef::User(n) if n == "a" => Some("5".into()),
            VarRef::User(n) if n == "s" => Some(literal(&Value::str("it's"))),
            VarRef::User(_) => Some("NULL".into()),
            VarRef::System(n) if n.eq_ignore_ascii_case("sql_mode") => Some("'STRICT'".into()),
            VarRef::System(_) => None,
        })
        .unwrap_or_else(|| sql.to_string())
    }

    #[test]
    fn variables() {
        assert_eq!(subst("SELECT @a", false), "SELECT 5 AS `@a`");
        assert_eq!(
            subst("SELECT @a, @A + 1, @s FROM t", false),
            "SELECT 5 AS `@a`, 5 + 1, 'it''s' AS `@s` FROM t"
        );
        assert_eq!(
            subst("SELECT c FROM t WHERE id = @a", false),
            "SELECT c FROM t WHERE id = 5"
        );
        assert_eq!(
            subst("SELECT 'x@a', `@a`, @'a' AS v", false),
            "SELECT 'x@a', `@a`, 5 AS v"
        );
        assert_eq!(
            subst("SET @x = @a + 1, @@session.sql_mode = @@sql_mode", true),
            "SET @x = 5 + 1, @@session.sql_mode = 'STRICT'"
        );
        assert_eq!(
            subst("SET @OLD = @@SQL_MODE, SQL_MODE = 'X'", true),
            "SET @OLD = 'STRICT', SQL_MODE = 'X'"
        );
        assert_eq!(subst("SET sql_mode = @nothing", true), "SET sql_mode = NULL");
        assert!(substitute_vars("SELECT 1", false, &mut |_| Some("x".into())).is_none());
        assert!(has_var_refs("SELECT @a"));
        assert!(!has_var_refs("SELECT 'a@b'"));
        assert_eq!(literal(&Value::Float(1.5)), "1.5e0");
        assert_eq!(literal(&Value::Bin(Bytes::from_static(b"\x01\xff"))), "0x01FF");
    }
}
