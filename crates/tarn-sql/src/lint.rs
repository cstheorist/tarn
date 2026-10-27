//! Declared table trees (issue #85).
//!
//! A table whose `COMMENT` holds `tarn:parent=<table>` declares that its rows
//! nest under the parent's: its primary key starts with the parent's whole
//! primary key, and every table of the tree is sharded by the root's key
//! (`PARTITION BY KEY`), so that a root and all of its descendants share one
//! bucket and therefore one shard. MySQL and MariaDB store the comment and
//! ignore it, so the schema stays portable; a foreign key cannot say the same
//! thing there, because neither accepts foreign keys on partitioned tables.
//!
//! The checks here report where the keys do not line up. They run at
//! `CREATE TABLE`, `ALTER TABLE` and `DROP TABLE` (the session reports them as
//! warnings, or refuses the statement under `tarn_tree_checks = ERROR`), and
//! offline over a schema file (`check_schema`). A missing parent is only a
//! warning, since a dump may create a child before its parent; the parent's
//! `CREATE TABLE` then checks the children that name it.

use crate::catalog::{Catalog, ColumnDef, TableDef};
use crate::plan::Stmt;
use crate::planner::{self, PlanCtx};
use std::sync::Arc;
use tarn_common::SqlResult;

/// Error and warning code of the checks (MySQL's ER_UNKNOWN_ERROR: there is
/// no closer one).
pub const CODE: u16 = 1105;

/// The longest chain of parents followed (a cycle ends sooner).
const MAX_DEPTH: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    Note,
    Warning,
}

impl Level {
    pub fn name(self) -> &'static str {
        match self {
            Level::Note => "Note",
            Level::Warning => "Warning",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finding {
    pub level: Level,
    /// `db`.`table` the finding is about.
    pub db: String,
    pub table: String,
    pub message: String,
    /// The statement is refused for it under `tarn_tree_checks = ERROR` when
    /// it is about the statement's own table.
    pub strict: bool,
}

/// Looks a table up by database and name (case-insensitive).
pub type Lookup<'a> = dyn Fn(&str, &str) -> Option<Arc<TableDef>> + 'a;

/// What a table comment says: the parent, if it names one, and annotations
/// this version of Tarn does not know.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Annotations {
    /// (database, table): the database is the child's unless qualified.
    pub parent: Option<(Option<String>, String)>,
    /// Further `tarn:parent=` tokens (only the first counts).
    pub extra_parents: usize,
    pub unknown: Vec<String>,
}

/// Annotations are `tarn:<name>[=<value>]` tokens anywhere in the comment,
/// separated by whitespace, `,` or `;`.
pub fn annotations(comment: &str) -> Annotations {
    let mut a = Annotations::default();
    for tok in comment.split(|c: char| c.is_whitespace() || c == ',' || c == ';') {
        let Some(rest) = strip_prefix_ci(tok, "tarn:") else {
            continue;
        };
        let (name, value) = match rest.split_once('=') {
            Some((n, v)) => (n, Some(v)),
            None => (rest, None),
        };
        if name.eq_ignore_ascii_case("parent") {
            let v = value.unwrap_or("").trim_matches(|c| c == '`' || c == '"' || c == '\'');
            if v.is_empty() {
                a.unknown.push(tok.to_string());
            } else if a.parent.is_some() {
                a.extra_parents += 1;
            } else {
                a.parent = Some(match v.split_once('.') {
                    Some((db, t)) => (Some(unquote(db)), unquote(t)),
                    None => (None, v.to_string()),
                });
            }
        } else {
            a.unknown.push(tok.to_string());
        }
    }
    a
}

fn strip_prefix_ci<'s>(s: &'s str, prefix: &str) -> Option<&'s str> {
    let head = s.get(..prefix.len())?;
    head.eq_ignore_ascii_case(prefix).then(|| &s[prefix.len()..])
}

fn unquote(s: &str) -> String {
    s.trim_matches(|c| c == '`' || c == '"' || c == '\'').to_string()
}

/// The parent a table names: (database, table).
pub fn parent_of(t: &TableDef) -> Option<(String, String)> {
    let (db, name) = annotations(&t.comment).parent?;
    Some((db.unwrap_or_else(|| t.db.clone()), name))
}

fn same_table(t: &TableDef, db: &str, name: &str) -> bool {
    t.db.eq_ignore_ascii_case(db) && t.name.eq_ignore_ascii_case(name)
}

fn cols_text(t: &TableDef, cols: &[usize]) -> String {
    let names: Vec<&str> = cols.iter().map(|&c| t.columns[c].name.as_str()).collect();
    format!("({})", names.join(", "))
}

/// Why key column `child` of table `ct` and the parent's `parent` of table
/// `pt` would not hash and compare alike, if they would not: integers need
/// the same signedness, text the same collation; binary strings always
/// match; other types must be the same type.
fn key_type_mismatch(ct: &str, child: &ColumnDef, pt: &str, parent: &ColumnDef) -> Option<String> {
    let (c, p) = (child.ty(), parent.ty());
    if c.is_text() && p.is_text() {
        return (child.collation_name() != parent.collation_name()).then(|| {
            format!(
                "{}.{} has collation {} but {}.{} has {}",
                ct,
                child.name,
                child.collation_name(),
                pt,
                parent.name,
                parent.collation_name()
            )
        });
    }
    let same = if c.is_integer() && p.is_integer() {
        c.unsigned == p.unsigned
    } else if c.is_binary() && p.is_binary() {
        true
    } else {
        c == p
    };
    (!same).then(|| {
        format!(
            "{}.{} is {} but {}.{} is {}",
            ct, child.name, child.ty, pt, parent.name, parent.ty
        )
    })
}

/// The checks of table `t`, which may not be in the catalog yet (or be an
/// altered version of the one there): `look` finds every other table.
pub fn check_table(look: &Lookup<'_>, t: &TableDef) -> Vec<Finding> {
    let mut out = Vec::new();
    let finding = |level, message: String, strict| Finding {
        level,
        db: t.db.clone(),
        table: t.name.clone(),
        message,
        strict,
    };
    let ann = annotations(&t.comment);
    for u in &ann.unknown {
        out.push(finding(
            Level::Warning,
            format!("unknown table annotation '{}'", u),
            false,
        ));
    }
    if ann.extra_parents > 0 {
        out.push(finding(
            Level::Warning,
            "more than one tarn:parent; only the first counts".into(),
            true,
        ));
    }
    for ix in &t.indexes {
        if let Some(lazy) = &ix.lazy {
            let prefix = &ix.cols[..lazy.route_parts as usize];
            if t.shard_key.iter().all(|c| prefix.contains(c)) {
                out.push(finding(Level::Warning,format!(
                    "lazy index '{}': its routing prefix already contains the full table shard key; a local read can route to one shard",ix.name
                ),false));
            }
        }
    }
    let Some((pdb, pname)) = parent_of(t) else {
        return out;
    };
    let tag = format!("tarn:parent={}", pname);
    if same_table(t, &pdb, &pname) {
        out.push(finding(
            Level::Warning,
            format!("{}: a table cannot be its own parent", tag),
            true,
        ));
        return out;
    }
    let Some(parent) = look(&pdb, &pname) else {
        out.push(finding(
            Level::Warning,
            format!("{}: table {}.{} does not exist (yet)", tag, pdb, pname),
            false,
        ));
        return out;
    };
    // The chain of ancestors up to the root, or a cycle back to `t`.
    let mut chain: Vec<Arc<TableDef>> = vec![parent.clone()];
    let mut root_known = true;
    loop {
        let cur = chain.last().unwrap().clone();
        let Some((db, name)) = parent_of(&cur) else {
            break;
        };
        if same_table(t, &db, &name) {
            let names: Vec<&str> = std::iter::once(t.name.as_str())
                .chain(chain.iter().map(|x| x.name.as_str()))
                .chain(std::iter::once(t.name.as_str()))
                .collect();
            out.push(finding(
                Level::Warning,
                format!("{}: the parent links form a cycle ({})", tag, names.join(" -> ")),
                true,
            ));
            return out;
        }
        if chain.len() >= MAX_DEPTH || chain.iter().any(|x| same_table(x, &db, &name)) {
            // a cycle above this table: reported for the tables in it
            root_known = false;
            break;
        }
        match look(&db, &name) {
            Some(p) => chain.push(p),
            None => {
                root_known = false;
                break;
            }
        }
    }
    // The primary key starts with the parent's, column by column.
    let (pk, ppk) = (&t.pk().cols, &parent.pk().cols);
    let prefix_ok = pk.len() >= ppk.len()
        && ppk
            .iter()
            .zip(pk.iter())
            .all(|(&p, &c)| t.columns[c].name.eq_ignore_ascii_case(&parent.columns[p].name));
    if !prefix_ok {
        out.push(finding(
            Level::Warning,
            format!(
                "{}: the primary key {} must start with the primary key of {}, {}",
                tag,
                cols_text(t, pk),
                parent.name,
                cols_text(&parent, ppk)
            ),
            true,
        ));
    } else {
        for (&p, &c) in ppk.iter().zip(pk.iter()) {
            if let Some(why) = key_type_mismatch(&t.name, &t.columns[c], &parent.name, &parent.columns[p]) {
                out.push(finding(
                    Level::Warning,
                    format!("{}: {}; equal keys must hash alike", tag, why),
                    true,
                ));
            }
        }
    }
    // The shard key is the root's: the root's shard key columns, at their
    // positions in its primary key, which every descendant's key starts with.
    if root_known && prefix_ok {
        let root = chain.last().unwrap();
        let rpk = &root.pk().cols;
        let positions: Option<Vec<usize>> = root.shard_key.iter().map(|c| rpk.iter().position(|x| x == c)).collect();
        if let Some(positions) = positions.filter(|ps| ps.iter().all(|&i| i < pk.len())) {
            let want: Vec<usize> = positions.iter().map(|&i| pk[i]).collect();
            if t.shard_key != want {
                let fix = cols_text(t, &want);
                out.push(finding(
                    Level::Warning,
                    format!(
                        "{}: the shard key {} must be the shard key of the root {}, {}: add PARTITION BY KEY {}",
                        tag,
                        cols_text(t, &t.shard_key),
                        root.name,
                        cols_text(root, &root.shard_key),
                        fix
                    ),
                    true,
                ));
            }
        }
    }
    // Unique keys outside the shard key are enforced through global indexes:
    // a write that changes one commits on a second shard.
    for idx in t.indexes.iter().skip(1) {
        if t.needs_global(idx) {
            out.push(finding(
                Level::Note,
                format!(
                    "{}: unique key {} does not contain the shard key; writes that change it also commit on the shard of its global index",
                    tag, idx.name
                ),
                false,
            ));
        }
    }
    out
}

/// Tables of the catalog that name `db`.`name` as their parent.
pub fn children_of(cat: &Catalog, db: &str, name: &str) -> Vec<Arc<TableDef>> {
    let mut v: Vec<Arc<TableDef>> = cat
        .all()
        .filter(|t| parent_of(t).is_some_and(|(d, n)| d.eq_ignore_ascii_case(db) && n.eq_ignore_ascii_case(name)))
        .cloned()
        .collect();
    v.sort_by(|a, b| (&a.db, &a.name).cmp(&(&b.db, &b.name)));
    v
}

/// Every table of the catalog that names a parent, or is named as one, with
/// `replaced` standing in for the catalog's tables of the same name and
/// `removed` left out: what `look` returns during the checks.
pub fn lookup_with<'a>(
    cat: &'a Catalog,
    replaced: &'a [Arc<TableDef>],
    removed: &'a [(String, String)],
) -> impl Fn(&str, &str) -> Option<Arc<TableDef>> + 'a {
    move |db: &str, name: &str| {
        if let Some(t) = replaced.iter().find(|t| same_table(t, db, name)) {
            return Some(t.clone());
        }
        if removed
            .iter()
            .any(|(d, n)| d.eq_ignore_ascii_case(db) && n.eq_ignore_ascii_case(name))
        {
            return None;
        }
        cat.get(db, name)
    }
}

/// Checks a schema file, such as a dump: its `CREATE DATABASE`, `USE` and
/// `CREATE TABLE` statements build a catalog (other statements are
/// skipped), then every table that names a parent, or is named as one, is
/// checked. `db`: the database of tables before any `USE`.
pub fn check_schema(statements: &[&str], db: Option<&str>, default_coll: &str) -> SqlResult<Vec<Finding>> {
    let mut databases: Vec<String> = db.map(|d| vec![d.to_string()]).unwrap_or_default();
    let mut current = db.map(str::to_string);
    let mut defs: Vec<TableDef> = Vec::new();
    for raw in statements {
        let sql = crate::prep::expand_comments(raw);
        let s = sql.trim().trim_end_matches(';').trim();
        let mut words = s.split_whitespace();
        let first = words.next().unwrap_or("").to_ascii_uppercase();
        let second = words.next().unwrap_or("").to_ascii_uppercase();
        match (first.as_str(), second.as_str()) {
            ("USE", _) => {
                let d = unquote(s[3..].trim());
                if !databases.iter().any(|x| x.eq_ignore_ascii_case(&d)) {
                    databases.push(d.clone());
                }
                current = Some(d);
            }
            ("CREATE", "DATABASE" | "SCHEMA") => {
                let name = s
                    .split_whitespace()
                    .rfind(|w| !w.eq_ignore_ascii_case("EXISTS"))
                    .map(unquote)
                    .unwrap_or_default();
                if !name.is_empty() && !databases.iter().any(|x| x.eq_ignore_ascii_case(&name)) {
                    databases.push(name);
                }
            }
            ("CREATE", "TABLE" | "TEMPORARY") => {
                let cat = Catalog::new(0, databases.clone(), defs.clone());
                let ctx = PlanCtx {
                    catalog: &cat,
                    db: current.as_deref(),
                    params: &[],
                    conn_coll: default_coll,
                    default_coll,
                };
                if let Stmt::CreateTable { def, .. } = planner::plan(s, &ctx)? {
                    defs.retain(|d| !same_table(d, &def.db, &def.name));
                    defs.push(*def);
                }
            }
            _ => {}
        }
    }
    let cat = Catalog::new(0, databases, defs);
    let look = lookup_with(&cat, &[], &[]);
    let mut tables: Vec<&Arc<TableDef>> = cat.all().collect();
    tables.sort_by(|a, b| (&a.db, &a.name).cmp(&(&b.db, &b.name)));
    Ok(tables.into_iter().flat_map(|t| check_table(&look, t)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan_all(sqls: &[&str]) -> Catalog {
        let mut defs: Vec<TableDef> = Vec::new();
        for (i, sql) in sqls.iter().enumerate() {
            let cat = Catalog::new(1, vec!["d".into()], defs.clone());
            let ctx = PlanCtx {
                catalog: &cat,
                db: Some("d"),
                params: &[],
                conn_coll: "utf8mb4_bin",
                default_coll: "utf8mb4_bin",
            };
            let Ok(Stmt::CreateTable { mut def, .. }) = planner::plan(sql, &ctx) else {
                panic!("{sql}")
            };
            def.space_id = 512 + i as u32;
            defs.push(*def);
        }
        Catalog::new(1, vec!["d".into()], defs)
    }

    fn check(cat: &Catalog, name: &str) -> Vec<String> {
        let look = lookup_with(cat, &[], &[]);
        check_table(&look, &cat.get("d", name).unwrap())
            .into_iter()
            .map(|f| format!("{}: {}", f.level.name(), f.message))
            .collect()
    }

    const CUSTOMER: &str =
        "CREATE TABLE customer (customer_id BIGINT NOT NULL, name VARCHAR(100), PRIMARY KEY (customer_id))";
    const ORDERS: &str = "CREATE TABLE orders (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL, \
        PRIMARY KEY (customer_id, order_id)) COMMENT 'tarn:parent=customer' PARTITION BY KEY (customer_id)";

    #[test]
    fn annotations_in_comments() {
        assert_eq!(annotations("plain text"), Annotations::default());
        let a = annotations("orders of a customer; tarn:parent=customer");
        assert_eq!(a.parent, Some((None, "customer".into())));
        let a = annotations("TARN:PARENT=`app`.`t`, tarn:lazy");
        assert_eq!(a.parent, Some((Some("app".into()), "t".into())));
        assert_eq!(a.unknown, vec!["tarn:lazy".to_string()]);
        let a = annotations("tarn:parent=a tarn:parent=b tarn:parnet=c tarn:parent=");
        assert_eq!(a.parent, Some((None, "a".into())));
        assert_eq!(a.extra_parents, 1);
        assert_eq!(a.unknown, vec!["tarn:parnet=c".to_string(), "tarn:parent=".to_string()]);
    }

    #[test]
    fn a_tree_that_lines_up() {
        let cat = plan_all(&[
            CUSTOMER,
            ORDERS,
            "CREATE TABLE order_line (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL, line_no INT NOT NULL, \
             PRIMARY KEY (customer_id, order_id, line_no)) COMMENT 'tarn:parent=orders' PARTITION BY KEY (customer_id)",
            // one row per parent row: the same key
            "CREATE TABLE customer_note (customer_id BIGINT NOT NULL, note TEXT, PRIMARY KEY (customer_id)) \
             COMMENT 'tarn:parent=customer'",
        ]);
        for t in ["customer", "orders", "order_line", "customer_note"] {
            assert_eq!(check(&cat, t), Vec::<String>::new(), "{t}");
        }
    }

    #[test]
    fn keys_that_do_not_line_up() {
        let cat = plan_all(&[
            CUSTOMER,
            ORDERS,
            // keyed without its order
            "CREATE TABLE line_a (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL, line_no INT NOT NULL, \
             PRIMARY KEY (customer_id, line_no)) COMMENT 'tarn:parent=orders' PARTITION BY KEY (customer_id)",
            // the right columns, sharded by its own key
            "CREATE TABLE line_b (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL, line_no INT NOT NULL, \
             PRIMARY KEY (customer_id, order_id, line_no)) COMMENT 'tarn:parent=orders'",
            // a key column of another sign
            "CREATE TABLE line_c (customer_id BIGINT UNSIGNED NOT NULL, order_id BIGINT NOT NULL, line_no INT NOT NULL, \
             PRIMARY KEY (customer_id, order_id, line_no)) COMMENT 'tarn:parent=orders' PARTITION BY KEY (customer_id)",
        ]);
        assert_eq!(
            check(&cat, "line_a"),
            vec!["Warning: tarn:parent=orders: the primary key (customer_id, line_no) must start with the primary key of orders, (customer_id, order_id)"]
        );
        assert_eq!(
            check(&cat, "line_b"),
            vec!["Warning: tarn:parent=orders: the shard key (customer_id, order_id, line_no) must be the shard key of the root customer, (customer_id): add PARTITION BY KEY (customer_id)"]
        );
        assert_eq!(
            check(&cat, "line_c"),
            vec!["Warning: tarn:parent=orders: line_c.customer_id is bigint unsigned but orders.customer_id is bigint; equal keys must hash alike"]
        );
    }

    #[test]
    fn key_types_that_hash_alike() {
        let cat = plan_all(&[
            "CREATE TABLE p (k VARCHAR(20) COLLATE utf8mb4_general_ci NOT NULL, n INT NOT NULL, PRIMARY KEY (k, n))",
            // a longer VARCHAR and a wider integer hash and compare alike
            "CREATE TABLE c1 (k VARCHAR(40) COLLATE utf8mb4_general_ci NOT NULL, n BIGINT NOT NULL, i INT NOT NULL, \
             PRIMARY KEY (k, n, i)) COMMENT 'tarn:parent=p' PARTITION BY KEY (k, n)",
            // another collation does not
            "CREATE TABLE c2 (k VARCHAR(20) COLLATE utf8mb4_bin NOT NULL, n INT NOT NULL, i INT NOT NULL, \
             PRIMARY KEY (k, n, i)) COMMENT 'tarn:parent=p' PARTITION BY KEY (k, n)",
        ]);
        assert_eq!(check(&cat, "c1"), Vec::<String>::new());
        assert_eq!(
            check(&cat, "c2"),
            vec!["Warning: tarn:parent=p: c2.k has collation utf8mb4_bin but p.k has utf8mb4_general_ci; equal keys must hash alike"]
        );
    }

    #[test]
    fn a_root_sharded_by_part_of_its_key() {
        let cat = plan_all(&[
            "CREATE TABLE r (tenant_id INT NOT NULL, id INT NOT NULL, PRIMARY KEY (tenant_id, id)) PARTITION BY KEY (tenant_id)",
            "CREATE TABLE ok (tenant_id INT NOT NULL, id INT NOT NULL, n INT NOT NULL, PRIMARY KEY (tenant_id, id, n)) \
             COMMENT 'tarn:parent=r' PARTITION BY KEY (tenant_id)",
            "CREATE TABLE bad (tenant_id INT NOT NULL, id INT NOT NULL, n INT NOT NULL, PRIMARY KEY (tenant_id, id, n)) \
             COMMENT 'tarn:parent=r' PARTITION BY KEY (tenant_id, id)",
        ]);
        assert_eq!(check(&cat, "ok"), Vec::<String>::new());
        assert_eq!(
            check(&cat, "bad"),
            vec!["Warning: tarn:parent=r: the shard key (tenant_id, id) must be the shard key of the root r, (tenant_id): add PARTITION BY KEY (tenant_id)"]
        );
    }

    #[test]
    fn missing_parents_cycles_and_annotations() {
        let cat = plan_all(&[
            "CREATE TABLE a (id INT PRIMARY KEY) COMMENT 'tarn:parent=b'",
            "CREATE TABLE b (id INT PRIMARY KEY) COMMENT 'tarn:parent=a'",
            "CREATE TABLE s (id INT PRIMARY KEY) COMMENT 'tarn:parent=s'",
            "CREATE TABLE o (id INT PRIMARY KEY) COMMENT 'tarn:parent=nosuch tarn:paremt=x'",
            "CREATE TABLE q (id INT PRIMARY KEY) COMMENT 'tarn:parent=other.a'",
        ]);
        assert_eq!(
            check(&cat, "a"),
            vec!["Warning: tarn:parent=b: the parent links form a cycle (a -> b -> a)"]
        );
        assert_eq!(
            check(&cat, "s"),
            vec!["Warning: tarn:parent=s: a table cannot be its own parent"]
        );
        assert_eq!(
            check(&cat, "o"),
            vec![
                "Warning: unknown table annotation 'tarn:paremt=x'",
                "Warning: tarn:parent=nosuch: table d.nosuch does not exist (yet)"
            ]
        );
        assert_eq!(
            check(&cat, "q"),
            vec!["Warning: tarn:parent=a: table other.a does not exist (yet)"]
        );
        // only the cycle counts as strict
        let look = lookup_with(&cat, &[], &[]);
        let strict: Vec<bool> = check_table(&look, &cat.get("d", "o").unwrap())
            .iter()
            .map(|f| f.strict)
            .collect();
        assert_eq!(strict, vec![false, false]);
    }

    #[test]
    fn unique_keys_outside_the_shard_key() {
        let cat = plan_all(&[
            CUSTOMER,
            "CREATE TABLE orders (customer_id BIGINT NOT NULL, order_id BIGINT NOT NULL, code VARCHAR(10), \
             PRIMARY KEY (customer_id, order_id), UNIQUE KEY by_code (code)) \
             COMMENT 'tarn:parent=customer' PARTITION BY KEY (customer_id)",
        ]);
        assert_eq!(
            check(&cat, "orders"),
            vec!["Note: tarn:parent=customer: unique key by_code does not contain the shard key; writes that change it also commit on the shard of its global index"]
        );
    }

    #[test]
    fn children_and_replaced_tables() {
        let cat = plan_all(&[CUSTOMER, ORDERS]);
        let kids: Vec<String> = children_of(&cat, "D", "Customer")
            .iter()
            .map(|t| t.name.clone())
            .collect();
        assert_eq!(kids, vec!["orders".to_string()]);
        // the parent as an ALTER TABLE would leave it: another key type
        let mut altered = (*cat.get("d", "customer").unwrap()).clone();
        altered.columns[0].ty = "bigint unsigned".into();
        altered.resolve_types().unwrap();
        let replaced = [Arc::new(altered)];
        let look = lookup_with(&cat, &replaced, &[]);
        let f = check_table(&look, &cat.get("d", "orders").unwrap());
        assert_eq!(f.len(), 1);
        assert!(
            f[0].message
                .contains("orders.customer_id is bigint but customer.customer_id is bigint unsigned"),
            "{:?}",
            f
        );
        // and dropped
        let gone = [("d".to_string(), "customer".to_string())];
        let look = lookup_with(&cat, &[], &gone);
        let f = check_table(&look, &cat.get("d", "orders").unwrap());
        assert_eq!(
            f[0].message,
            "tarn:parent=customer: table d.customer does not exist (yet)"
        );
    }

    #[test]
    fn schema_files() {
        // a dump creates tables in name order: the child before its parent
        let dump = [
            "/*!40101 SET NAMES utf8mb4 */",
            "CREATE DATABASE IF NOT EXISTS `app`",
            "USE `app`",
            "DROP TABLE IF EXISTS `order_line`",
            "CREATE TABLE `order_line` (`customer_id` bigint NOT NULL, `order_id` bigint NOT NULL, `line_no` int NOT NULL, \
             PRIMARY KEY (`customer_id`,`line_no`)) ENGINE=InnoDB COMMENT='tarn:parent=orders' \
             /*!50100 PARTITION BY KEY (customer_id) */",
            "INSERT INTO `order_line` VALUES (1,1,1)",
            "CREATE TABLE `orders` (`customer_id` bigint NOT NULL, `order_id` bigint NOT NULL, \
             PRIMARY KEY (`customer_id`,`order_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 \
             /*!50100 PARTITION BY KEY (customer_id) */",
        ];
        let f = check_schema(&dump, None, "utf8mb4_bin").unwrap();
        let got: Vec<String> = f
            .iter()
            .map(|f| format!("{}.{}: {}", f.db, f.table, f.message))
            .collect();
        assert_eq!(
            got,
            vec!["app.order_line: tarn:parent=orders: the primary key (customer_id, line_no) must start with the primary key of orders, (customer_id, order_id)"]
        );
    }
}
