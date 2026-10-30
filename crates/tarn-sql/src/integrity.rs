//! Declaration admission, not a new constraint enforcement path. Old catalogs
//! discarded CHECKs and ENUM/SET lists; their absence cannot certify old rows.

use tarn_common::{SqlError, SqlResult};

/// Catalog provenance only: CREATE rejected declarations it could not enforce.
/// ALTER preserves this value; it never upgrades a legacy table's provenance.
pub const DECLARATION_POLICY: u8 = 1;
pub const LEGACY_NOTICE: &str =
    " /* TARN LEGACY_UNENFORCED: foreign keys are metadata only; CHECK/ENUM/SET history unknown; inspect SHOW TARN INTEGRITY */";

pub fn reject(kind: &str) -> SqlError {
    SqlError::unsupported(format!("{kind} declarations: Tarn does not enforce their semantics"))
}

/// Keep legacy type decoding available for existing catalogs, but never accept
/// another value-list declaration as an unrestricted TEXT column.
pub fn check_column_type(text: &str) -> SqlResult<()> {
    let name = text.trim().split(['(', ' ']).next().unwrap_or_default();
    if name.eq_ignore_ascii_case("ENUM") || name.eq_ignore_ascii_case("SET") {
        return Err(reject(&name.to_ascii_uppercase()));
    }
    Ok(())
}

pub fn check_write(table: &crate::catalog::TableDef, legacy_allowed: bool) -> SqlResult<()> {
    if !legacy_allowed && !table.current_integrity_policy() {
        return Err(SqlError::unsupported(format!(
            "writes to legacy integrity schema '{}.{}'; inspect SHOW TARN INTEGRITY and migrate, or explicitly SET SESSION tarn_legacy_integrity='ALLOW' (unenforced)",
            table.db, table.name
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        catalog::Catalog,
        plan::Stmt,
        planner::{self, PlanCtx},
    };

    #[test]
    fn declarations_are_rejected_before_a_plan_can_publish() {
        let empty = Catalog::new(1, vec!["db".into()], vec![]);
        let context = |catalog| PlanCtx {
            catalog,
            db: Some("db"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
            statistics: None,
        };
        let Stmt::CreateTable { def, .. } =
            planner::plan("CREATE TABLE t(id INT PRIMARY KEY,v INT)", &context(&empty)).unwrap()
        else {
            panic!()
        };
        assert!(def.current_integrity_policy());
        let cat = Catalog::new(1, vec!["db".into()], vec![*def]);
        for ddl in [
            "CREATE TABLE x(id INT PRIMARY KEY,v INT,CHECK(v>0))",
            "CREATE TABLE x(id INT PRIMARY KEY,v INT CHECK(v>0))",
            "CREATE TABLE x(id INT PRIMARY KEY,v INT,FOREIGN KEY(v) REFERENCES t(id))",
            "CREATE TABLE x(id INT PRIMARY KEY,v INT REFERENCES t(id))",
            "CREATE TABLE x(id INT PRIMARY KEY,v ENUM('a','b'))",
            "CREATE TABLE x(id INT PRIMARY KEY,v SET('a','b'))",
            "ALTER TABLE t ADD CONSTRAINT c CHECK(v>0)",
            "ALTER TABLE t ADD FOREIGN KEY(v) REFERENCES t(id)",
            "ALTER TABLE t ADD v2 INT CHECK(v2>0)",
            "ALTER TABLE t ADD v2 ENUM('a','b')",
            "ALTER TABLE t MODIFY v SET('a','b')",
            "ALTER TABLE t CHANGE v v ENUM('a','b')",
            "ALTER TABLE t ALTER CHECK c ENFORCED",
            "ALTER TABLE t ALTER CHECK c NOT ENFORCED",
            "ALTER TABLE t ADD safe INT, ADD CONSTRAINT c CHECK(v>0)",
        ] {
            let error = planner::plan(ddl, &context(&cat)).unwrap_err();
            assert_eq!((error.code, error.sqlstate), (1235, *b"42000"), "{ddl}: {error}");
        }
        // Keywords inside identifiers, data and comments are not declarations.
        assert!(planner::plan(
            "CREATE TABLE x(id INT PRIMARY KEY,`enum` TEXT DEFAULT 'CHECK FOREIGN KEY SET') COMMENT 'CHECK (id>0)'",
            &context(&cat)
        )
        .is_ok());
    }

    #[test]
    fn old_catalogs_remain_unknown_after_metadata_removal_and_alter() {
        let empty = Catalog::new(1, vec!["db".into()], vec![]);
        let context = |catalog| PlanCtx {
            catalog,
            db: Some("db"),
            params: &[],
            conn_coll: "utf8mb4_bin",
            default_coll: "utf8mb4_bin",
            statistics: None,
        };
        let Stmt::CreateTable { def, .. } =
            planner::plan("CREATE TABLE t(id INT PRIMARY KEY,v TEXT)", &context(&empty)).unwrap()
        else {
            panic!()
        };
        let mut json = serde_json::to_value(def).unwrap();
        json.as_object_mut().unwrap().remove("integrity_policy");
        json["foreign_keys"] =
            serde_json::json!([{"name":"fk","cols":["v"],"ref_db":"db","ref_table":"p","ref_cols":["id"]}]);
        let old = crate::catalog::parse_def_json(&json.to_string()).unwrap();
        assert_eq!(old.integrity_policy, 0);
        assert!(!old.current_integrity_policy());
        assert_eq!(check_write(&old, false).unwrap_err().code, 1235);
        assert!(check_write(&old, true).is_ok());
        let cat = Catalog::new(1, vec!["db".into()], vec![old.clone()]);
        let Stmt::Alter(plans) =
            planner::plan("ALTER TABLE t DROP FOREIGN KEY fk, ADD note INT", &context(&cat)).unwrap()
        else {
            panic!()
        };
        let changed = crate::alter::apply(&old, &plans[0], &cat, "2026-10-29 00:00:00").unwrap();
        assert!(changed.def.foreign_keys.is_empty());
        let reloaded = crate::catalog::parse_def_json(&changed.def.lua_def().json).unwrap();
        assert_eq!(reloaded.integrity_policy, 0);
        assert!(!reloaded.current_integrity_policy());
        assert!(check_column_type("enum('a')").is_err());
    }
}
