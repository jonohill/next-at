use std::fs::File;

use regex::Regex;
use sea_orm_cli::{DateTimeCrate, GenerateSubcommands, MigrateSubcommands};
use std::fs;
use tempfile::TempDir;

fn main() {
    println!("cargo:rerun-if-changed=migration/src");

    const MIGRATION_DIR: &str = "./migration";
    const ENTITY_DIR: &str = "./src/entity";

    // need an actual file so it can be shared between commands
    let tmp_dir = TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("db.sqlite");
    File::create(&db_path).unwrap();
    let db_url = format!("sqlite://{}", db_path.display());

    let cmd = MigrateSubcommands::Up { num: None };
    sea_orm_cli::run_migrate_command(Some(cmd), MIGRATION_DIR, None, Some(db_url.clone()), false)
        .unwrap();

    let cmd = GenerateSubcommands::Entity {
        output_dir: ENTITY_DIR.to_string(),
        database_url: db_url,
        with_serde: "both".to_string(),
        // the rest are just the default values
        compact_format: true,
        expanded_format: false,
        include_hidden_tables: false,
        tables: vec![],
        ignore_tables: vec![],
        max_connections: 1,
        database_schema: "public".to_string(),
        serde_skip_deserializing_primary_key: false,
        serde_skip_hidden_column: false,
        with_copy_enums: false,
        date_time_crate: DateTimeCrate::Chrono,
        lib: false,
        model_extra_derives: vec![],
        model_extra_attributes: vec![],
        enum_extra_attributes: vec![],
        enum_extra_derives: vec![],
        seaography: false,
    };

    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async { sea_orm_cli::run_generate_command(cmd, false).await })
        .unwrap();

    // The generated primary key values should be u64
    // https://github.com/SeaQL/sea-orm/issues/2051

    let re_pk_type = Regex::new(r#"(#\[sea_orm\(primary_key.+\][^,]+: )Option<i32>"#).unwrap();

    for entry in fs::read_dir(ENTITY_DIR).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext != "rs" {
                    continue;
                }
                let file_contents = fs::read_to_string(&path).unwrap();
                let new_contents =
                    // We ignore all unused imports in the generated code
                    "#![allow(unused_imports)]\n".to_string() +
                    &re_pk_type.replace_all(&file_contents, "${1}i64");
                fs::write(path, new_contents).unwrap();
            }
        }
    }
}
