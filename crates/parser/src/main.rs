//! Jibs DSL Parser CLI

use std::io::IsTerminal;
use std::{env, fs};

fn main() {
    let filename = env::args().nth(1).expect("Usage: jibs_parser <file.jibs>");
    let source = fs::read_to_string(&filename).expect("Failed to read file");

    match jibs_parser::parse(&source) {
        Ok(program) => {
            println!("Successfully parsed {} statements:", program.statements.len());
            for (i, (stmt, _span)) in program.statements.iter().enumerate() {
                let kind = match &stmt.kind {
                    jibs_parser::ast::StatementKind::Import(_) => "import",
                    jibs_parser::ast::StatementKind::Var(_) => "var",
                    jibs_parser::ast::StatementKind::Faker(_) => "faker",
                    jibs_parser::ast::StatementKind::Relation(_) => "relation",
                    jibs_parser::ast::StatementKind::IgnoreRelation(_) => "ignore_relation",
                    jibs_parser::ast::StatementKind::Anonymize(_) => "anonymize",
                    jibs_parser::ast::StatementKind::Exclude(_) => "exclude",
                    jibs_parser::ast::StatementKind::Ignore(_) => "ignore",
                    jibs_parser::ast::StatementKind::Full(_) => "full",
                    jibs_parser::ast::StatementKind::Aggregate(_) => "aggregate",
                    jibs_parser::ast::StatementKind::Get(_) => "get",
                    jibs_parser::ast::StatementKind::Preserve(_) => "preserve",
                    jibs_parser::ast::StatementKind::Set(_) => "set",
                    jibs_parser::ast::StatementKind::After(_) => "after",
                };
                let has_attr = if stmt.attribute.is_some() { " (conditional)" } else { "" };
                println!("  {}. {}{}", i + 1, kind, has_attr);
            }
        }
        Err(errors) => {
            let color = std::io::stderr().is_terminal();
            eprint!("{}", jibs_parser::render_errors(&filename, &source, &errors, color));
            std::process::exit(1);
        }
    }
}
