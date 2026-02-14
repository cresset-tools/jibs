//! Jibs DSL Parser CLI

use ariadne::{Color, Label, Report, ReportKind, Source};
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
                    jibs_parser::ast::StatementKind::Anonymize(_) => "anonymize",
                    jibs_parser::ast::StatementKind::Exclude(_) => "exclude",
                    jibs_parser::ast::StatementKind::Ignore(_) => "ignore",
                    jibs_parser::ast::StatementKind::Aggregate(_) => "aggregate",
                    jibs_parser::ast::StatementKind::Include(_) => "include",
                    jibs_parser::ast::StatementKind::Preserve(_) => "preserve",
                    jibs_parser::ast::StatementKind::Set(_) => "set",
                    jibs_parser::ast::StatementKind::After(_) => "after",
                };
                let has_attr = if stmt.attribute.is_some() { " (conditional)" } else { "" };
                println!("  {}. {}{}", i + 1, kind, has_attr);
            }
        }
        Err(errors) => {
            for error in errors {
                Report::build(ReportKind::Error, &filename, error.span.start)
                    .with_message(&error.message)
                    .with_label(
                        Label::new((&filename, error.span.clone()))
                            .with_message(&error.message)
                            .with_color(Color::Red),
                    )
                    .finish()
                    .print((&filename, Source::from(&source)))
                    .unwrap();
            }
            std::process::exit(1);
        }
    }
}
