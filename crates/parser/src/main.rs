//! MySQL Import DSL CLI

use ariadne::{Color, Label, Report, ReportKind, Source};
use std::{env, fs};

fn main() {
    let filename = env::args().nth(1).expect("Usage: mysqlimport-parser <file.dsl>");
    let source = fs::read_to_string(&filename).expect("Failed to read file");

    match mysqlimport_parser::parse(&source) {
        Ok(program) => {
            println!("Successfully parsed {} statements:", program.statements.len());
            for (i, (stmt, _span)) in program.statements.iter().enumerate() {
                let kind = match &stmt.kind {
                    mysqlimport_parser::ast::StatementKind::Import(_) => "import",
                    mysqlimport_parser::ast::StatementKind::Var(_) => "var",
                    mysqlimport_parser::ast::StatementKind::Faker(_) => "faker",
                    mysqlimport_parser::ast::StatementKind::Relation(_) => "relation",
                    mysqlimport_parser::ast::StatementKind::Anonymize(_) => "anonymize",
                    mysqlimport_parser::ast::StatementKind::Exclude(_) => "exclude",
                    mysqlimport_parser::ast::StatementKind::Ignore(_) => "ignore",
                    mysqlimport_parser::ast::StatementKind::Aggregate(_) => "aggregate",
                    mysqlimport_parser::ast::StatementKind::Include(_) => "include",
                    mysqlimport_parser::ast::StatementKind::Preserve(_) => "preserve",
                    mysqlimport_parser::ast::StatementKind::Set(_) => "set",
                    mysqlimport_parser::ast::StatementKind::After(_) => "after",
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
