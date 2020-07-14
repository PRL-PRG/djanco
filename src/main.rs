mod ast;

#[macro_use]
extern crate lalrpop_util;

lalrpop_mod!(pub grammar); // synthesized by LALRPOP

pub fn parse(input: &str) -> Result<ast::Query, String> {
    match grammar::QUERYParser::new().parse(input) {
        Ok(ast) => Ok(ast),
        Err(e)  => Err(format!("{:?}", e)),
    }
}

macro_rules! prompt {
    () => {
        print!("> ");
        io::stdout().flush().expect("Could not flush stdout");
    }
}

fn main() {
    use std::io::{self, BufRead, Write};

    println!();
    println!("Available features (optional parameters in parentheses):");
    println!();
    println!("    commits(path==GLOB, path!=GLOB)");
    println!("    changes(path==GLOB, path!=GLOB)");
    println!("    additions(path==GLOB, path!=GLOB)");
    println!("    deletions(path==GLOB, path!=GLOB)");
    println!();
    println!();
    println!("Available connectives between features:");
    println!();
    println!("    &&");
    println!();
    println!();

    prompt!();
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let query: &str = &line.unwrap();
        println!("Parsing: {}", query);
        println!("AST:     {:?}", parse(query));

        prompt!();
    }
}

#[cfg(test)]
mod tests {
    use crate::parse;
    use crate::ast::{Query, Expression, Feature, Parameter, Property, Connective};

    fn one_feature(feature: Feature) -> Query {
        Query::simple(Expression::from_feature(feature))
    }

    fn conjunction(features: Vec<Feature>) -> Query {
        assert!(features.len() > 1);
        Query::simple(Expression::from_features(Connective::Conjunction, features).unwrap())
    }

    fn parse_ok(input: &str, expected: Query) {
        assert_eq!(parse(input), Ok(expected));
    }

    #[test] fn test_commits() {
        let input = "commits";
        let expected = one_feature(Feature::commits_simple());

        parse_ok(input,expected);
    }

    #[test] fn test_commits_with_empty_parens() {
        let input = "commits()";
        let expected = one_feature(Feature::commits_simple());

        parse_ok(input,expected);
    }

    #[test] fn test_commits_with_equal_path_filter() {
        let input = r#"commits(path=="test/*")"#;
        let expected = one_feature(Feature::commits_with_parameter(Parameter::path_equal_str("test/*")));

        parse_ok(input, expected);
    }

    #[test] fn test_commits_with_different_path_filter() {
        let input = r#"commits(path!="test/*")"#;
        let expected = one_feature(Feature::commits_with_parameter(Parameter::path_different_str("test/*")));

        parse_ok(input, expected);
    }

    #[test] fn test_commits_with_extra_comma() {
        let input = r#"commits(path!="test/*",)"#;
        let expected = one_feature(Feature::commits_with_parameter(Parameter::path_different_str("test/*")));

        parse_ok(input, expected);
    }

    #[test] fn test_commits_with_elapsed_time() {
        let input = "commits.elapsedTime";
        let expected = one_feature(Feature::commits_with_property(Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_commits_with_elapsed_time_and_empty_parens_() {
        let input = "commits().elapsedTime()";
        let expected = one_feature(Feature::commits_with_property(Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_commits_with_equal_path_filter_and_elapsed_time() {
        let parameter = Parameter::path_equal_str("test/*");
        let input = r#"commits(path=="test/*").elapsedTime"#;
        let expected = one_feature(Feature::commits(vec![parameter], Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_commits_with_different_path_filter_and_elapsed_time() {
        let parameter = Parameter::path_different_str("test/*");
        let input = r#"commits(path!="test/*").elapsedTime"#;
        let expected = one_feature(Feature::commits(vec![parameter], Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_additions() {
        let input = "additions";
        let expected = one_feature(Feature::additions_simple());

        parse_ok(input, expected);
    }

    #[test] fn test_additions_with_empty_parens() {
        let input = "additions()";
        let expected = one_feature(Feature::additions_simple());

        parse_ok(input, expected);
    }

    #[test] fn test_additions_with_equal_path_filter() {
        let input = r#"additions(path=="test/*")"#;
        let expected = one_feature(Feature::additions_with_parameter(Parameter::path_equal_str("test/*")));

        parse_ok(input, expected);
    }

    #[test] fn test_additions_with_different_path_filter() {
        let input = r#"additions(path!="test/*")"#;
        let expected = one_feature(Feature::additions_with_parameter(Parameter::path_different_str("test/*")));

        parse_ok(input, expected);
    }

    #[test] fn test_additions_with_extra_comma() {
        let input = r#"additions(path!="test/*",)"#;
        let expected = one_feature(Feature::additions_with_parameter(Parameter::path_different_str("test/*")));

        parse_ok(input, expected);
    }

    #[test] fn test_additions_with_elapsed_time() {
        let input = "additions.elapsedTime";
        let expected = one_feature(Feature::additions(vec![], Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_additions_with_elapsed_time_and_empty_parens_() {
        let input = "additions().elapsedTime()";
        let expected = one_feature(Feature::additions(vec![], Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_additions_with_equal_path_filter_and_elapsed_time() {
        let parameter = Parameter::path_equal_str("test/*");
        let input = r#"additions(path=="test/*").elapsedTime"#;
        let expected = one_feature(Feature::additions(vec![parameter], Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_additions_with_different_path_filter_and_elapsed_time() {
        let input = r#"additions(path!="test/*").elapsedTime"#;
        let expected = one_feature(Feature::additions(vec![Parameter::path_different_str("test/*")],
                                                      Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_deletions() {
        let input = "deletions";
        let expected = one_feature(Feature::deletions_simple());

        parse_ok(input, expected);
    }

    #[test] fn test_deletions_with_empty_parens() {
        let input = "deletions()";
        let expected = one_feature(Feature::deletions_simple());

        parse_ok(input, expected);
    }

    #[test] fn test_deletions_with_equal_path_filter() {
        let input = r#"deletions(path=="test/*")"#;
        let expected = one_feature(Feature::deletions_with_parameter(Parameter::path_equal_str("test/*")));

        parse_ok(input, expected);
    }

    #[test] fn test_deletions_with_different_path_filter() {
        let input = r#"deletions(path!="test/*")"#;
        let expected = one_feature(Feature::deletions_with_parameter(Parameter::path_different_str("test/*")));

        parse_ok(input, expected);
    }

    #[test] fn test_deletions_with_extra_comma() {
        let input = r#"deletions(path!="test/*",)"#;
        let expected = one_feature(Feature::deletions_with_parameter(Parameter::path_different_str("test/*")));

        parse_ok(input, expected);
    }

    #[test] fn test_deletions_with_elapsed_time() {
        let input = "deletions.elapsedTime";
        let expected = one_feature(Feature::deletions_with_property(Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_deletions_with_elapsed_time_and_empty_parens_() {
        let input = "deletions().elapsedTime()";
        let expected = one_feature(Feature::deletions_with_property(Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_deletions_with_equal_path_filter_and_elapsed_time() {
        let input = r#"deletions(path=="test/*").elapsedTime"#;
        let expected = one_feature(Feature::deletions(vec![Parameter::path_equal_str("test/*")],
                                                      Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_deletions_with_different_path_filter_and_elapsed_time() {
        let input = r#"deletions(path!="test/*").elapsedTime"#;
        let expected = one_feature(Feature::deletions(vec![Parameter::path_different_str("test/*")],
                                                      Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_changes() {
        let input = "changes";
        let expected = one_feature(Feature::changes_simple());

        parse_ok(input, expected);
    }

    #[test] fn test_changes_with_empty_parens() {
        let input = "changes()";
        let expected = one_feature(Feature::changes_simple());

        parse_ok(input, expected);
    }

    #[test] fn test_changes_with_equal_path_filter() {
        let input = r#"changes(path=="test/*")"#;
        let expected = one_feature(Feature::changes_with_parameter(Parameter::path_equal_str("test/*")));

        parse_ok(input, expected);
    }

    #[test] fn test_changes_with_different_path_filter() {
        let input = r#"changes(path!="test/*")"#;
        let expected = one_feature(Feature::changes_with_parameter(Parameter::path_different_str("test/*")));

        parse_ok(input, expected);
    }

    #[test] fn test_changes_with_extra_comma() {
        let input = r#"changes(path!="test/*",)"#;
        let expected = one_feature(Feature::changes_with_parameter(Parameter::path_different_str("test/*")));

        parse_ok(input, expected);
    }

    #[test] fn test_changes_with_elapsed_time() {
        let input = "changes.elapsedTime";
        let expected = one_feature(Feature::changes_with_property(Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_changes_with_elapsed_time_and_empty_parens_() {
        let input = "changes().elapsedTime()";
        let expected = one_feature(Feature::changes_with_property(Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_changes_with_equal_path_filter_and_elapsed_time() {
        let input = r#"changes(path=="test/*").elapsedTime"#;
        let expected = one_feature(Feature::changes(vec![Parameter::path_equal_str("test/*")],
                                                    Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_changes_with_different_path_filter_and_elapsed_time() {
        let input = r#"changes(path!="test/*").elapsedTime"#;
        let expected = one_feature(Feature::changes(vec![Parameter::path_different_str("test/*")],
                                                    Property::ElapsedTime));

        parse_ok(input, expected);
    }

    #[test] fn test_and_connector_2() {
        let input = r#"commits && changes"#;
        let expected = conjunction(vec![Feature::commits_simple(),
                                        Feature::changes_simple()]);

        parse_ok(input, expected);
    }

    #[test] fn test_and_connector_3() {
        let input = r#"commits && changes && additions"#;
        let expected = conjunction(vec![Feature::commits_simple(),
                                        Feature::changes_simple(),
                                        Feature::additions_simple()]);

        parse_ok(input, expected);
    }
}