mod ast;

#[macro_use]
extern crate lalrpop_util;

lalrpop_mod!(pub grammar); // synthesized by LALRPOP

pub fn parse(input: &str) -> Result<ast::Query, String> {
    match grammar::QUERYParser::new().parse(input) {
        Err(e) => Err(format!("{:?}", e)),
        Ok(ast) => Ok(ast),
    }
}

fn main() {
    println!("{:?}", parse("commits"));
}

#[cfg(test)]
mod tests {
    use crate::parse;
    use crate::ast::{Query, Expression, Feature, Parameter, Property};

    fn wrap(feature: Feature) -> Query {
        Query::simple(Expression::head(feature))
    }

    fn parse_ok(input: &str, expected: Query) {
        assert_eq!(parse(input), Ok(expected));
    }

    #[test] fn test_commits() {
        parse_ok("commits",
                 wrap(Feature::commits_simple()))
    }

    #[test] fn test_commits_with_empty_parens() {
        parse_ok("commits()",
                 wrap(Feature::commits_simple()))
    }

    #[test] fn test_commits_with_equal_path_filter() {
        let parameter = Parameter::path_equal_str("test/*");
        parse_ok(r#"commits(path=="test/*")"#,
                 wrap(Feature::commits_with_parameter(parameter)))
    }

    #[test] fn test_commits_with_different_path_filter() {
        let parameter = Parameter::path_different_str("test/*");
        parse_ok(r#"commits(path!="test/*")"#,
                 wrap(Feature::commits_with_parameter(parameter)))
    }

    #[test] fn test_commits_with_extra_comma() {
        let parameter = Parameter::path_different_str("test/*");
        parse_ok(r#"commits(path!="test/*",)"#,
                 wrap(Feature::commits_with_parameter(parameter)))
    }

    #[test] fn test_commits_with_elapsed_time() {
        parse_ok("commits.elapsedTime",
                 wrap(Feature::commits_with_property(Property::ElapsedTime)))
    }

    #[test] fn test_commits_with_elapsed_time_and_empty_parens_() {
        parse_ok("commits().elapsedTime()",
                 wrap(Feature::commits_with_property(Property::ElapsedTime)))
    }

    #[test] fn test_commits_with_equal_path_filter_and_elapsed_time() {
        let parameter = Parameter::path_equal_str("test/*");
        parse_ok(r#"commits(path=="test/*").elapsedTime"#,
                 wrap(Feature::commits(vec![parameter], Property::ElapsedTime)))
    }

    #[test] fn test_commits_with_different_path_filter_and_elapsed_time() {
        let parameter = Parameter::path_different_str("test/*");
        parse_ok(r#"commits(path!="test/*").elapsedTime"#,
                 wrap(Feature::commits(vec![parameter], Property::ElapsedTime)))
    }

    #[test] fn test_additions() {
        parse_ok("additions",
                 wrap(Feature::additions_simple()))
    }

    #[test] fn test_additions_with_empty_parens() {
        parse_ok("additions()",
                 wrap(Feature::additions_simple()))
    }

    #[test] fn test_additions_with_equal_path_filter() {
        let parameter = Parameter::path_equal_str("test/*");
        parse_ok(r#"additions(path=="test/*")"#,
                 wrap(Feature::additions_with_parameter(parameter)))
    }

    #[test] fn test_additions_with_different_path_filter() {
        let parameter = Parameter::path_different_str("test/*");
        parse_ok(r#"additions(path!="test/*")"#,
                 wrap(Feature::additions_with_parameter(parameter)))
    }

    #[test] fn test_additions_with_extra_comma() {
        let parameter = Parameter::path_different_str("test/*");
        parse_ok(r#"additions(path!="test/*",)"#,
                 wrap(Feature::additions_with_parameter(parameter)))
    }

    #[test] fn test_additions_with_elapsed_time() {
        parse_ok("additions.elapsedTime",
                 wrap(Feature::additions_with_property(Property::ElapsedTime)))
    }

    #[test] fn test_additions_with_elapsed_time_and_empty_parens_() {
        parse_ok("additions().elapsedTime()",
                 wrap(Feature::additions_with_property(Property::ElapsedTime)))
    }

    #[test] fn test_additions_with_equal_path_filter_and_elapsed_time() {
        let parameter = Parameter::path_equal_str("test/*");
        parse_ok(r#"additions(path=="test/*").elapsedTime"#,
                 wrap(Feature::additions(vec![parameter], Property::ElapsedTime)))
    }

    #[test] fn test_additions_with_different_path_filter_and_elapsed_time() {
        let parameter = Parameter::path_different_str("test/*");
        parse_ok(r#"additions(path!="test/*").elapsedTime"#,
                 wrap(Feature::additions(vec![parameter], Property::ElapsedTime)))
    }

    #[test] fn test_deletions() {
        parse_ok("deletions",
                 wrap(Feature::deletions_simple()))
    }

    #[test] fn test_deletions_with_empty_parens() {
        parse_ok("deletions()",
                 wrap(Feature::deletions_simple()))
    }

    #[test] fn test_deletions_with_equal_path_filter() {
        let parameter = Parameter::path_equal_str("test/*");
        parse_ok(r#"deletions(path=="test/*")"#,
                 wrap(Feature::deletions_with_parameter(parameter)))
    }

    #[test] fn test_deletions_with_different_path_filter() {
        let parameter = Parameter::path_different_str("test/*");
        parse_ok(r#"deletions(path!="test/*")"#,
                 wrap(Feature::deletions_with_parameter(parameter)))
    }

    #[test] fn test_deletions_with_extra_comma() {
        let parameter = Parameter::path_different_str("test/*");
        parse_ok(r#"deletions(path!="test/*",)"#,
                 wrap(Feature::deletions_with_parameter(parameter)))
    }

    #[test] fn test_deletions_with_elapsed_time() {
        parse_ok("deletions.elapsedTime",
                 wrap(Feature::deletions_with_property(Property::ElapsedTime)))
    }

    #[test] fn test_deletions_with_elapsed_time_and_empty_parens_() {
        parse_ok("deletions().elapsedTime()",
                 wrap(Feature::deletions_with_property(Property::ElapsedTime)))
    }

    #[test] fn test_deletions_with_equal_path_filter_and_elapsed_time() {
        let parameter = Parameter::path_equal_str("test/*");
        parse_ok(r#"deletions(path=="test/*").elapsedTime"#,
                 wrap(Feature::deletions(vec![parameter], Property::ElapsedTime)))
    }

    #[test] fn test_deletions_with_different_path_filter_and_elapsed_time() {
        let parameter = Parameter::path_different_str("test/*");
        parse_ok(r#"deletions(path!="test/*").elapsedTime"#,
                 wrap(Feature::deletions(vec![parameter], Property::ElapsedTime)))
    }

    #[test] fn test_changes() {
        parse_ok("changes",
                 wrap(Feature::changes_simple()))
    }

    #[test] fn test_changes_with_empty_parens() {
        parse_ok("changes()",
                 wrap(Feature::changes_simple()))
    }

    #[test] fn test_changes_with_equal_path_filter() {
        let parameter = Parameter::path_equal_str("test/*");
        parse_ok(r#"changes(path=="test/*")"#,
                 wrap(Feature::changes_with_parameter(parameter)))
    }

    #[test] fn test_changes_with_different_path_filter() {
        let parameter = Parameter::path_different_str("test/*");
        parse_ok(r#"changes(path!="test/*")"#,
                 wrap(Feature::changes_with_parameter(parameter)))
    }

    #[test] fn test_changes_with_extra_comma() {
        let parameter = Parameter::path_different_str("test/*");
        parse_ok(r#"changes(path!="test/*",)"#,
                 wrap(Feature::changes_with_parameter(parameter)))
    }

    #[test] fn test_changes_with_elapsed_time() {
        parse_ok("changes.elapsedTime",
                 wrap(Feature::changes_with_property(Property::ElapsedTime)))
    }

    #[test] fn test_changes_with_elapsed_time_and_empty_parens_() {
        parse_ok("changes().elapsedTime()",
                 wrap(Feature::changes_with_property(Property::ElapsedTime)))
    }

    #[test] fn test_changes_with_equal_path_filter_and_elapsed_time() {
        let parameter = Parameter::path_equal_str("test/*");
        parse_ok(r#"changes(path=="test/*").elapsedTime"#,
                 wrap(Feature::changes(vec![parameter], Property::ElapsedTime)))
    }

    #[test] fn test_changes_with_different_path_filter_and_elapsed_time() {
        let parameter = Parameter::path_different_str("test/*");
        parse_ok(r#"changes(path!="test/*").elapsedTime"#,
                 wrap(Feature::changes(vec![parameter], Property::ElapsedTime)))
    }
}