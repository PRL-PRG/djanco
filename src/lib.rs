pub mod ast;
//pub mod printer;

#[macro_use]
extern crate lalrpop_util;

lalrpop_mod!(pub grammar); // synthesized by LALRPOP

pub fn parse(input: &str) -> Result<ast::Query, String> {
    match grammar::QUERYParser::new().parse(input) {
        Ok(ast) => Ok(ast),
        Err(e)  => Err(format!("{:?}", e)),
    }
}

#[cfg(test)]
mod tests {
    use crate::parse;
    use crate::ast::*;

    fn one_feature(feature: Feature) -> Query {
        one_expression(Expression::from_feature(feature))
    }

    fn one_expression(expression: Expression) -> Query {
        Query::simple(Expressions::from_expression(expression))
    }

    fn one_comparison(operator: RelationalOperator, feature: Feature, number: i64) -> Query {
        let left = Operand::from_feature(feature);
        let right = Operand::from_number(number);
        let expression = Expression::new(operator, left, right);
        Query::simple(Expressions::from_expression(expression))
    }

    fn feature_conjunction(features: Vec<Feature>) -> Query {
        assert!(features.len() > 1);
        expression_conjunction(features.into_iter().map(|f| Expression::from_feature(f)).collect())
    }

    fn expression_conjunction(expressions: Vec<Expression>) -> Query {
        assert!(expressions.len() > 1);
        Query::simple(Expressions::from_expressions(Connective::Conjunction, expressions).unwrap())
    }

    fn comparison_conjunction(comparisons: Vec<(Feature, RelationalOperator, i64)>) -> Query {
        assert!(comparisons.len() > 1);
        let expressions = comparisons.into_iter().map(|(f, o, n)| {
            Expression::new(o, Operand::from_feature(f), Operand::from_number(n))
        }).collect();
        Query::simple(Expressions::from_expressions(Connective::Conjunction, expressions).unwrap())
    }

    fn parse_ok(input: &str, expected: Query) {
        assert_eq!(parse(input), Ok(expected));
    }

    #[test] fn test_commits() {
        let input = "commits";
        let expected = one_feature(Feature::commits_simple());

        parse_ok(input,expected);
    }

    #[test] fn test_commits_equal_something() {
        let input = "commits == 42";
        let expected = one_comparison(RelationalOperator::Equal,
                                      Feature::commits_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_commits_different_something() {
        let input = "commits != 42";
        let expected = one_comparison(RelationalOperator::Different,
                                      Feature::commits_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_commits_less_something() {
        let input = "commits < 42";
        let expected = one_comparison(RelationalOperator::Less,
                                      Feature::commits_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_commits_less_equal_something() {
        let input = "commits <= 42";
        let expected = one_comparison(RelationalOperator::LessEqual,
                                      Feature::commits_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_commits_greater_something() {
        let input = "commits > 42";
        let expected = one_comparison(RelationalOperator::Greater,
                                      Feature::commits_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_commits_greater_equal_something() {
        let input = "commits >= 42";
        let expected = one_comparison(RelationalOperator::GreaterEqual,
                                      Feature::commits_simple(), 42);

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

    #[test] fn test_commits_with_filter_equal_something() {
        let input = r#"commits(path=="test/*") == 42"#;
        let expected = one_comparison(RelationalOperator::Equal,
                                      Feature::commits_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_commits_with_filter_different_something() {
        let input = r#"commits(path=="test/*") != 42"#;
        let expected = one_comparison(RelationalOperator::Different,
                                      Feature::commits_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_commits_with_filter_less_something() {
        let input = r#"commits(path=="test/*") < 42"#;
        let expected = one_comparison(RelationalOperator::Less,
                                      Feature::commits_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_commits_with_filter_less_equal_something() {
        let input = r#"commits(path=="test/*") <= 42"#;
        let expected = one_comparison(RelationalOperator::LessEqual,
                                      Feature::commits_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_commits_with_filter_greater_something() {
        let input = r#"commits(path=="test/*") > 42"#;
        let expected = one_comparison(RelationalOperator::Greater,
                                      Feature::commits_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_commits_with_filter_greater_equal_something() {
        let input = r#"commits(path=="test/*") >= 42"#;
        let expected = one_comparison(RelationalOperator::GreaterEqual,
                                      Feature::commits_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
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

    #[test] fn test_additions_equal_something() {
        let input = "additions == 42";
        let expected = one_comparison(RelationalOperator::Equal,
                                      Feature::additions_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_additions_different_something() {
        let input = "additions != 42";
        let expected = one_comparison(RelationalOperator::Different,
                                      Feature::additions_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_additions_less_something() {
        let input = "additions < 42";
        let expected = one_comparison(RelationalOperator::Less,
                                      Feature::additions_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_additions_less_equal_something() {
        let input = "additions <= 42";
        let expected = one_comparison(RelationalOperator::LessEqual,
                                      Feature::additions_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_additions_greater_something() {
        let input = "additions > 42";
        let expected = one_comparison(RelationalOperator::Greater,
                                      Feature::additions_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_additions_greater_equal_something() {
        let input = "additions >= 42";
        let expected = one_comparison(RelationalOperator::GreaterEqual,
                                      Feature::additions_simple(), 42);

        parse_ok(input,expected);
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

    #[test] fn test_additions_with_filter_equal_something() {
        let input = r#"additions(path=="test/*") == 42"#;
        let expected = one_comparison(RelationalOperator::Equal,
                                      Feature::additions_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_additions_with_filter_different_something() {
        let input = r#"additions(path=="test/*") != 42"#;
        let expected = one_comparison(RelationalOperator::Different,
                                      Feature::additions_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_additions_with_filter_less_something() {
        let input = r#"additions(path=="test/*") < 42"#;
        let expected = one_comparison(RelationalOperator::Less,
                                      Feature::additions_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_additions_with_filter_less_equal_something() {
        let input = r#"additions(path=="test/*") <= 42"#;
        let expected = one_comparison(RelationalOperator::LessEqual,
                                      Feature::additions_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_additions_with_filter_greater_something() {
        let input = r#"additions(path=="test/*") > 42"#;
        let expected = one_comparison(RelationalOperator::Greater,
                                      Feature::additions_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_additions_with_filter_greater_equal_something() {
        let input = r#"additions(path=="test/*") >= 42"#;
        let expected = one_comparison(RelationalOperator::GreaterEqual,
                                      Feature::additions_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
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

    #[test] fn test_deletions_equal_something() {
        let input = "deletions == 42";
        let expected = one_comparison(RelationalOperator::Equal,
                                      Feature::deletions_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_deletions_different_something() {
        let input = "deletions != 42";
        let expected = one_comparison(RelationalOperator::Different,
                                      Feature::deletions_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_deletions_less_something() {
        let input = "deletions < 42";
        let expected = one_comparison(RelationalOperator::Less,
                                      Feature::deletions_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_deletions_less_equal_something() {
        let input = "deletions <= 42";
        let expected = one_comparison(RelationalOperator::LessEqual,
                                      Feature::deletions_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_deletions_greater_something() {
        let input = "deletions > 42";
        let expected = one_comparison(RelationalOperator::Greater,
                                      Feature::deletions_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_deletions_greater_equal_something() {
        let input = "deletions >= 42";
        let expected = one_comparison(RelationalOperator::GreaterEqual,
                                      Feature::deletions_simple(), 42);

        parse_ok(input,expected);
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

    #[test] fn test_deletions_with_filter_equal_something() {
        let input = r#"deletions(path=="test/*") == 42"#;
        let expected = one_comparison(RelationalOperator::Equal,
                                      Feature::deletions_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_deletions_with_filter_different_something() {
        let input = r#"deletions(path=="test/*") != 42"#;
        let expected = one_comparison(RelationalOperator::Different,
                                      Feature::deletions_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_deletions_with_filter_less_something() {
        let input = r#"deletions(path=="test/*") < 42"#;
        let expected = one_comparison(RelationalOperator::Less,
                                      Feature::deletions_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_deletions_with_filter_less_equal_something() {
        let input = r#"deletions(path=="test/*") <= 42"#;
        let expected = one_comparison(RelationalOperator::LessEqual,
                                      Feature::deletions_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_deletions_with_filter_greater_something() {
        let input = r#"deletions(path=="test/*") > 42"#;
        let expected = one_comparison(RelationalOperator::Greater,
                                      Feature::deletions_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_deletions_with_filter_greater_equal_something() {
        let input = r#"deletions(path=="test/*") >= 42"#;
        let expected = one_comparison(RelationalOperator::GreaterEqual,
                                      Feature::deletions_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
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

    #[test] fn test_changes_equal_something() {
        let input = "changes == 42";
        let expected = one_comparison(RelationalOperator::Equal,
                                      Feature::changes_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_changes_different_something() {
        let input = "changes != 42";
        let expected = one_comparison(RelationalOperator::Different,
                                      Feature::changes_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_changes_less_something() {
        let input = "changes < 42";
        let expected = one_comparison(RelationalOperator::Less,
                                      Feature::changes_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_changes_less_equal_something() {
        let input = "changes <= 42";
        let expected = one_comparison(RelationalOperator::LessEqual,
                                      Feature::changes_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_changes_greater_something() {
        let input = "changes > 42";
        let expected = one_comparison(RelationalOperator::Greater,
                                      Feature::changes_simple(), 42);

        parse_ok(input,expected);
    }

    #[test] fn test_changes_greater_equal_something() {
        let input = "changes >= 42";
        let expected = one_comparison(RelationalOperator::GreaterEqual,
                                      Feature::changes_simple(), 42);

        parse_ok(input,expected);
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

    #[test] fn test_changes_with_filter_equal_something() {
        let input = r#"changes(path=="test/*") == 42"#;
        let expected = one_comparison(RelationalOperator::Equal,
                                      Feature::changes_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_changes_with_filter_different_something() {
        let input = r#"changes(path=="test/*") != 42"#;
        let expected = one_comparison(RelationalOperator::Different,
                                      Feature::changes_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_changes_with_filter_less_something() {
        let input = r#"changes(path=="test/*") < 42"#;
        let expected = one_comparison(RelationalOperator::Less,
                                      Feature::changes_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_changes_with_filter_less_equal_something() {
        let input = r#"changes(path=="test/*") <= 42"#;
        let expected = one_comparison(RelationalOperator::LessEqual,
                                      Feature::changes_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_changes_with_filter_greater_something() {
        let input = r#"changes(path=="test/*") > 42"#;
        let expected = one_comparison(RelationalOperator::Greater,
                                      Feature::changes_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
    }

    #[test] fn test_changes_with_filter_greater_equal_something() {
        let input = r#"changes(path=="test/*") >= 42"#;
        let expected = one_comparison(RelationalOperator::GreaterEqual,
                                      Feature::changes_with_parameter(Parameter::path_equal_str("test/*")),
                                      42);

        parse_ok(input,expected);
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
        let input = r#"commits and changes"#;
        let expected = feature_conjunction(vec![Feature::commits_simple(),
                                                Feature::changes_simple()]);

        parse_ok(input, expected);
    }

    #[test] fn test_and_connector_3() {
        let input = r#"commits and changes and additions"#;
        let expected = feature_conjunction(vec![Feature::commits_simple(),
                                                Feature::changes_simple(),
                                                Feature::additions_simple()]);

        parse_ok(input, expected);
    }


    #[test] fn test_and_connector_comparisons_2() {
        let input = r#"commits == 42 and changes != 42"#;
        let expected = comparison_conjunction(
            vec![(Feature::commits_simple(), RelationalOperator::Equal,     42),
                 (Feature::changes_simple(), RelationalOperator::Different, 42)]);

        parse_ok(input, expected);
    }

    #[test] fn test_and_connector_comparisons_3() {
        let input = r#"commits == 42 and changes != 42 and additions < 42"#;
        let expected = comparison_conjunction(
            vec![(Feature::commits_simple(),   RelationalOperator::Equal,     42),
                 (Feature::changes_simple(),   RelationalOperator::Different, 42),
                 (Feature::additions_simple(), RelationalOperator::Less,      42)]);

        parse_ok(input, expected);
    }
}