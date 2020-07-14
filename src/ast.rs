#[derive(PartialEq,Debug,Clone)]
pub struct Query {
    //assignments: Vector<Assignment>
    expression: Expression,
}

#[derive(PartialEq,Debug,Clone)]
pub struct Expression {
    head: Feature,
    tail: Vec<(Connective, Feature)>,
}

#[derive(PartialEq,Debug,Copy,Clone)]
pub enum Connective {
    Conjunction,
    //Disjunction
}

#[derive(PartialEq,Debug,Clone)]
pub enum Feature {
    Commits   { parameters: Vec<Parameter>, property: Option<Property> },
    Additions { parameters: Vec<Parameter>, property: Option<Property> },
    Deletions { parameters: Vec<Parameter>, property: Option<Property> },
    Changes   { parameters: Vec<Parameter>, property: Option<Property> },
}

#[derive(PartialEq,Debug,Clone)]
pub enum Parameter {
    Path { operator: StringOperator, pattern: String }
}

#[derive(PartialEq,Debug,Copy,Clone)]
pub enum Property {
    ElapsedTime,
    //Percentile(u32),
}

#[derive(PartialEq,Debug,Copy,Clone)]
pub enum StringOperator {
    Equal,
    Different,
}

#[derive(PartialEq,Debug,Copy,Clone)]
pub enum RelationalOperator {
    Equal,
    Different,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

impl Query {
    pub fn simple (expression: Expression) -> Query {
        Query {expression}
    }
}

impl Expression {
    pub fn new (head: Feature, tail: Vec<(Connective, Feature)>) -> Expression {
        Expression { head, tail }
    }
    pub fn from_feature (head: Feature) -> Expression {
        Expression { head, tail: Vec::new() }
    }
    pub fn from_features (connective: Connective, mut features: Vec<Feature>) -> Result<Expression, String> {
        if features.length() < 1 {
            Err("At least one feature must be provided.");
        }

        let head: Feature = features.remove(0);
        let tail: Vec<(Connective, Feature)> =
            features.into_iter().map(|feature| (connective, feature)).collect();

        Ok(Expression { head, tail })
    }
}

impl Feature {
    pub fn commits_simple() -> Feature {
        Feature::Commits { parameters: Vec::new(), property: None }
    }
    pub fn commits_with_parameter(parameter: Parameter) -> Feature {
        Feature::Commits { parameters: vec![parameter], property: None }
    }
    pub fn commits_with_parameters(parameters: Vec<Parameter>) -> Feature {
        Feature::Commits { parameters, property: None }
    }
    pub fn commits_with_property(property: Property) -> Feature {
        Feature::Commits { parameters: Vec::new(), property: Some(property) }
    }
    pub fn commits(parameters: Vec<Parameter>, property: Property) -> Feature {
        Feature::Commits { parameters, property: Some(property) }
    }

    pub fn additions_simple() -> Feature {
        Feature::Additions { parameters: Vec::new(), property: None }
    }
    pub fn additions_with_parameter(parameter: Parameter) -> Feature {
        Feature::Additions { parameters: vec![parameter], property: None }
    }
    pub fn additions_with_parameters(parameters: Vec<Parameter>) -> Feature {
        Feature::Additions { parameters, property: None }
    }
    pub fn additions_with_property(property: Property) -> Feature {
        Feature::Additions { parameters: Vec::new(), property: Some(property) }
    }
    pub fn additions(parameters: Vec<Parameter>, property: Property) -> Feature {
        Feature::Additions { parameters, property: Some(property) }
    }

    pub fn deletions_simple() -> Feature {
        Feature::Deletions { parameters: Vec::new(), property: None }
    }
    pub fn deletions_with_parameter(parameter: Parameter) -> Feature {
        Feature::Deletions { parameters: vec![parameter], property: None }
    }
    pub fn deletions_with_parameters(parameters: Vec<Parameter>) -> Feature {
        Feature::Deletions { parameters, property: None }
    }
    pub fn deletions_with_property(property: Property) -> Feature {
        Feature::Deletions { parameters: Vec::new(), property: Some(property) }
    }
    pub fn deletions(parameters: Vec<Parameter>, property: Property) -> Feature {
        Feature::Deletions { parameters, property: Some(property) }
    }

    pub fn changes_simple() -> Feature {
        Feature::Changes { parameters: Vec::new(), property: None }
    }
    pub fn changes_with_parameter(parameter: Parameter) -> Feature {
        Feature::Changes { parameters: vec![parameter], property: None }
    }
    pub fn changes_with_parameters(parameters: Vec<Parameter>) -> Feature {
        Feature::Changes { parameters, property: None }
    }
    pub fn changes_with_property(property: Property) -> Feature {
        Feature::Changes { parameters: Vec::new(), property: Some(property) }
    }
    pub fn changes(parameters: Vec<Parameter>, property: Property) -> Feature {
        Feature::Changes { parameters, property: Some(property) }
    }
}

impl Parameter {
    pub fn path_equal(pattern: String) -> Parameter {
        Parameter::Path { operator: StringOperator::Equal, pattern }
    }
    pub fn path_different(pattern: String) -> Parameter {
        Parameter::Path { operator: StringOperator::Different, pattern }
    }
    pub fn path_equal_str(pattern: &str) -> Parameter {
        Parameter::Path { operator: StringOperator::Equal, pattern: pattern.to_string() }
    }
    pub fn path_different_str(pattern: &str) -> Parameter {
        Parameter::Path { operator: StringOperator::Different, pattern: pattern.to_string() }
    }
}

#[macro_export]
macro_rules! compose_parameters {
    ( $head: expr, $tail: expr ) => {{
        let mut parameters = Vec::new();
        parameters.push($head);
        let clean_tail: Vec<Parameter> = $tail.iter().map(|e| e.1.clone()).collect();
        parameters.extend(clean_tail);
        parameters
    }}
}

