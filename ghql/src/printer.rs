use crate::ast::*;

pub trait Source {
    fn to_source(&self) -> String;
}

impl Source for Query {
    fn to_source(&self) -> String {
        self.expressions.to_source()
    }
}

impl Source for Expressions {
    fn to_source(&self) -> String {
        let mut string = String::new();
        string.push_str(&self.head.to_source());
        for (connective, feature) in self.tail.iter() {
            string.push_str(" ");
            string.push_str(connective.to_source().as_str());
            string.push_str(" ");
            string.push_str(feature.to_source().as_str());
        }
        return string
    }
}

impl Source for Expression {
    fn to_source(&self) -> String {
        let mut string = String::new();

        match self {
            Expression::Simple(feature) => {
                string.push_str(feature.to_source().as_str())
            },

            Expression::Compound { operator, left, right } => {
                string.push_str(left.to_source().as_str());
                string.push_str(" ");
                string.push_str(operator.to_source().as_str());
                string.push_str(" ");
                string.push_str(right.to_source().as_str());
            },
        }

        return string;
    }
}

impl Source for Connective {
    fn to_source(&self) -> String {
        match self {
            Connective::Conjunction => "and".to_string()
        }
    }
}

impl Source for Operand {
    fn to_source(&self) -> String {
        match self {
            Operand::Feature(feature) => feature.to_source(),
            Operand::Number(number)   => number.to_string(),
        }
    }
}

impl Feature {
    fn parameters_to_source(parameters: &Vec<Parameter>) -> String {
        let mut string = String::new();
        if parameters.len() > 0 {
            string.push_str("(");
            let strings: Vec<String> = parameters.iter().map(|p| p.to_source()).collect();
            string.push_str(strings.join(", ").as_str());
            string.push_str(")");
        }
        return string;
    }

    fn property_to_source(property: &Option<Property>) -> String {
        let mut string = String::new();
        if let Some(actual) = property {
            string.push_str(".");
            string.push_str(actual.to_source().as_str());
        }
        return string;
    }
}

impl Source for Feature {
    fn to_source(&self) -> String {
        let mut string = String::new();

        match self {
            Feature::Commits { parameters, property } => {
                string.push_str("commits");
                string.push_str(Feature::parameters_to_source(parameters).as_str());
                string.push_str(Feature::property_to_source(property).as_str())
            }

            Feature::Additions { parameters, property } => {
                string.push_str("additions");
                string.push_str(Feature::parameters_to_source(parameters).as_str());
                string.push_str(Feature::property_to_source(property).as_str())
            }

            Feature::Deletions { parameters, property } => {
                string.push_str("deletions");
                string.push_str(Feature::parameters_to_source(parameters).as_str());
                string.push_str(Feature::property_to_source(property).as_str())
            }

            Feature::Changes { parameters, property } => {
                string.push_str("changes");
                string.push_str(Feature::parameters_to_source(parameters).as_str());
                string.push_str(Feature::property_to_source(property).as_str())
            }
        }
        return string;
    }
}

impl Source for RelationalOperator {
    fn to_source(&self) -> String {
        match self {
            RelationalOperator::Equal        => "==".to_string(),
            RelationalOperator::Different    => "!=".to_string(),
            RelationalOperator::Less         =>  "<".to_string(),
            RelationalOperator::LessEqual    => "<=".to_string(),
            RelationalOperator::Greater      =>  ">".to_string(),
            RelationalOperator::GreaterEqual => ">=".to_string(),
        }
    }
}

impl Source for StringOperator {
    fn to_source(&self) -> String {
        match self {
            StringOperator::Equal            => "==".to_string(),
            StringOperator::Different        => "!=".to_string(),
        }
    }
}

impl Source for Parameter {
    fn to_source(&self) -> String {
        match self {
            Parameter::Path { operator, pattern } => {
                let mut string = String::new();
                string.push_str("path");
                string.push_str(" ");
                string.push_str(operator.to_source().as_str());
                string.push_str(" ");
                string.push_str(r#"""#);
                string.push_str(pattern.as_str());
                string.push_str(r#"""#);
                return string;
            }
        }
    }
}

impl Source for Property {
    fn to_source(&self) -> String {
        match self {
            Property::ElapsedTime => "elapsedTime".to_string(),
        }
    }
}
