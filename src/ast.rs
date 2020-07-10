/**
 * A query consists of either:
 *  - a single feature: commits(...)
 *  - a tree of features joined by connectives: commit(...) && (files(...) && ...)
 * Since the tree is recursive, the query is boxed.
 */
#[derive(PartialEq,Debug,Clone)]
pub enum Query {
    Simple(Feature),
    Compound(Feature, Connective, Box<Query>),
}

/**
 * Connectives between features.
 */
#[derive(PartialEq,Debug,Clone)]
pub enum Connective {
    Conjunction,
}

#[derive(PartialEq,Debug,Clone)]
pub struct Feature {
    name: String,                               // replace with a tag if it matters for performance
    filters: Vector<Filter>,
    properties: Vector<Calculation>
}

#[derive(PartialEq,Debug,Clone)]
pub struct Filter {
    name: String,
    operator: Operator,
    value: Value,
}

#[derive(PartialEq,Debug,Clone)]
pub enum Property {
    ElapsedTime,
}

#[derive(PartialEq,Debug,Clone)]
pub enum StringOperator {
    Equal,
    Different,
}

#[derive(PartialEq,Debug,Clone)]
pub enum RelationalOperator {
    Equal,
    Different,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

#[derive(PartialEq,Debug,Clone)]
pub enum Value {
    Glob(String), // eg. "test/*"
}

pub enum FeatureTag {
    Commits,
    Additions,
    Deletions,
    Changes,
}