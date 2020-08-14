//pub mod ast;

use ghql::parse;

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
    println!("Available operators used on features to create selectors:");
    println!();
    println!("    FEATURE == NUMBER, NUMBER == FEATURE");
    println!("    FEATURE != NUMBER, NUMBER != FEATURE");
    println!("    FEATURE >  NUMBER, NUMBER >  FEATURE");
    println!("    FEATURE >= NUMBER, NUMBER >= FEATURE");
    println!("    FEATURE <  NUMBER, NUMBER <  FEATURE");
    println!("    FEATURE <= NUMBER, NUMBER <= FEATURE");
    println!();
    println!("Available connectives between feature selectors:");
    println!();
    println!("    and");
    println!();
    println!("Examples:");
    println!();
    println!("    commits >= 1000");
    println!();
    println!("    returns a list of projects which have at least 1000 commits");
    println!();
    println!("    deletions(\"README.md\")");
    println!();
    println!("    returns a list of projects which have at least one commit that deletes");
    println!("    a file called \"README.md\" from he project's root directory");
    println!();
    println!("    commits(path==\"*.rs\") >= 1000 and commits(path==\"*.rs\") <= 10000");
    println!();
    println!("    returns a list of projects which have between 1000 and 10000 commits");
    println!("    that touch a file whose name ends in \"rs\"");
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