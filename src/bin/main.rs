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
    println!();
    println!("Available operators used on features:");
    println!();
    println!("    FEATURE == VALUE, VALUE == FEATURE");
    println!("    FEATURE != VALUE, VALUE != FEATURE");
    println!("    FEATURE >  VALUE, VALUE >  FEATURE");
    println!("    FEATURE >= VALUE, VALUE >= FEATURE");
    println!("    FEATURE <  VALUE, VALUE <  FEATURE");
    println!("    FEATURE <= VALUE, VALUE <= FEATURE");
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