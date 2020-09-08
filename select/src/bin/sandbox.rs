use dcd::Project;

fn main() {
    fn my_query(projects: impl Iterator<Item=Project>) -> impl Iterator<Item=Project> {
        Box::new(projects)
    }

    fn bla<F,I>(f: F) -> () where F: FnMut(I) -> I, I: Iterator<Item=Project> {
        let projects: Vec::<Project> = vec![];
        let selection: Vec::<Project> = f(projects.iter()).collect();

    }

    bla(my_query)
}