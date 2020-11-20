use djanco::objects::{Project, ProjectId};

struct A {}
struct B { b: usize }
struct C { c: String }
struct D<'a> { d: &'a usize }
struct E { e: Vec<usize> }
struct F { e: Option<usize> }

fn main() {
    println!("usize {}", std::mem::size_of::<usize>());
    println!("A     {}", std::mem::size_of::<A>());
    println!("B     {}", std::mem::size_of::<B>());
    println!("C     {}", std::mem::size_of::<C>());
    println!("D     {}", std::mem::size_of::<D>());
    println!("E     {}", std::mem::size_of::<E>());
    println!("ProjectId {}", std::mem::size_of::<ProjectId>());
    println!("String {}", std::mem::size_of::<String>());
    println!("Project {}", std::mem::size_of::<Project>());
    println!("F {}", std::mem::size_of::<Option<usize>>());
}
