# A short guide to creating an attribute in Djanco

It's a bit of a laborious process and has a bit of boilerplate---some of which
unnecessary, but a lot of it in the servie of making the DSL look pretty. As an example we will create an attribute we'll call LastCommit, which will be the most recent commit in the project.

## Add attribute

Find the module for the entity you want to add an attribute to in lib.rs. There
are the following entities 

- `Project`s, 
- `Commit`s, 
- `Head`s, 
- `Change`s, 
- `User`s, 
- `Path`s, and 
- `Snapshot`s.

Module for Projects starts at line 546:

```
pub mod project {
    use crate::objects;
    use crate::time;
    use crate::attrib::*;
    use crate::Timestamp;
    use crate::Store;

    ...
```

An attribute is defined like this:

```
impl_attribute[SIGILS  PARENT_ENTITY, ATTRIBUTE_NAME, ATTRIBUTE_TYPE, METHOD_NAME]
```

**SIGILS**

Add an attribute via `impl_attribute` macro. It starts with up to three some
shorthand sigils:

`!` or `?`: `! are obligatory attributes that every project has and never
return `None`. `?` are atributes that always return an `Option` and can always
be `None`.

`+` or nothing: `+` are attributes that return one of the entity type
(Projects, Commits, etc.) which wraps them with a reference to the database.
Since attributes need access to the database to be retrieved, if an entity is
returned without `+`, it will not have all the attribute methods that it
normally would. You skip `+` for simple types like Strings and ints.

`..` or nothing: `..` indicates that it returns a collection---it returns a
vector (possibly empty). These atributes are automatically countable in the
DSL.  If you skip `..` it returns a single value of some type.

**PARENT_ENTITY**

Which entity to attach the attribute to? `objects::T` where `T` is the name of
the struct that describes the entity: `objects::Project`, `objects::Commit`,
etc.

**ATTRIBUTE NAME**

The name of the attribute. This will be the name of the struct that describes
the attribute in the DSL. It should start with a capital letter and be camel
case. The macro generates this attribute.

**ATTRIBUTE_TYPE**

The type of the attribute's value. Let's say this is some type `T`, then the
actual value returned by the attribute will depend on the sigils used to define it:

`!` -> `T`    
`!+` -> `ItemWithData<T>` (wrapped in a rewference to the Database)    
`!..` -> `Vec<T>`  
`!+..` -> `Vec<ItemWithData<T>>`  
`?` -> `Option<T>`   
`?+` -> `Option<ItemWithData<T>>`   
`?..` -> `Option<Vec<T>>`  
`?+..` -> `Option<Vec<ItemWithData<T>>>`  

**METHOD NAME**

The name of the method that gets this value in `ItemWithData<PARENT_ENTITY>`.
The method takes no arguments. In the cases of `..` attributes, there are two
methods listed: one retrieves the elements, and the other retrieves just the
element count.

**Example**

LastCommit is declared like this.

```
impl_attribute![?+    objects::Project, LastCommit, objects::Commit, last_commit];
```

The sygil is `?+` because the project might not have commits (`?`), and it
returns an entity, which should then be quariable, so it should be wrapped in
ItemWithData (`+`).

## Add a method to the entity struct

We add a method to retrieve the attribute value from the database. This is just
boilerbaplte. It requires adding a method in two structs in `objects.rs`. Let's
say we're adding something to Projects, then we add a method to
`objects::Project` (line 346) and `objects::ItemWithData<Project>` (line 706). 

The method we add in `objects::ItemwithData<T>` is called the same as
METHOD_NAME above, has only &self as an argument and returns the same type as
ATTRIBUTE_TYPE, so:

`!` -> `T`    
`!+` -> `ItemWithData<T>`
`?` -> `Option<T>`   
`?+` -> `Option<ItemWithData<T>>`   

In case of `..` attributes there are two methods. The first one returns the
elements:

`!..` -> `Vec<T>`  
`!+..` -> `Vec<ItemWithData<T>>`  
`?..` -> `Option<Vec<T>>`  
`?+..` -> `Option<Vec<ItemWithData<T>>>`  

The second one returns the count of the element and has the return type of:

`!..` -> `usize`  
`!+..` -> `usize`  
`?..` -> `Option<usize>`  
`?+..` -> `Option<usize>`  

Typically this method is meant to call a method of the same name in
`PARENT_ENTITY` (So the function in `objects::ItemWithData<Project>::bla` calls
the function in `objects::Project::bla`). The method has the same name and type
as the one above (more or less, we can do some conversion here), but it has
another argument of type `&Database`. We pass `self.data` via this argument.
The parent entity is available via `self.item`. 

Example:

```
impl<'a> ItemWithData<'a, Project> {

    //...

    pub fn last_commit<'b>(&'b self) -> Option<ItemWithData<'a,Commit>> { 
        self.item.last_commit(&self.data).attach_data_to_inner(self.data)          
    }

    //...
}    
```

The other method calls the database to retrieve the data for this particular
object. The object is relatively small, containig an ID adn all information
that is unavoidable to load. We pass this ID to the database. The method in the
database does not exist yet, so we do that next. 

Example:

```
impl Project {

    //...

    pub fn last_commit(&self, db: &Database) -> Option<Commit> { 
        db.last_commit(&self.id)                
    }
    
    //...
}
```

## Database method 

The objects are thin and all the actual work is performed in trhe database
object. The database has internal mutability, so more boilerplate here.

In `data.rs` in `Database` (line 92) we add the method called in the method we
did previously. It has the same return type (roughly). Inside it, we just call
a method of the same name (with prepended name of the entity) on the internal
mutable object (`Data`).

We also pass in a refernece to a source, which is the datastore of the 

Example:

```
impl Database {
    //...

    pub fn last_commit(&self, id: &ProjectId) -> Option<Commit> {
        self.data.borrow_mut().project_last_commit(&self.source, id)
    }
    
    //...
}
```

(Rust note: remember this `borrow_mut` means references cannot pass from the
inside to the outside.)

Then, we put the method into `Data` in the same file (line 899). This method
grabs a map that has all the values for this attribute in the database and
retrieves a single element from it. (It may need to clone the result because of
the aforementioned `borrow_mut`---there's a method added to all
Option<Cloneable> types called `pirate` that does that).

The map may or may not be loaded into memory. We call a function that loads it
if there is a need to. These are called smart_load_ATTRIBUTE_NAME.

```
impl Data {

    //...

    pub fn project_last_commit(&mut self, source: &Source, id: &ProjectId) -> Option<Commit> {
        self.smart_load_project_last_commit(source).get(id).pirate()
    }

    //...
}  
```

The method smart_load_ATRIBUTE_NAME will call a dirty macro that generates a
bunch of additional code. There are two macros it may call. 

`load_from_source!` will load the data into the map directly from the Parasite
datastore and nothing else. It takes three parameters: self, the name of the
map where the data is to be loaded, and the reference to the source (Parasite
instance). The method returns a BTreeMap reference from the ID of PARENT_ENTITY
to ATTRIBUTE_TYPE.

Example to load straight from source (line 1146):

```
impl Data {
    // ...
    
    fn smart_load_project_last_commit(&mut self, source: &Source) -> &BTreeMap<ProjectId, Commit> {
        load_from_source!(self, project_last_commit, source)
    }

    // ...
}
```

`load_with_prerequisites!` will load data from the source and other attributes.
It will make sure that the prerequisite attributes are also loaded into memory
before computing our new attribute. There are a variable number of parameters.
There's always self, the name of the map, and the reference to source. Then
there is the number of prerequisite attributes given as `one` or `two` or
`three`.  There was never a need for more so far, so these are the only
alternatives for now. Let me know if you need more. Then, we list the names of
the maps that we will use to compute this attribute.

Example to compute from prerequisites:

```
impl Data {
    // ...
    
    fn smart_load_project_last_commit(&mut self, source: &Source) -> &BTreeMap<ProjectId, Commit> {
        load_with_prerequisites!(self, project_last_commit, source, two, commits, project_commits)
    }

    // ...
}
```

## Create Persistent Map


This method requires that we have a map inside of `Data` to put the data (line 773). The
map should be of type `PersistentMap`. The type of the map is generic on an
Extractor, which we will define later.

Example:

```
pub(crate) struct Data {
    //...

    project_last_commit: PersistentMap<ProjectLastCommitExtractor>,

    //...
}
```

Then, we make sure the map is initialized when `Data` is constructed (line
822). The constructor to PersistentMap has a string indicating its name, a copy
of the constructor, and acopy of the path to the Database's cache. 

You can turn off caching for specific maps by calling `without_cache` on them,
but since our attributes will probably involve a lot of calculation, it will
almost always be better to cache them, so don't bother.

Example:

```
    impl Data {
    pub fn new(/*source: DataSource,*/ cache_dir: CacheDir, log: Log) -> Data {
        let dir = cache_dir.as_string();
        Data {
            // ...

            project_last_commit: PersistentMap::new("project_last_commit", log.clone(),dir.clone()),

            // ...
        }
```

## Create an Extractor

This is where the heavy lifting happens. The extractor is a class that
specifies how the calculate the actual attribute. It is called when needed and
given the prerequisites we asked for with the dirty macros
`load_with_prerequisites!` and `load_from_source!`. It creates a BTreeMap
containing the values of the attribute for each entity. (See line 300-770 for
existing implementations).

An extractor is essentially an empty struct with a specific name that
implements a couple of Rust traits.

The first trait to implement is a `MapExtractor`. A `MapExtractor` spercifies
what the `Key` and `Value` of the map are. The key is an ID of an entity, and
the value is a value of type ATTRIBUTE_TYPE.

Example:

```
impl MapExtractor for ProjectLastCommitExtractor {
    type Key = ProjectId;
    type Value = Commit;
}
```

The second trait to implement depends on the number of prerequisites we have
(only source, or `one`/`two`/`three` prerequisites). 

If we have only source (we're using `load_from_source!`, then we implement
`SingleMapExtractor`. A `SingleMapExtractor` has one associated type called `A`
and defines an associated function:

```
extract(a: &Self::A) -> BTreeMap<Self::Key, Self::Value>;
```

For us, `A` will be type `Source` (containing a reference to Parasite
datastore). The extract function will use Parasite's methods to construct the
output map.

Example:

```
impl SingleMapExtractor for ProjectLastCommitExtractor {
    type A = Source;
    extract(source: &Self::A) -> BTreeMap<Self::Key, Self::Value> { 
        // Key=ProjectId, Value=Commit, defined in MapExtractor
        // I would not advise doing it just from source in this case :p
        unimplemented!()
    }
}
```

In most cases, the attributes are calculated from prerequisites. For this we
need Single<a[Extractor, DoubleMapExtarctor, and TripleMapExtractor traits.
They work similarly to SingleMapExtractor with source, but they have more
associated types and more arguments in the extract function. Source is not
passed to them, just the prerequisites.

```
pub trait SingleMapExtractor: MapExtractor {
    type A;
    fn extract(a: &Self::A) -> BTreeMap<Self::Key, Self::Value>;
}

pub trait DoubleMapExtractor: MapExtractor {
    type A; type B;
    fn extract(a: &Self::A, b: &Self::B) -> BTreeMap<Self::Key, Self::Value>;
}

pub trait TripleMapExtractor: MapExtractor {
    type A; type B; type C;
    fn extract(a: &Self::A, b: &Self::B, c: &Self::C) -> BTreeMap<Self::Key, Self::Value>;
}
```

Example:

```
impl DoubleMapExtractor for ProjectLastCommitExtractor {
    type A = BTreeMap<CommitId, Commit>;
    type B = BTreeMap<ProjectId, Vec<CommitId>>
    fn extract(commits: &Self::A, project_commits: &Self::B) -> BTreeMap<Self::Key, Self::Value> {

       // I didn't test this snippet 
       project_commits.iter().map(|(project_id, commit_ids)| {
           commit_ids.iter().flat_map(|commit_id| {
               commits.get(commit_id).map(|commit| {
                   (commit.author_timestamp(), commit.clone())) // get commit timestamp
               })
           })
           .sorted() // sort by timestamp
           .last() // get last
           .map(|(_, commit)| commit) // ditch timestamp
       })
       .collect() // convert iterator to BTreeMap
    }
}
```

## Et voilá!

Now we should be able to sue the attribute in queries:

```
database.projects()
    .map_into(Select!(project::Itself, project::LastCommit))
    .into_csv("bla.csv")
```

