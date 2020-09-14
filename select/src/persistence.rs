use std::fs::File;
use std::io::Error;
use std::collections::{BTreeMap, HashMap};
use byteorder::{WriteBytesExt, ReadBytesExt, BigEndian};
use crate::objects::{CommitId, ProjectId, UserId, PathId, Project, Commit, User, Path, Message};
use std::hash::Hash;

pub trait Persistent {
    fn dump(&self, file: &mut File) -> Result<(),   Error>;
    fn load(       file: &mut File) -> Result<Self, Error> where Self: Sized;
}

impl Persistent for u64 {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        file.write_u64::<BigEndian>(*self)
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        file.read_u64::<BigEndian>()
    }
}

impl Persistent for i64 {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        file.write_i64::<BigEndian>(*self)
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        file.read_i64::<BigEndian>()
    }
}

impl Persistent for u128 {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        file.write_u128::<BigEndian>(*self)
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        file.read_u128::<BigEndian>()
    }
}

impl Persistent for usize {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        file.write_u64::<BigEndian>(*self as u64)
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        Ok(file.read_u64::<BigEndian>()? as usize)
    }
}

impl Persistent for u8 {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        file.write_u8(*self)
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        file.read_u8()
    }
}

impl Persistent for String {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        self.len().dump(file)?;
        for value in self.bytes() {
            value.dump(file)?
        }
        Ok(())
    }
    fn load(file: &mut File) -> Result<Self, Error> {
        let length = usize::load(file)?;
        let mut vector: Vec<u8> = Vec::with_capacity(length);
        for _ in 0..length {
            vector.push(u8::load(file)?)
        }
        std::str::from_utf8(vector.as_slice()).map_or_else(
            |e| Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
            |s| Ok(s.to_owned())
        )
    }
}

impl<T> Persistent for Option<T> where T: Persistent + Sized {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        match self {
            Some(value) => { 170u8.dump(file)?; value.dump(file) }
            None => { 0u8.dump(file) }
        }
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        match u8::load(file)? {
            0u8 => Ok(None),
            170u8 => Ok(Some(T::load(file)?)),
            _ => Err(std::io::Error::from(std::io::ErrorKind::InvalidData))
        }

    }
}

impl<K, V> Persistent for (K, V) where K: Persistent + Sized, V: Persistent + Sized {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        self.0.dump(file)?; self.1.dump(file)
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        Ok((K::load(file)?, V::load(file)?))
    }
}

impl<P> Persistent for Vec<P> where P: Persistent + Sized {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        self.len().dump(file)?;
        for value in self.iter() {
            value.dump(file)?
        }
        Ok(())
    }
    fn load(file: &mut File) -> Result<Self, Error> {
        let length = usize::load(file)?;
        let mut vector: Self = Vec::with_capacity(length);
        for _ in 0..length {
            vector.push(P::load(file)?)
        }
        Ok(vector)
    }
}

impl<K, V> Persistent for HashMap<K, V> where K: Persistent + Sized + Hash + Eq, V: Persistent + Sized {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        self.len().dump(file)?;
        for (key, value) in self.iter() {
            key.dump(file)?;
            value.dump(file)?;
        }
        Ok(())
    }
    fn load(file: &mut File) -> Result<Self, Error> {
        let length = usize::load(file)?;
        let mut map: Self = HashMap::new();
        for _ in 0..length {
            let key = K::load(file)?;
            let value = V::load(file)?;
            map.insert(key, value);
        }
        Ok(map)
    }
}

// TODO: we could save some space here by forgetting the ID
impl<K, V> Persistent for BTreeMap<K, V> where K: Persistent + Sized + Ord, V: Persistent + Sized {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        self.len().dump(file)?;
        for (key, value) in self.iter() {
            key.dump(file)?;
            value.dump(file)?;
        }
        Ok(())
    }
    fn load(file: &mut File) -> Result<Self, Error> {
        let length = usize::load(file)?;
        let mut tree: Self = BTreeMap::new();
        for _ in 0..length {
            let key = K::load(file)?;
            let value = V::load(file)?;
            tree.insert(key, value);
        }
        Ok(tree)
    }
}

//** ==== Object Persistence ==================================================================== **/
impl Persistent for ProjectId {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        self.0.dump(file)
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        u64::load(file).map(|n| ProjectId(n))
    }
}
impl Persistent for CommitId {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        self.0.dump(file)
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        u64::load(file).map(|n| CommitId(n))
    }
}
impl Persistent for UserId {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        self.0.dump(file)
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        u64::load(file).map(|n| UserId(n))
    }
}
impl Persistent for PathId {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        self.0.dump(file)
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        u64::load(file).map(|n| PathId(n))
    }
}

impl Persistent for Project {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        Ok({
            self.id.dump(file)?;
            self.url.dump(file)?;
            self.last_update.dump(file)?;
            self.language.dump(file)?;
            self.stars.dump(file)?;
            self.issues.dump(file)?;
            self.buggy_issues.dump(file)?;
            self.heads.dump(file)?;
            self.metadata.dump(file)?;
        })
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        Ok(Project {
            id: ProjectId::load(file)?,
            url: String::load(file)?,
            last_update: i64::load(file)?,
            language: Option::load(file)?,
            stars: Option::load(file)?,
            issues: Option::load(file)?,
            buggy_issues: Option::load(file)?,
            heads: Vec::load(file)?,
            metadata: HashMap::load(file)?,
        })
    }
}

impl Persistent for Commit {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        Ok({
            self.id.dump(file)?;
            self.hash.dump(file)?;
            self.author.dump(file)?;
            self.committer.dump(file)?;
            self.author_time.dump(file)?;
            self.committer_time.dump(file)?;
            self.additions.dump(file)?;
            self.deletions.dump(file)?;
        })
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        Ok(Commit{
            id: CommitId::load(file)?,
            hash: String::load(file)?,
            author: UserId::load(file)?,
            committer: UserId::load(file)?,
            author_time: i64::load(file)?,
            committer_time: i64::load(file)?,
            additions: Option::load(file)?,
            deletions: Option::load(file)?,
        })
    }
}

impl Persistent for User {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        Ok({
            self.id.dump(file)?;
            self.email.dump(file)?;
            self.name.dump(file)?;
        })
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        Ok(User {
            id: UserId::load(file)?,
            email: String::load(file)?,
            name: String::load(file)?,
        })
    }
}

impl Persistent for Path {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        Ok({
            self.id.dump(file)?;
            self.path.dump(file)?;
        })
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        Ok(Path{
            id: PathId::load(file)?,
            path: String::load(file)?,
        })
    }
}

impl Persistent for Message {
    fn dump(&self, file: &mut File) -> Result<(), Error> {
        self.contents.dump(file)
    }
    fn load(file: &mut File) -> Result<Self, Error> where Self: Sized {
        Ok(Message{
            contents: Vec::load(file)?,
        })
    }
}