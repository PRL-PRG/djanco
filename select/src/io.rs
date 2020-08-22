use std::fs::{File, create_dir_all};
use std::path::{Path, PathBuf};
use std::io::Error;
use byteorder::{ReadBytesExt, BigEndian, WriteBytesExt};
use itertools::Itertools;

pub(crate) fn read_vec_vec_u64(path: &Path) -> Result<Vec<Vec<u64>>, Error> {
    let mut file = File::open(path)?;
    let num_rows = file.read_u64::<BigEndian>()?;
    let mut vector: Vec<Vec<u64>> = Vec::with_capacity(num_rows as usize);
    for _ in 0..num_rows {
        match file.read_u64::<BigEndian>() {
            Ok(num_columns) => {
                let mut row = Vec::with_capacity(num_columns as usize);
                for _ in 0..num_columns {
                    row.push(file.read_u64::<BigEndian>()?);
                }
                vector.push(row);
            },
            Err(e) => return Err(e),
        }
    }
    Ok(vector)
}

#[allow(dead_code)]
pub(crate) fn write_vec_vec_u64_from<I>(path: &PathBuf, iterator: I) -> Result<(), Error> where I: Iterator<Item=(u64, Vec<u64>)>{
    let mut dir = path.clone();
    dir.pop();
    create_dir_all(&dir)?;
    let iterator = iterator
        .sorted_by_key(|(row_id, _row)| *row_id);
    let mut file = File::create(path)?;
    file.write_u64::<BigEndian>(iterator.len() as u64)?;
    for (_, row) in iterator {
        file.write_u64::<BigEndian>(row.len() as u64)?;
        for element in row.iter() {
            file.write_u64::<BigEndian>(*element)?;
        }
    }
    Ok(())
}


pub(crate) fn write_vec_vec_u64(path: &PathBuf, vector: &Vec<Vec<u64>>) -> Result<(), Error> {
    let mut dir = path.clone();
    dir.pop();
    create_dir_all(&dir)?;
    let mut file = File::create(path)?;
    file.write_u64::<BigEndian>(vector.len() as u64)?;
    for row in vector.iter() {
        file.write_u64::<BigEndian>(row.len() as u64)?;
        for element in row.iter() {
            file.write_u64::<BigEndian>(*element)?;
        }
    }
    Ok(())
}