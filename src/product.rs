use itertools::Itertools;
use std::vec::IntoIter;

pub trait CartesianMegaproduct<T> {
    fn into_megaproduct(self) -> IntoIter<Vec<T>>;
}

impl<I,T> CartesianMegaproduct<T> for I where I: Iterator<Item=Vec<Vec<T>>>, T: Clone {
    fn into_megaproduct(self) -> IntoIter<Vec<T>> {
        megaproduct(self.collect()).into_iter()
    }
}

fn combine_rows<T>(mut a: Vec<T>, mut b: Vec<T>) -> Vec<T> {
    let mut combined = Vec::new();
    combined.append(&mut a);
    combined.append(&mut b);
    combined
}

pub fn megaproduct<T>(mut vector_of_objects: Vec<Vec<Vec<T>>>) -> Vec<Vec<T>> where T: Clone {
    let number_of_objects = vector_of_objects.len();
    if number_of_objects == 0usize {
        return Vec::new()
    }
    if number_of_objects == 1usize {
        return vector_of_objects.pop().unwrap()
    }

    // let number_of_items_from_each_object: Vec<usize> =
    //     vector_of_objects.iter().map(|v| v.len()).collect();
    // // if number_of_items_from_each_object.iter().all(|length| *length == 1usize) { // this part is broken
    // //     return vector_of_objects.into_iter().map(|v| {
    // //         //v.into_iter().flat_map(|v| v).collect::<Vec<T>>()
    // //     }).collect()
    // // }
    // if number_of_items_from_each_object.iter().all(|length| *length == 0usize) {
    //     return Vec::new()
    // }

    let mut first: bool = true;
    let mut accumulator: Vec<Vec<T>> = vec![];
    for vector_of_rows in vector_of_objects {
        accumulator = if first {
            first = false;
            vector_of_rows
        } else {
            accumulator.into_iter()
                .cartesian_product(vector_of_rows.into_iter())
                .map(|(accumulator_element, row_element)| {
                    combine_rows(accumulator_element, row_element)
                }).collect::<Vec<Vec<T>>>()
        }
    }
    accumulator
}

#[cfg(test)]
mod test {
    use crate::product::CartesianMegaproduct;

    #[test]
    fn test1() {
        let object_1_row_1 = vec!["1".to_owned()];
        let object_1_row_2 = vec!["2".to_owned()];
        let object_1_row_3 = vec!["3".to_owned()];
        let object_1 = vec![object_1_row_1, object_1_row_2, object_1_row_3];

        let test = vec![object_1];
        let product: Vec<Vec<String>> = test.into_iter().into_megaproduct().collect();
        for row in product.iter() {
            println!("{:?}", row);
        }

        let expected_result = vec![
            vec!["1".to_owned()],
            vec!["2".to_owned()],
            vec!["3".to_owned()],
        ];

        assert_eq!(expected_result, product);
    }

    #[test]
    fn test2() {
        let object_1_row_1 = vec!["1".to_owned()];
        let object_1 = vec![object_1_row_1];

        let object_2_row_1 = vec!["a".to_owned()];
        let object_2 = vec![object_2_row_1];

        let test = vec![object_1, object_2];
        let product: Vec<Vec<String>> = test.into_iter().into_megaproduct().collect();
        for row in product.iter() {
            println!("{:?}", row);
        }

        let expected_result = vec![
            vec!["1".to_owned(), "a".to_owned()],
        ];

        assert_eq!(expected_result, product);
    }

    #[test]
    fn test3() {
        let object_1_row_1 = vec!["1".to_owned()];
        let object_1_row_2 = vec!["2".to_owned()];
        let object_1_row_3 = vec!["3".to_owned()];
        let object_1 = vec![object_1_row_1, object_1_row_2, object_1_row_3];

        let object_2_row_1 = vec!["a".to_owned()];
        let object_2_row_2 = vec!["b".to_owned()];
        let object_2_row_3 = vec!["c".to_owned()];
        let object_2 = vec![object_2_row_1, object_2_row_2, object_2_row_3];

        let object_3_row_1 = vec!["x".to_owned()];
        let object_3_row_2 = vec!["y".to_owned()];
        let object_3_row_3 = vec!["z".to_owned()];
        let object_3 = vec![object_3_row_1, object_3_row_2, object_3_row_3];

        let test = vec![object_1, object_2, object_3];
        let product: Vec<Vec<String>> = test.into_iter().into_megaproduct().collect();
        for row in product.iter() {
            println!("{:?}", row);
        }

        let expected_result = vec![
            vec!["1".to_owned(), "a".to_owned(), "x".to_owned()],
            vec!["1".to_owned(), "a".to_owned(), "y".to_owned()],
            vec!["1".to_owned(), "a".to_owned(), "z".to_owned()],
            vec!["1".to_owned(), "b".to_owned(), "x".to_owned()],
            vec!["1".to_owned(), "b".to_owned(), "y".to_owned()],
            vec!["1".to_owned(), "b".to_owned(), "z".to_owned()],
            vec!["1".to_owned(), "c".to_owned(), "x".to_owned()],
            vec!["1".to_owned(), "c".to_owned(), "y".to_owned()],
            vec!["1".to_owned(), "c".to_owned(), "z".to_owned()],
            vec!["2".to_owned(), "a".to_owned(), "x".to_owned()],
            vec!["2".to_owned(), "a".to_owned(), "y".to_owned()],
            vec!["2".to_owned(), "a".to_owned(), "z".to_owned()],
            vec!["2".to_owned(), "b".to_owned(), "x".to_owned()],
            vec!["2".to_owned(), "b".to_owned(), "y".to_owned()],
            vec!["2".to_owned(), "b".to_owned(), "z".to_owned()],
            vec!["2".to_owned(), "c".to_owned(), "x".to_owned()],
            vec!["2".to_owned(), "c".to_owned(), "y".to_owned()],
            vec!["2".to_owned(), "c".to_owned(), "z".to_owned()],
            vec!["3".to_owned(), "a".to_owned(), "x".to_owned()],
            vec!["3".to_owned(), "a".to_owned(), "y".to_owned()],
            vec!["3".to_owned(), "a".to_owned(), "z".to_owned()],
            vec!["3".to_owned(), "b".to_owned(), "x".to_owned()],
            vec!["3".to_owned(), "b".to_owned(), "y".to_owned()],
            vec!["3".to_owned(), "b".to_owned(), "z".to_owned()],
            vec!["3".to_owned(), "c".to_owned(), "x".to_owned()],
            vec!["3".to_owned(), "c".to_owned(), "y".to_owned()],
            vec!["3".to_owned(), "c".to_owned(), "z".to_owned()],
        ];

        assert_eq!(expected_result, product);
    }

}