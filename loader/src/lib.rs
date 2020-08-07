mod need;
mod filters;
mod tables;
mod preprocess;
mod selectors;
mod api; // this will be replaced with the api from the downloader
mod api_extensions;

extern crate ghql;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
