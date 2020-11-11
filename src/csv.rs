macro_rules! create_file {
    ($location:expr) => {{
        let path = std::path::PathBuf::from($location.into());
        let dir_path = { let mut dir_path = path.clone(); dir_path.pop(); dir_path };
        std::fs::create_dir_all(&dir_path)?;
        std::fs::File::create(path)
    }}
}