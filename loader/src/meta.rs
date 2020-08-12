use dcd::Project;

pub type Language = String;

pub trait ProjectMeta {
    fn get_stars(&self)             -> Option<u64>;
    fn get_stars_or_zero(&self)     -> u64;
    fn get_language(&self)          -> Option<String>;
    fn get_language_or_empty(&self) -> String;
}

impl ProjectMeta for Project {
    fn get_stars(&self) -> Option<u64> {
        self.metadata.get("stars").map(|s| s.parse().unwrap())
    }

    fn get_stars_or_zero(&self) -> u64 {
        match self.metadata.get("stars") {
            Some(s) => s.parse::<u64>().unwrap(),
            None => 0u64,
        }
    }

    fn get_language(&self) -> Option<String> {
        self.metadata.get("ght_language").map(|s| s.trim().to_string())
    }

    fn get_language_or_empty(&self) -> String {
        match self.metadata.get("ght_language") {
            Some(s) => s.trim().to_string(),
            None => String::new(),
        }
    }
}