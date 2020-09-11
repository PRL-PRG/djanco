#[derive(Clone, Copy, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub enum LogLevel { Quiet, Verbose }

#[macro_export]
macro_rules! log {
    ($level:expr, $message:expr) => {
        match $level {
            LogLevel::Quiet => {},
            LogLevel::Verbose => { eprintln!("{}", $message) },
        }
    }
}

#[macro_export]
macro_rules! log_header {
    ($level:expr, $message:expr) => {
        match $level {
            LogLevel::Quiet => {},
            LogLevel::Verbose => { eprintln!("{}...", $message) },
        }
    }
}

#[macro_export]
macro_rules! log_item {
    ($level:expr, $message:expr) => {
        match $level {
            LogLevel::Quiet => {},
            LogLevel::Verbose => { eprintln!("  - {}", $message) },
        }
    }
}

#[macro_export]
macro_rules! log_addendum {
    ($level:expr, $message:expr) => {
        match $level {
            LogLevel::Quiet => {},
            LogLevel::Verbose => { eprintln!("    {}", $message) },
        }
    }
}