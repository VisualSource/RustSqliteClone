pub mod args;
pub mod execute;
pub mod meta;
pub mod prepare;

#[derive(Debug, PartialEq)]
pub enum Statement {
    /// insert into {TABLE} {COLLUMN-NAME?(,)} VALUES (expr?(,))
    Insert {
        cols: Vec<String>,
        data: Vec<String>,
        table: String,
    },
    Select {
        table: String,
        columns: Vec<String>,
    },
    Create {
        primary_key: usize,
        table: String,
        cols: Vec<(String, u8, bool)>,
    },
}
