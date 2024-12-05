use std::{collections::HashMap, fmt};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    pub table: String,
    pub column: String,
    pub data_type: String,
    pub description: Option<String>
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.table, self.column)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Join {
    from_column: Column,
    to_column: Column
}

impl fmt::Display for Join {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.from_column, self.to_column)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub description: Option<String>,
    pub columns: Vec<Column>,
    pub relationships: HashMap<String, Vec<Join>>
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Database {
    pub name: String,
    pub tables: HashMap<String, Table>
}
