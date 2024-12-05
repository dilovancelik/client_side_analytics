
use crate::data_model::{Column, Database};
use std::{fmt, str::FromStr};
use serde::{Serialize, Deserialize};
use wasm_bindgen::{prelude::wasm_bindgen, JsError};

#[derive(Serialize, Deserialize)]
struct Aggregation {
    column: Column,
    aggregation_type: AggregationType
}

impl fmt::Display for Aggregation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.aggregation_type == AggregationType::COUNT {
            write!(f, "count() {}", self.column.column)
        } else {
            write!(f, "{}({}) {}", self.aggregation_type, self.column, self.column.column)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Filter {
    column: Column,
    negate: bool,
    operator: OperatorType,
    values: Vec<String>
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let negate_str = if self.negate{ "NOT "} else { "" }; 
        match self.operator {
            OperatorType::EQUAL => {
                assert_eq!(self.values.len(), 1, "Equal operator should only have one value");            
                write!(f, "{} ({} = {})", negate_str, self.column, self.values.first().unwrap())
            },
            OperatorType::IN => {
                assert!(self.values.len() > 0, "In operator should have atleast one value");
                let values = self.values.join(", ");
                write!(f, "{} ({} in ({}))", negate_str, self.column, values)
            },
            OperatorType::BETWEEN => {
                assert_eq!(self.values.len(), 2, "Between operator should only have 2 values");
                write!(f, "{} ({} between {} and {})", negate_str, self.column, self.values[0], self.values[1])
            }
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum OperatorType {
    EQUAL,
    BETWEEN,
    IN
}

impl FromStr for OperatorType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "eq" => Ok(Self::EQUAL),
            "between" => Ok(Self::BETWEEN),
            "in" => Ok(Self::IN),
            _ => Err(())
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum AggregationType {
    SUM,
    AVG, 
    MIN,
    MAX,
    COUNT
}

impl FromStr for AggregationType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "sum" => Ok(AggregationType::SUM),
            "avg" => Ok(AggregationType::AVG),
            "min" => Ok(AggregationType::MIN),
            "max" => Ok(AggregationType::MAX),
            "count" => Ok(AggregationType::COUNT),
            _ => Err(())
        }
    }
}

impl fmt::Display for AggregationType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let outstring = match self {
            AggregationType::SUM => "sum",
            AggregationType::AVG => "avg",
            AggregationType::MIN => "min",
            AggregationType::MAX => "max",
            AggregationType::COUNT => "count"
        };
        write!(f, "{}", outstring)
    }
}

#[derive(Serialize, Deserialize)]
pub struct Query {
    labels: Vec<Column>,
    aggregations: Vec<Aggregation>,
    filters: Vec<Filter>
}

#[wasm_bindgen]
pub fn parse_json_query(json_query: String, json_data_model: String) -> Result<String, JsError> {
    let query_obj = match serde_json::from_str(json_query.as_str()) {
        Ok(query) => query,
        Err(e) => return Err(JsError::new(e.to_string().as_str()))
    };

    let model = match serde_json::from_str(json_data_model.as_str()) {
        Ok(model) => model,
        Err(e) => return Err(JsError::new(e.to_string().as_str()))
    };

    match sql_builder(query_obj, model) {
        Ok(query) => return Ok(query),
        Err(e) => return Err(JsError::new(e.to_string().as_str()))
    }
}

fn sql_builder(query: Query, data_model: Database) -> Result<String, JoinError> {
    
    let mut tables: Vec<String> = Vec::new();
    let mut aggregations: Vec<String> = Vec::new();
    let mut labels: Vec<String> = Vec::new();
    let mut filters: Vec<String> = Vec::new();
    
    query.aggregations.iter()
        .for_each(|agg| {
            if !tables.contains(&agg.column.table) { tables.push(agg.column.table.clone()); }
            if !aggregations.contains(&agg.to_string()) { aggregations.push(agg.to_string()); }
        });

    query.labels.iter()
        .for_each(|label| {
            if !tables.contains(&label.table) { tables.push(label.table.clone()) }
            if !labels.contains(&label.to_string()) { labels.push(label.to_string()); }
        });

    query.filters.iter()
        .for_each(|filter| if filters.contains(&filter.to_string()) { 
            filters.push(filter.to_string())
        });
    
    let from_stmt = match generate_from_stmt(tables, &data_model) {
        Ok(from_stmt) => from_stmt,
        Err(e) => return Err(e)
    };

    let mut select_stmt: String = String::from("SELECT ");
    select_stmt.push_str(labels.join(",\n\t").as_str());
    if labels.len() > 0 {
        select_stmt.push_str(",\n\t")
    }
    select_stmt.push_str(aggregations.join(",").as_str()); 

    let mut group_by_stmt: String = String::from("");
    if aggregations.len() > 0 {
        group_by_stmt.push_str("GROUP BY ");
        group_by_stmt.push_str(labels.join(",\n\t").as_str());
    }

    let mut where_stmt: String = String::from("");
    if filters.len() > 0 {
        let first_filter = filters.first().unwrap().to_string();
        where_stmt.push_str(format!("WHERE {}\n", first_filter).as_str());
        
        for filter in filters.iter().skip(1) {
            where_stmt.push_str(format!("\tAND {}\n", filter.to_string()).as_str()); 
        }

    }
    let query_str = format!("{}\n\n{}\n\n{}\n\n{}", select_stmt, from_stmt, where_stmt, group_by_stmt); 

    Ok(query_str)    
}

fn generate_from_stmt(tables: Vec<String>, data_model: &Database) -> Result<String, JoinError> {
    assert!(tables.len() > 0, "There must be atleast one table");

    let mut from_statement = String::new();
    let mut previous_tables = vec![tables.first().unwrap()];
    from_statement.push_str(format!("FROM {}\n", tables.first().unwrap()).as_str());
    
    for table in tables.iter().skip(1) {
        let mut table_joins = vec![format!("\tJOIN {} ON", table)];
        let relationships = match &data_model.tables.get(table) {
            Some(relationships) => &relationships.relationships,
            None => return Err(JoinError { message: format!("Table {} has no relationship, and cannot be included in joins", table) })
        };
        let mut joins: String = String::from("");
        for previous_table in &previous_tables {
            if let Some(_joins) = relationships.get(previous_table.as_str()) {
                let table_join_str = _joins 
                    .iter()
                    .map(|join| format!(" {}", &join.to_string()))
                    .collect::<Vec<String>>()
                    .join("\n\t\tAND");
                joins.push_str(table_join_str.as_str());
            
            } else {
                return Err(JoinError { message: format!("No relastionship exists between {} and {}", previous_table, table) });
            }
        }
        previous_tables.push(table);
        table_joins.push(joins);
        from_statement.push_str(table_joins.join("").as_str());
    }

    Ok(from_statement)
}


struct JoinError {
    message: String
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
