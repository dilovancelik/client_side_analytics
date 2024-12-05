mod statics;
mod query_builder;
mod data_model;

use core::str;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn add1000000000_wasm() -> i32 {
    let mut i: i32 = 0;
    while i < 1000000000 {
        i += 1;
    }
    return i;
}

#[wasm_bindgen]
pub fn fibonacci_wasm(n: i32) -> i32 {
    if n <= 0 {
        return 0;
    } else if n == 1 {
        return 1;
    } else {
        return fibonacci_wasm(n - 1) + fibonacci_wasm(n - 2);
    }
}

#[wasm_bindgen]
pub fn sql_parser_autocomplete(query: String, tables: Vec<String>, cursor_loc: usize) -> String {
    let split_query: Vec<_> = query.split_whitespace().collect();
    let mut result_query = query.clone().split_at_mut(cursor_loc - 1).0.to_string();
    let word_count = split_query.len();

    for table in &tables {

        let table_identifiers: Vec<_> = split_query
            .iter()
            .enumerate()
            .filter_map(|(index, &word)| if word.to_lowercase() == table.to_lowercase() { Some(index)} else { None } )
            .collect();
        for i in table_identifiers {
            if i >= word_count {
                continue;
            }
            let mut index = i + 1;
            if split_query[i+1] == "as" {
                index += 1; 
            }
            let identifier: &str = &split_query[index].to_lowercase();
            if !statics::DUCKDB_KEYWORDS.contains(&identifier) {
                result_query = result_query.replace(&format!(" {identifier}."), &format!(" {table}."));
            }
        }
    };

    return result_query;
}
