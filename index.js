import init, { sql_parser_autocomplete } from "./webasm_module/pkg/webasm_module.js";
import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm/+esm';

await init();

const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();

// Select a bundle based on browser checks
const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
);

// Instantiate the asynchronus version of DuckDB-Wasm
const worker = new Worker(worker_url);
const logger = new duckdb.ConsoleLogger();
const db = new duckdb.AsyncDuckDB(logger, worker);
await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
URL.revokeObjectURL(worker_url);

var loadedTables = [];
window.suggestionButtonCounter = -1;

window.loadData = async function () {
    var fileText = document.getElementById("filetext")
    fileText.textContent = "Creating table..."
    const c = await db.connect();

    const file = document.getElementById("file").files[0];
    var fileNameArray = file.name.split(".");
    var extension = fileNameArray.pop();
    var tableName = fileNameArray.join("").replaceAll(" ", "_").replaceAll("-", "_");
    var content = await file.arrayBuffer();

    await db.registerFileBuffer(`/${file.name}`, new Uint8Array(content));

    var query = ""
    if (extension === "csv") {
        query = `CREATE TABLE ${tableName} AS FROM read_csv_auto('/${file.name}', header = true, sample_size=-1)`;
    } else if (extension === "json") {
        query = `CREATE TABLE ${tableName} AS FROM read_json_auto('/${file.name}')`;
    } else if (extension === "parquet") {
        query = `CREATE TABLE ${tableName}]AS FROM read_parquet('/${file.name}')`;
    }
    await c.query(query);
    const result = await c.query(`DESCRIBE ${tableName}`);

    addTableToList(tableName, result)
    saveTableMetadata(tableName, result)
    await c.close()
    loadedTables.push(tableName)
    fileText.textContent = "Table created, choose CSV, JSON or Parquet to create a new table"


}

const executeQuery = async () => {
    suggestionElement.innerHTML = "";
    var errorMessage = document.getElementById("error");
    errorMessage.innerHTML = "";
    var table = document.getElementById("result");
    table.innerHTML = "";
    var query = document.getElementById("query_textarea").value;
    const c = await db.connect();
    c.query(query)
        .then((result) => {
            showResult(result)
        })
        .catch((error) => {
            var errorMessage = document.getElementById("error")
            var htmlError = error.message.split("\n").map((line) => "> " + line).slice(0, -1).join("<br/>")
            errorMessage.innerHTML = htmlError
        })
        .finally(async () => {
            await c.close()
        })
}

function showResult(result) {
    var headers = [];
    var headerRow = document.createElement("tr");
    var table = document.getElementById("result")
    result.schema.fields.map((field) => {
        var header = document.createElement("th")
        header.innerHTML = field.name
        headerRow.appendChild(header)
        headers.push(field.name)
    })
    table.appendChild(headerRow)
    result.toArray().map((row) => {
        var tableRow = document.createElement("tr")
        headers.forEach((field) => {
            var cell = document.createElement("td")
            cell.innerHTML = row[field]
            tableRow.appendChild(cell)
        })
        table.appendChild(tableRow)
    })
}
window.addTableToList = function (tableName, description) {
    var tableList = document.getElementById("tables");
    var tableItem = createTableSchema(tableName, description)
    tableList.appendChild(tableItem)
}

const saveTableMetadata = (tableName, description) => {
    var metaData = {}
    description.toArray().map((row) => {
        var name = row["column_name"]
        metaData[name] = row.toJSON();
    })
    window.sessionStorage.setItem(tableName, JSON.stringify(metaData))
}
const createTableSchema = (tableName, description) => {
    var metadata = document.createElement("details");
    var title = document.createElement("summary");
    title.innerHTML = tableName
    metadata.appendChild(title);
    var list = document.createElement("ul"); 

    description.toArray().map((row) => {
        var nullable = ((row["null"] === "YES") ? "NULL" : "NOT NULL");
        var listitem = document.createElement("li");
        listitem.innerHTML = `<span class="tablename">${row["column_name"]}</span> <span class="tableproperties">(${row["column_type"]}, ${nullable})</span>`;
        list.appendChild(listitem);
    })
    metadata.appendChild(list);
    return metadata
}

var query_area = document.getElementById("query_textarea");
var suggestionElement = document.getElementById("auto_suggestions");

query_area.addEventListener("focusin", async () => {
    autocompleteConnection = await db.connect();
});
query_area.addEventListener("focusout", async () => {
    await autocompleteConnection.close()
});

var keydown_handled = false;
query_area.addEventListener("keyup", async (e) => {
    if (e.key === " " || keydown_handled) {
        return
    }
    const cursor_loc = query_area.selectionEnd;
    const query = query_area.value;
    let auto_query = sql_parser_autocomplete(query, loadedTables, cursor_loc);
    var str_query = `SELECT * FROM sql_auto_complete('${auto_query}') LIMIT 5;`
    var result = await autocompleteConnection.query(str_query)
    var startIndex = findIndexOfCurrentWord();
    var currentWord = query.substring(startIndex + 1 , cursor_loc);
    var word_length = cursor_loc - startIndex - 1;
    suggestionElement.innerHTML = "";
    suggestionButtonCounter = -1;
    result
        .toArray()
        .map((row) => row["suggestion"])
        .filter((suggestion) => suggestion.slice(0, word_length).toLowerCase() == currentWord.toLowerCase())
        .map((suggestion) => {
            const _suggestion = document.createElement("button");
            _suggestion.innerHTML = suggestion.replaceAll(" ", "");
            _suggestion.classList.add("suggestion")
            suggestionElement.appendChild(_suggestion);
    });

});

query_area.addEventListener("keydown", (e) => {
    if (e.key === "Tab" && !e.shiftKey) {
        e.preventDefault();
        keydown_handled = true;
        query_area.setRangeText(
            '    ',
            query_area.selectionStart,
            query_area.selectionStart,
            'end'
        )
        return false;
    } else if (e.shiftKey && e.key === "Tab") {
        e.preventDefault();
        keydown_handled = true;
        
        var suggestions = document.getElementById("auto_suggestions").childNodes;
        var previous_suggestion = suggestions[suggestionButtonCounter];
        if (suggestionButtonCounter === suggestions.length) {
            suggestionButtonCounter = -1;
        }
        suggestionButtonCounter += 1;
        var current_suggestion = suggestions[suggestionButtonCounter];
        if (previous_suggestion) {
            previous_suggestion.classList.remove("suggestion_active")
        }
        if (current_suggestion) {
            current_suggestion.classList.add("suggestion_active")
        }a
        // var currentsuggestion = suggestions.childNodes[suggestionButtonCounter];
        return false;
    } else if (e.key === "Enter" && document.getElementsByClassName("suggestion_active").length !== 0) {
        e.preventDefault();
        keydown_handled = true;
        var active_suggestion = document.getElementsByClassName("suggestion_active")[0];
        replaceCurrentWord(active_suggestion.textContent);
        suggestionElement.innerHTML = "";
        return false;
    } else if (e.key === "F5") {
        e.preventDefault();
        keydown_handled = true;
        executeQuery();
    } else {
        keydown_handled = false;
    }
    
});

const replaceCurrentWord = (newWord) => {
    const currentValue = query_area.value;
    const cursorPos = query_area.selectionStart;
    const startIndex = findIndexOfCurrentWord();

    const newValue = currentValue.substring(0, startIndex + 1) +
                    newWord +
                    currentValue.substring(cursorPos);
    query_area.value = newValue;
    query_area.focus();
    query_area.selectionStart = query_area.selectionEnd = startIndex + 1 + newWord.length;
};
var autocompleteConnection; 


const findIndexOfCurrentWord = () => {
    // Get current value and cursor position
    const currentValue = query_area.value;
    const cursorPos = query_area.selectionStart;

    // Iterate backwards through characters until we find a space or newline character
    let startIndex = cursorPos - 1;
    while (startIndex >= 0 && !/[.\s\[]/.test(currentValue[startIndex])) {
        startIndex--;
    }
    return startIndex;
};

document.getElementById("execute_button").addEventListener("click", () => {
    executeQuery();
});
