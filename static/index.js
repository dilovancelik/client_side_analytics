import init, { sql_parser_autocomplete } from "../webasm_module/pkg/webasm_module.js";
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
var suggestionButtonCounter = -1;
window.chartData = {};

const loadData = async () => {
    const start = Date.now();
    var table_result = document.getElementById("table_result_text");
    table_result.innerHTML = "";
    table_result.classList.remove("query_error");
    var fileText = document.getElementById("filetext");

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
        query = `CREATE TABLE ${tableName} AS FROM read_csv_auto('/${file.name}', header = true)`;
    } else if (extension === "json") {
        query = `CREATE TABLE ${tableName} AS FROM read_json_auto('/${file.name}')`;
    } else if (extension === "parquet") {
        query = `CREATE TABLE ${tableName} AS FROM read_parquet('/${file.name}')`;
    }
    c.query(query)
        .then(async () => {
            c.query(`DESCRIBE ${tableName}`)
                .then((result) => {
                    addTableToList(tableName, result)
                    saveTableMetadata(result)
                    loadedTables.push(tableName)
                    fileText.textContent = "Table created, choose CSV, JSON or Parquet to create a new table"
                    const end = Date.now();
                    table_result.innerHTML = `Table created in ${Math.floor((end - start) / 1000)} seconds.`;
                })
                .catch((error) => {
                    console.log(error);
                    var htmlError = error.message.split("\n").slice(0, -1).join("<br/>");
                    table_result.classList.add("query_error");
                    table_result.innerHTML = htmlError;
                    fileText.textContent = "Table error creation, try again.";
                });
        })
        .catch((error) => {
            console.log("hello")
            var htmlError = error.message.split("\n").slice(0, -1).join("<br/>");
            table_result.classList.add("query_error");
            table_result.innerHTML = htmlError;
            fileText.textContent = "Table error creation, try again.";
        })
        .finally(async () => {
            await c.close();
        });
}

const executeQuery = async () => {
    var header_area = document.getElementById("result_header");
    header_area.style.display = "block";
    const start = Date.now();
    var result_text = document.getElementById("query_result_text");
    result_text.innerHTML = "";
    result_text.classList.remove("query_error");
    suggestionElement.innerHTML = "";
    var button = document.getElementById("execute_button");
    button.textContent = "Executing..."
    var table = document.getElementById("table_result");
    table.innerHTML = "";
    var chart = document.getElementById("chart_result");
    chart.innerHTML = "";
    var query = document.getElementById("query_textarea").value;
    const c = await db.connect();
    c.query(query)
        .then((result) => {
            const end = Date.now();
            var csvLink = createCSV(result);
            var jsonLink = createJSON(result);
            var query_text = document.createElement("small")
            query_text.innerHTML = `Query finished in ${Math.floor((end - start) / 1000)} seconds, showing first 100 rows. You can download the entire result here:`;
            result_text.appendChild(query_text);
            result_text.appendChild(csvLink);
            result_text.appendChild(jsonLink);
            showResult(result)
        })
        .catch((error) => {
            var htmlError = error.message.split("\n").map((line) => "> " + line).join("<br/>");
            result_text.classList.add("query_error");
            result_text.innerHTML = htmlError;
            console.log(htmlError);
            console.log(error);
        })
        .finally(async () => {
            await c.close()
            button.textContent = "Execute"
        })
}

const showResult = (result) => {
    var header_area = document.getElementById("result_header");
    header_area.style.display = "block";
    var headers = [];
    var result_type_buttons = document.getElementById("result_type");
    result_type_buttons.style.display = "inline-block";
    var headerRow = document.createElement("tr");
    var table = document.getElementById("table_result");
    var result_area = document.getElementById("result_output");
    result.schema.fields.map((field) => {
        var header = document.createElement("th");
        header.innerHTML = field.name;
        headerRow.appendChild(header);
        headers.push(field.name);
    })
    table.appendChild(headerRow);
    result.toArray().slice(0, 99).map((row) => {
        var tableRow = document.createElement("tr")
        headers.forEach((field) => {
            var cell = document.createElement("td")
            cell.innerHTML = row[field]
            tableRow.appendChild(cell)
        })
        table.appendChild(tableRow)
    })
    result_area.style.display = "inline-block";
    document.getElementsByName("chart_area").forEach((x) => x.style.display = "none");
    generateChartData(result);
    //generateChart(result);
}

const createJSON = (result) => {
    let content = "data:text/json;charset=utf-8,[\n";
    result.toArray()
        .map((row) => {
            content += `${row.toString()},\n`
        })
    content = content.slice(0, -2);
    content += "\n]"
    var encodedUri = encodeURI(content);
    var link = document.createElement("a");
    link.setAttribute("href", encodedUri);
    link.setAttribute("download", "result.json");
    link.innerHTML = "<small>Download JSON</small>"
    link.style.padding = "5px"
    return link;
}
const createCSV = (result) => {
    let content = "data:text/csv;charset=utf-8,";
    result.schema.fields
        .map((field) => field["name"])
        .map((header) => content += `${header},`);
    content += "\n";
    content += result.toArray()
        .map((row) => row.toArray().join(","))
        .join("\n");
    var encodedUri = encodeURI(content);
    var link = document.createElement("a");
    link.setAttribute("href", encodedUri);
    link.setAttribute("download", "result.csv");
    link.innerHTML = "<small>Download CSV</small>"
    link.style.padding = "5px"
    return link;
}

const addTableToList = (tableName, description) => {
    var tableList = document.getElementById("tables");
    var tableItem = createTableSchema(tableName, description)
    tableList.appendChild(tableItem)
}

const saveTableMetadata = (description) => {
    var metaData = {}
    description.toArray().map((row) => {
        var name = row["column_name"]
        metaData[name] = row.toJSON();
    })
}

const createTableSchema = (tableName, description) => {
    var metadata = document.createElement("details");
    var title = document.createElement("summary");
    title.innerHTML = tableName
    metadata.appendChild(title);
    var list = document.createElement("table");

    description.toArray().map((row) => {
        var nullable = ((row["null"] === "YES") ? "NULL" : "NOT NULL");
        var listitem = document.createElement("tr");
        listitem.innerHTML = `<small>${row["column_name"]} (${row["column_type"]}, ${nullable})</small>`;
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
    var currentWord = query.substring(startIndex + 1, cursor_loc);
    var word_length = cursor_loc - startIndex - 1;
    suggestionElement.innerHTML = "";
    suggestionButtonCounter = -1;
    result
        .toArray()
        .map((row) => row["suggestion"])
        .filter((suggestion) => suggestion.slice(0, word_length).toLowerCase() == currentWord.toLowerCase())
        .map((suggestion) => {
            const _suggestion = document.createElement("ins");
            _suggestion.innerHTML = suggestion.replaceAll(" ", "");
            _suggestion.style.padding = "10px";
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
        }
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

const generateChartData = (result) => {
    chartData = {}
    
    var x_value = document.getElementById("xvalue");
    var y_values = document.getElementById("yvalues");
    x_value.innerHTML = "";
    y_values.innerHTML = "";
    
    const label_title = document.createElement("option");
    label_title.textContent = "Select Label"
    label_title.disabled = true;
    x_value.appendChild(label_title);

    const y_title = document.createElement("option");
    y_title.textContent = "Select Y values";
    y_title.disabled = true;
    y_values.appendChild(y_title);

    result
        .schema
        .fields
        .map((field) => {
            chartData[field.name] = { "type": field.type.toString(), "data": []};
            const label_value = document.createElement("option");
            label_value.textContent = field.name;
            label_value.value = field.name;
            x_value.appendChild(label_value);

            const y_value = document.createElement("option");
            y_value.value = field.name;
            y_value.textContent = field.name;
            y_values.appendChild(y_value);
        })
    result
        .toArray()
        .map((row) => Object.keys(chartData).map((field) => chartData[field]["data"].push(row[field])));

};

const generateChart = () => {
    const ctx = document.getElementById("chart_result");
    var chart_type = document.getElementById("chart_type").value;
    if (Chart.getChart("chart_result")) {
        Chart.getChart("chart_result").destroy();
    };
    
    var chart_label = document.getElementById("xvalue").value;
    const fields = Array.from(document.getElementById("yvalues").selectedOptions).map((x) => x.value);
    document.querySelectorAll('input[id="chart_y_value"]:checked').forEach((x) => fields.push(x.name));

    var labels = chartData[chart_label]["data"];
    var datasets = [];
    fields.map((field) => datasets[field] = datasets.push({
        label: field,
        data: chartData[field]["data"],
        borderWidth: 1
    }));
    console.log(datasets)

    new Chart(ctx, {
        type: chart_type,
        data: {
            labels: labels,
            datasets: datasets
        },
        options: {
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
};

const showChart = () => {
    const chart = document.getElementsByName("chart_area");
    const table = document.getElementById("table_result");
    chart.forEach((x) => x.style.display = "block");
    table.style.display = "none";
}

const showTable = () => {
    const chart = document.getElementsByName("chart_area");
    const table = document.getElementById("table_result");
    chart.forEach((x) => x.style.display = "none");
    table.style.display = "block";
}

document.getElementById("execute_button").addEventListener("click", () => {
    executeQuery();
});

document.getElementById("file").addEventListener("change", () => {
    loadData();
})

document.getElementById("chart_type_result").addEventListener("click", (e) => {
    e.preventDefault();
    showChart();
})

document.getElementById("table_type_result").addEventListener("click", (e) => {
    e.preventDefault();
    showTable();
})


document.getElementById("generate_chart_button").addEventListener("click", () => {
    generateChart();
})
