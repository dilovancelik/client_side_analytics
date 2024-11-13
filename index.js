
import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm/+esm';

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

    result.toArray().map((row) => console.log(row.toJSON()));
    addTableToList(tableName)
    await c.close()
    fileText.textContent = "Table created, choose CSV, JSON or Parquet to create a new table"

}

window.executeQuery = async function () {
    var errorMessage = document.getElementById("error");
    errorMessage.innerHTML = "";
    var table = document.getElementById("result");
    table.innerHTML = "";
    var query = document.getElementById("query").value;
    const c = await db.connect();
    c.query(query)
        .then((result) => {
            console.log("Hello")
            showResult(result)
            console.log("World")
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
window.addTableToList = function (tableName) {
    var tableList = document.getElementById("tables")
    var tableItem = document.createElement("li")
    tableItem.appendChild(document.createTextNode(tableName))
    tableList.appendChild(tableItem)
}

