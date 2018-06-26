"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Bigtable = require("@google-cloud/bigtable");
class BigtableClient {
    constructor(config) {
        this.config = config;
    }
    async init() {
        const { projectId, keyFilename, instanceName, tableName, columnFamilyName } = this.config;
        const bigtable = new Bigtable({
            projectId,
            keyFilename
        });
        const instance = bigtable.instance(instanceName);
        const instanceExists = await instance.exists();
        if (!instanceExists || !instanceExists[0]) {
            await instance.create();
        }
        this.table = instance.table(tableName);
        const tableExists = await this.table.exists();
        if (!tableExists || !tableExists[0]) {
            await this.table.create(tableName);
        }
        this.columnFamily = this.table.family(columnFamilyName);
        const columnFamilyExists = await this.columnFamily.exists();
        if (!columnFamilyExists || !columnFamilyExists[0]) {
            await this.columnFamily.create({
                versions: 1
            });
        }
        this.cfName = this.columnFamily.familyName;
    }
    getParsedValue(value) {
        let result = value;
        try {
            result = JSON.parse(value);
        }
        catch (error) {
            // DO Nothing
        }
        return result;
    }
    getCfName() {
        return this.cfName;
    }
    async getRow(key) {
        const row = this.table.row(key + "");
        const result = { key };
        let rowGet = null;
        try {
            rowGet = await row.get();
        }
        catch (error) {
            if (!error.message.startsWith("Unknown row")) {
                throw error;
            }
            // Set the result to null if it throws at row.get - Error: Unknown row
            return null;
        }
        if (rowGet &&
            rowGet[0] &&
            rowGet[0].data &&
            rowGet[0].data[this.cfName]) {
            const row = rowGet[0].data[this.cfName];
            Object.keys(row).forEach(column => {
                if (row[column] && row[column][0] && row[column][0].value) {
                    result[column] = this.getParsedValue(row[column][0].value);
                }
            });
        }
        return result;
    }
    deleteCells(key, cells) {
        if (!cells.length) {
            return;
        }
        const row = this.table.row(key);
        const cellIndentifiers = cells
            .map((cell) => `${this.cfName}:${cell}`);
        return row.deleteCells(cellIndentifiers);
    }
    async scanCells(filter, etl) {
        return new Promise((resolve, reject) => {
            const results = [];
            this.table.createReadStream({
                filter
            })
                .on("error", (error) => {
                reject(error);
            })
                .on("data", (result) => {
                results.push(etl ? etl(result) : result);
            })
                .on("end", (result) => {
                resolve(results);
            });
        });
    }
    async insertRow(object) {
        if (!object.data || typeof object.data !== "object") {
            return;
        }
        const data = {};
        const dataKeys = Object.keys(object.data);
        dataKeys.map((key) => {
            const value = object.data[key];
            const sanitizedValue = (value && typeof value === "object") ?
                JSON.stringify(value) : value;
            data[key] = sanitizedValue || "";
        });
        return this.table.insert([{
                key: object.key,
                data: {
                    [this.cfName]: data
                }
            }]);
    }
    async increaseRow(object) {
        const promises = [];
        const row = this.table.row(object.key + "");
        if (!object.data) {
            return;
        }
        const rules = Object.keys(object.data)
            .map((key) => {
            const value = object.data[key];
            // Only increment if the value is integer, append if non number or decimal
            const identifier = key.includes("range") ? "append" : "increment";
            return {
                column: `${this.cfName}:${key}`,
                [identifier]: identifier === "append" ? `${value},` : (value || 0)
            };
        })
            .filter((rule) => rule.increment !== 0);
        if (rules.length > 0) {
            promises.push(row.createRules(rules));
        }
        return Promise.all(promises);
    }
    async deleteRow(id) {
        const key = id + "";
        if (!key) {
            return;
        }
        const row = this.table.row(key);
        return row.delete();
    }
    close() {
        // Do nothing
    }
}
exports.BigtableClient = BigtableClient;
//# sourceMappingURL=BigtableClient.js.map