"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const BigtableClient_1 = require("./BigtableClient");
class BigTableFactory {
    constructor(config) {
        this.config = config;
        this.cache = {};
    }
    // Get or initialize BigTableClient based on TableConfig
    async getOrCreate(tableConfig) {
        const { tableName, columnFamilyName } = tableConfig;
        if (this.cache[tableName]) {
            return this.cache[tableName];
        }
        const config = Object.assign({}, this.config, {
            tableName,
            columnFamilyName
        });
        const btClient = new BigtableClient_1.BigtableClient(config);
        await btClient.init();
        this.cache[tableName] = btClient;
        return this.cache[tableName];
    }
    close() {
        const tableCacheKeys = Object.keys(this.cache);
        if (tableCacheKeys.length) {
            tableCacheKeys.map((tableName) => {
                this.cache[tableName].close();
            });
        }
        this.cache = {};
    }
}
exports.BigTableFactory = BigTableFactory;
//# sourceMappingURL=BigtableFactory.js.map