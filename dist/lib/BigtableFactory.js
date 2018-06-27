"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Bigtable = require("@google-cloud/bigtable");
const BigtableClient_1 = require("./BigtableClient");
const DEFAULT_COLUMN = "value";
const DEFAULT_MAX_VERSIONS = 1;
class BigtableFactory {
    constructor(config) {
        this.config = config;
    }
    async init() {
        const { projectId, keyFilename, instanceName, } = this.config;
        const bigtable = new Bigtable({
            projectId,
            keyFilename,
        });
        this.instance = bigtable.instance(instanceName);
        const instanceExists = await this.instance.exists();
        if (!instanceExists || !instanceExists[0]) {
            await this.instance.create();
        }
    }
    // Get or initialize BigTableClient based on TableConfig
    async get(tableConfig) {
        const bigtableClient = new BigtableClient_1.BigtableClient(tableConfig, this.instance, this.config.ttlScanIntervalMs);
        await bigtableClient.init();
        return bigtableClient;
    }
    async close() {
        // TODO: Close instance?
    }
}
exports.BigtableFactory = BigtableFactory;
//# sourceMappingURL=BigtableFactory.js.map