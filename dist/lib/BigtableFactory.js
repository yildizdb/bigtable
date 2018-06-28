"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const Bigtable = require("@google-cloud/bigtable");
const BigtableClient_1 = require("./BigtableClient");
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
    async get(tableConfig) {
        const bigtableClient = new BigtableClient_1.BigtableClient(tableConfig, this.instance, this.config.ttlScanIntervalMs, this.config.minJitterMs, this.config.maxJitterMs);
        await bigtableClient.init();
        return bigtableClient;
    }
    async close() {
        // Do nothing
    }
}
exports.BigtableFactory = BigtableFactory;
//# sourceMappingURL=BigtableFactory.js.map