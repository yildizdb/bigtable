import * as Bigtable from "@google-cloud/bigtable";

import { BigtableClient } from "./BigtableClient";
import { BigtableFactoryConfig, BigtableClientConfig } from "./interfaces";

export class BigtableFactory {

  private config: BigtableFactoryConfig;
  private instance: Bigtable.instance;

  constructor(config: BigtableFactoryConfig) {
    this.config = config;
  }

  public async init() {

    const {
      projectId,
      keyFilename,
      instanceName,
    } = this.config;

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

  public async get(tableConfig: BigtableClientConfig) {

    const bigtableClient = new BigtableClient(
      tableConfig,
      this.instance,
      this.config.ttlScanIntervalMs,
      this.config.minJitterMs,
      this.config.maxJitterMs,
    );
    await bigtableClient.init();

    return bigtableClient;
  }

  public async close() {

    // Do nothing
  }

}
