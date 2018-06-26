import * as Bigtable from "@google-cloud/bigtable";

import { BigtableClient } from "./BigtableClient";
import { BigtableFactoryConfig, BigtableClientConfig } from "./interfaces";

const DEFAULT_COLUMN = "value";
const DEFAULT_MAX_VERSIONS = 1;

export class BigTableFactory {

  private config: BigtableFactoryConfig;
  private instance: any; // TODO: Should get the correct type of instance

  private defaultColumn!: string;

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

  // Get or initialize BigTableClient based on TableConfig
  public async get(tableConfig: BigtableClientConfig) {

    const bigtableClient = new BigtableClient(tableConfig, this.instance);
    await bigtableClient.init();

    return bigtableClient;
  }

  public async close() {

    // TODO: Close instance?
  }

}
