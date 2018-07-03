import * as Bigtable from "@google-cloud/bigtable";
import * as Debug from "debug";

import { BigtableClient } from "./BigtableClient";
import { BigtableFactoryConfig, BigtableClientConfig } from "./interfaces";

const debug = Debug("yildiz:bigtable:factory");

export class BigtableFactory {

  private config: BigtableFactoryConfig;
  private instance?: Bigtable.Instance;
  private instances: BigtableClient[];

  constructor(config: BigtableFactoryConfig) {
    this.config = config;
    this.instances = [];
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

    if (!this.instance) {
      throw new Error("Failed to create instance " + instanceName);
    }

    const instanceExists = await this.instance.exists();
    if (!instanceExists || !instanceExists[0]) {
      debug("Instance", instanceName, "does not exist, creating..");
      await this.instance.create();
      debug("Instance", instanceName, "created.");
    } else {
      debug("Instance", instanceName, "already exists.");
    }
  }

  public async get(tableConfig: BigtableClientConfig) {

    const bigtableClient = new BigtableClient(
      tableConfig,
      this.instance as Bigtable.Instance,
      this.config.ttlScanIntervalMs,
      this.config.minJitterMs,
      this.config.maxJitterMs,
    );

    await bigtableClient.init();
    this.instances.push(bigtableClient);

    debug("Created new client instance for tableConfig:", tableConfig);
    return bigtableClient;
  }

  public close() {
    debug("Closing all known instances", this.instances.length);
    this.instances.forEach((instance) => instance.close());
    this.instances = [];
  }
}
