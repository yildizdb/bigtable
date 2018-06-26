import { BigtableClient } from "./BigtableClient";
import { BigtableConfig } from "./interfaces";

export interface TableConfig {
  tableName: string;
  columnFamilyName: string;
}

export class BigTableFactory {

  private config: BigtableConfig;
  private cache: any;

  constructor(config: BigtableConfig) {
    this.config = config;
    this.cache = {};
  }

  // Get or initialize BigTableClient based on TableConfig
  async getOrCreate(tableConfig: TableConfig) {

    const {
      tableName,
      columnFamilyName
    } = tableConfig;

    if (this.cache[tableName]) {
      return this.cache[tableName];
    }

    const config = Object.assign(
      {},
      this.config,
      {
        tableName,
        columnFamilyName
      }
    );

    const btClient = new BigtableClient(config);
    await btClient.init();

    this.cache[tableName] = btClient;

    return this.cache[tableName];
  }

  close() {

    const tableCacheKeys =  Object.keys(this.cache);

    if (tableCacheKeys.length) {
      tableCacheKeys.map((tableName: string) => {
        this.cache[tableName].close();
      });
    }

    this.cache = {};
  }

}
