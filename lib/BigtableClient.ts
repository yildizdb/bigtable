import * as Bigtable from "@google-cloud/bigtable";

import { BigtableClientConfig } from "./interfaces";

const DEFAULT_COLUMN = "value";
const DEFAULT_MAX_VERSIONS = 1;

export class BigtableClient {

  private config: BigtableClientConfig;
  private instance: any; // TODO: get the correct type of instance
  private table!: any; // TODO: get the correct type of table
  private tableTTL!: any;

  private defaultColumn!: string;
  private cfName!: string;
  private isInitialized: boolean;

  constructor(config: BigtableClientConfig, instance: any) {

    this.instance = instance;
    this.config = config;
    this.isInitialized = false;
  }

  /**
   * Parsing the stored string, and return as it is if not parsable
   * @param value
   */
  private getParsedValue(value: any) {

    let result = value;

    try {
      result = JSON.parse(value);
    } catch (error) {
      // Do Nothing
    }

    return result;
  }

  /**
   * Generic insert for both row and cell
   * @param rowKey
   * @param data
   */
  private async insert(rowKey: string, data: any): Promise<any> {

    if (!rowKey || !data) {
      return;
    }

    const dataKeys = Object.keys(data);
    const cleanedData: any = {};

    dataKeys.map((key: string) => {

      const value = data[key];
      const sanitizedValue = (value && typeof value === "object") ?
        JSON.stringify(value) : value;

      cleanedData[key] = sanitizedValue || "";
    });

    return this.table.insert([{
      key: rowKey,
      data: {
        [this.cfName]: cleanedData,
      },
    }]);
  }

  /**
   * Generic retrieve for both row and cell
   * @param rowKey
   * @param column
   */
  public async retrieve(rowKey: string, column?: string): Promise<any> {

    const columnName = column || this.defaultColumn;
    const identifier = columnName ? `${this.cfName}:${columnName}` : undefined;

    const row = this.table.row(rowKey + "");

    const result: any = {rowKey};
    let rowGet = null;

    try {
      rowGet = await row.get(identifier);
    } catch (error) {

      if (!error.message.startsWith("Unknown row")) {
        throw error;
      }

      // Set the result to null if it throws at row.get - Error: Unknown row
      return null;
    }

    if (
      rowGet &&
      rowGet[0] &&
      rowGet[0].data &&
      rowGet[0].data[this.cfName]
    ) {
      const rowData = rowGet[0].data[this.cfName];
      Object.keys(rowData).forEach((columnKey: string) => {
        if (rowData[columnKey] && rowData[columnKey][0] && rowData[columnKey][0].value) {
          result[columnKey] = this.getParsedValue(rowData[columnKey][0].value);
        }
      });
    }

    return result;
  }

  /**
   * Scan and return cells based on filters
   * @param filter
   * @param etl
   */
  private async scanCells(filter: any[], etl?: (result: any) => {}): Promise<any> {

    return new Promise((resolve, reject) => {

      const results: any[] = [];

      this.table.createReadStream({
        filter,
      })
        .on("error", (error: Error) => {
          reject(error);
        })
        .on("data", (result: any) => {
          results.push(etl ? etl(result) : result);
        })
        .on("end", (result: any) => {
          resolve(results);
        });
    });
  }

  /**
   * Initialization function for the client
   */
  public async init() {

    if (this.isInitialized) {
      return;
    }

    const {
      name,
      columnFamily,
      defaultColumn = DEFAULT_COLUMN,
      defaultValue,
      maxVersions = DEFAULT_MAX_VERSIONS,
    } = this.config;

    this.defaultColumn = defaultColumn;

    this.table = this.instance.table(name);
    const tableExists = await this.table.exists();
    if (!tableExists || !tableExists[0]) {
      await this.table.create(name);
    }

    const cFamily = this.table.family(columnFamily);
    const cFamilyExists = await cFamily.exists();
    if (!cFamilyExists || !cFamilyExists[0]) {
      await cFamily.create({
        versions: maxVersions,
      });
    }

    this.tableTTL = this.instance.table(name);
    const tableTTLExists = await this.tableTTL.exists();
    if (!tableTTLExists || !tableTTLExists[0]) {
      await this.tableTTL.create(name);
    }

    const cFamilyTTL = this.table.family(columnFamily);
    const cFamilyTTLExists = await cFamilyTTL.exists();
    if (!cFamilyTTLExists || !cFamilyTTLExists[0]) {
      await cFamilyTTL.create({
        versions: maxVersions,
      });
    }

    this.cfName = cFamily.familyName;
    this.isInitialized = true;

    // TODO: Initialized job for ttl
  }

  /**
   * Set or append a value of cell
   * @param rowKey
   * @param value
   * @param ttl
   * @param column
   */
  public set(rowKey: string, value: string, ttl?: boolean, column?: string): Promise<any> {

    const data = {
      [column ? column : this.defaultColumn]: value,
    };

    return this.insert(rowKey, data);
  }

  /**
   * Get a value of cell
   * @param rowKey
   * @param column
   */
  public async get(rowKey: string, column?: string): Promise<any> {

    if (!rowKey) {
      return;
    }

    const columnName = column || this.defaultColumn || "";

    return await this.retrieve(rowKey, columnName);
  }

  /**
   * Delete a value of cell
   * @param rowKey
   * @param column
   */
  public delete(rowKey: string, column?: string) {

    if (!rowKey) {
      return;
    }

    const row = this.table.row(rowKey + "");
    const columnName = column || this.defaultColumn || "";

    return row.deleteCells([`${this.cfName}:${columnName}`]);
  }

  /**
   * Set values of multiple column based on objects
   * @param rowKey
   * @param columnsObject
   * @param ttl
   */
  public multiSet(rowKey: string, columnsObject: any, ttl) {

    return this.insert(rowKey, columnsObject);
  }

  /**
   * Get the whole row values as an object
   * @param rowKey
   */
  public async getRow(rowKey: string): Promise<any> {

    return await this.retrieve(rowKey);
  }

  /**
   * Delete the whole row values as an object
   * @param rowKey
   */
  public async deleteRow(rowKey: string): Promise<any> {

    if (!rowKey) {
      return;
    }

    const row = this.table.row(rowKey);

    return row.delete();
  }

  public close() {
    // Do nothing
  }
}
