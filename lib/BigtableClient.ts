import * as Bigtable from "@google-cloud/bigtable";

export interface Config {
  projectId: string;
  keyFilename: string;
  instanceName: string;
  tableName: string;
  columnFamilyName: string;
}

export class BigtableClient {

  private config: Config;
  private table: any;
  private columnFamily: any;
  private cfName!: string;

  constructor(config: Config) {
    this.config = config;
  }

  public async init() {

    const {
      projectId,
      keyFilename,
      instanceName,
      tableName,
      columnFamilyName,
    } = this.config;

    const bigtable = new Bigtable({
      projectId,
      keyFilename,
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
        versions: 1,
      });
    }

    this.cfName = this.columnFamily.familyName;
  }

  private getParsedValue(value: any) {

    let result = value;

    try {
      result = JSON.parse(value);
    } catch (error) {
      // DO Nothing
    }

    return result;
  }

  public getCfName(): string {
    return this.cfName;
  }

  public async getRow(key: string): Promise<any> {

    const row = this.table.row(key + "");

    const result: any = {key};
    let rowGet = null;

    try {
      rowGet = await row.get();
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
      Object.keys(rowData).forEach((column) => {
        if (rowData[column] && rowData[column][0] && rowData[column][0].value) {
          result[column] = this.getParsedValue(rowData[column][0].value);
        }
      });
    }

    return result;
  }

  public deleteCells(key: string, cells: any[]) {

    if (!cells.length) {
      return;
    }

    const row = this.table.row(key);
    const cellIndentifiers = cells
      .map((cell: string) => `${this.cfName}:${cell}`);

    return row.deleteCells(cellIndentifiers);
  }

  public async scanCells(filter: any[], etl?: (result: any) => {}): Promise<any> {

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

  public async insertRow(object: any): Promise<any> {

    if (!object.data || typeof object.data !== "object") {
      return;
    }

    const data: any = {};
    const dataKeys = Object.keys(object.data);

    dataKeys.map((key: string) => {

      const value = object.data[key];
      const sanitizedValue = (value && typeof value === "object") ?
        JSON.stringify(value) : value;

      data[key] = sanitizedValue || "";
    });

    return this.table.insert([{
      key: object.key,
      data: {
        [this.cfName]: data,
      },
    }]);
  }

  public async increaseRow(object: any): Promise<any> {

    const promises: Array<Promise<any>> = [];
    const row = this.table.row(object.key + "");

    if (!object.data) {
      return;
    }

    const rules = Object.keys(object.data)
      .map((key: string) => {

        const value = object.data[key];

        // Only increment if the value is integer, append if non number or decimal
        const identifier = key.includes("range") ? "append" : "increment";

        return {
          column: `${this.cfName}:${key}`,
          [identifier]: identifier === "append" ? `${value},` : (value || 0),
        };
      })
      .filter((rule: any) => rule.increment !== 0);

    if (rules.length > 0) {
      promises.push(
        row.createRules(rules),
      );
    }

    return Promise.all(promises);
  }

  public async deleteRow(id: string): Promise<any> {

    const key = id + "";

    if (!key) {
      return;
    }

    const row = this.table.row(key);

    return row.delete();
  }

  public close() {
    // Do nothing
  }
}
