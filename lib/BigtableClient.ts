import * as Bigtable from "@google-cloud/bigtable";
import * as Debug from "debug";
import * as murmur from "murmurhash";

import { BigtableClientConfig, RuleColumnFamily, BulkData } from "./interfaces";
import { JobTTLEvent } from "./JobTTLEvent";
import { EventEmitter } from "events";

const debug = Debug("yildiz:bigtable:client");

const DEFAULT_TTL_SCAN_INTERVAL_MS = 5000;
const DEFAULT_MIN_JITTER_MS = 2000;
const DEFAULT_MAX_JITTER_MS = 30000;
const DEFAULT_MAX_VERSIONS = 1;

const DEFAULT_CLUSTER_COUNT = 3;
const DEFAULT_MURMUR_SEED = 0;
const DEFAULT_TTL_BATCHSIZE = 250;

const DEFAULT_INSERT_BULK_LIMIT = 3500;
const DEFAULT_INSERT_BULK_WITH_TTL_LIMIT = 1000;

const DEFAULT_COLUMN = "value";
const DEFAULT_COLUMN_FAMILY = "default";
const COUNTS = "counts";

export class BigtableClient extends EventEmitter {

  private config: BigtableClientConfig;
  private instance: Bigtable.Instance;
  private table!: Bigtable.Table;
  private tov!: NodeJS.Timer;
  private job: JobTTLEvent;
  private defaultColumn!: string;
  private intervalInMs: number;
  private minJitterMs: number;
  private maxJitterMs: number;
  private isInitialized: boolean;
  private murmurSeed: number;
  private insertBulkLimit: number;
  private insertBulkLimitTTL: number;

  public cfName!: string;
  public tableMetadata!: Bigtable.Table;
  public cfNameMetadata!: string;
  public tableTTLReference!: Bigtable.Table;
  public cfNameTTLReference!: string;
  public clusterCount: number;
  public ttlBatchSize: number;

  constructor(
    config: BigtableClientConfig,
    instance: Bigtable.Instance,
    intervalInMs: number,
    minJitterMs: number,
    maxJitterMs: number,
    clusterCount?: number,
    murmurSeed?: number,
    ttlBatchSize?: number,
    insertBulkLimit?: number,
    insertBulkLimitTTL?: number,
  ) {
    super();

    this.instance = instance;
    this.intervalInMs = intervalInMs || DEFAULT_TTL_SCAN_INTERVAL_MS;
    this.minJitterMs = minJitterMs || DEFAULT_MIN_JITTER_MS;
    this.maxJitterMs = maxJitterMs || DEFAULT_MAX_JITTER_MS;
    this.clusterCount = clusterCount || DEFAULT_CLUSTER_COUNT;
    this.murmurSeed = murmurSeed || DEFAULT_MURMUR_SEED;
    this.ttlBatchSize = ttlBatchSize || DEFAULT_TTL_BATCHSIZE;
    this.insertBulkLimit = insertBulkLimit || DEFAULT_INSERT_BULK_LIMIT;
    this.insertBulkLimitTTL = insertBulkLimitTTL || DEFAULT_INSERT_BULK_WITH_TTL_LIMIT;
    this.config = config;
    this.isInitialized = false;
    this.job = new JobTTLEvent(this, this.intervalInMs);
  }

  /**
   * Helper function to generate ttlRowKey
   * @param rowKey
   * @param data
   */
  private getTTLRowKey(ttl: number) {

    const deleteTimestamp = Date.now() + (1000 * ttl);
    const salt = murmur.v3(deleteTimestamp.toString(), this.murmurSeed) % this.clusterCount;

    return `ttl#${salt}#${deleteTimestamp}`;
  }

  /**
   * Helper function to check and delete Reference Keys if exists
   * @param referenceKeys
   */
  private async deleteReferenceKeys(referenceKeys: string[]) {
    const options = {
      keys: referenceKeys,
    };

    const etl = (result: any) => {

      const value = result.data &&
        result.data[this.cfNameTTLReference] &&
        result.data[this.cfNameTTLReference].ttlKey &&
        result.data[this.cfNameTTLReference].ttlKey[0] &&
        result.data[this.cfNameTTLReference].ttlKey[0].value;

      if (!value) {
        return null;
      }

      return {
        identifier: result.id,
        ttlKey: value,
      };
    };

    // Get an array of results from reference table if each columns has its corresponding ttlKey
    const ttlReferenceRow = await this.scanCellsInternal(this.tableTTLReference, options, etl);

    // Create a mutate rules based on the array returned
    if (ttlReferenceRow && ttlReferenceRow.length) {
      const deleteMutates = ttlReferenceRow
        .map((singleRow: {identifier: string; ttlKey: string; }) => ({
          key: singleRow.ttlKey,
          data: [`${this.cfNameMetadata}:${singleRow.identifier}`],
        }));

      const isMetadata = true;
      await this.multiDelete(deleteMutates, isMetadata);
    }
  }

  /**
   * Getting the mutate array for Bulk operations, it will be used to delete ttl and ttl reference
   * @param rowKey
   * @param ttl in Seconds
   * @param columnNames as column identifier
   * @param ttlRowkey to get uniform ttlRowKey
   */
  private async getMutateArrayForBulk(rowKey: string, ttl: number, columnNames: string[], ttlRowKey: string) {

    const ttlData: Bigtable.GenericObject = {};
    const referenceKeys: string[] = [];
    const ttlReferenceData: Bigtable.TableInsertFormat[] = [];

    columnNames.forEach((columnName: string) => {

      const columnQualifier = `${this.cfName}#${rowKey}#${columnName}`;

      ttlData[columnQualifier] = ttl;
      referenceKeys.push(columnQualifier);

      ttlReferenceData.push({
        key: columnQualifier,
        data: {
          [this.cfNameTTLReference]: {
            ttlKey: ttlRowKey,
          },
        },
      });
    });

    await this.deleteReferenceKeys(referenceKeys);

    return {
      ttlData,
      ttlReferenceData,
    };
  }

  /**
   * Do checking on the ttl if every cell has their own ttl or it's a bulk ttl,
   * and it will update the ttl reference table and metadata table accordingly
   * @param insertData BulkData insert from internal Bigtable type
   * @param ttlBulk in Seconds
   */
  private async upsertTTLOnBulk(insertData: BulkData[], ttlBulk?: number) {

    const ttlSingleExists = insertData
      .map((insertSingleData) => insertSingleData.ttl)
      .filter((ttlSingle) => !!ttlSingle)
      .length;

    if (!ttlSingleExists && !ttlBulk) {
      return;
    }

    // Preparation data
    const insertDataFull = insertData
      .filter((insertSingleData) => insertSingleData.ttl || ttlBulk)
      .map((insertSingleData) => Object.assign(
        {},
        insertSingleData,
        {
          ttlKey: this.getTTLRowKey((insertSingleData.ttl || ttlBulk) as number),
          fullQualifier: `${insertSingleData.family || this.cfName}#${insertSingleData.row}#${insertSingleData.column}`,
        },
      ));

    // Keys to delete on ttl reference table if exist
    const referenceKeys: string[] = insertDataFull
      .filter((insertSingleData) => insertSingleData.ttlKey)
      .map((insertSingleData) => insertSingleData.fullQualifier);

    // Data to replace the ttl reference table data
    const ttlReferenceData: Bigtable.TableInsertFormat[] = insertDataFull
      .filter((insertSingleData) => insertSingleData.ttlKey)
      .map((insertSingleData) => ({
        key: insertSingleData.fullQualifier,
        data: {
          [this.cfNameTTLReference]: {
            ttlKey: insertSingleData.ttlKey,
          },
        },
      }));

    // Data to be inserted as ttl row in metadata table as an reduced object to reduce the mutation
    const ttlDataObj = insertDataFull
      .filter((insertSingleData) => insertSingleData.ttlKey)
      .map((insertSingleData) => ({
        key: insertSingleData.ttlKey,
        data: {
          [this.cfNameMetadata]: {
            [insertSingleData.fullQualifier]: insertSingleData.ttl || ttlBulk,
          },
        },
      }))
      .reduce((cummulator, currObj) => {

        if (!cummulator[currObj.key]) {
          cummulator[currObj.key] = currObj.data;
          return cummulator;
        }

        cummulator[currObj.key] = Object.assign({}, cummulator[currObj.key], currObj.data);
        return cummulator;
      }, {} as any);

    // Return the correct schema before insertion
    const ttlData: Bigtable.TableInsertFormat[] = Object
      .keys(ttlDataObj)
      .map((ttlDataKey: string) => ({
        key: ttlDataKey,
        data: ttlDataObj[ttlDataKey],
      }));

    await Promise.all([
      this.deleteReferenceKeys(referenceKeys),
      this.tableTTLReference.insert(ttlReferenceData),
      this.tableMetadata.insert(ttlData),
    ]);
  }

  /**
   * Check if upsert of counts needs to be done on the metadata table
   * @param insertData BulkData insert from internal Bigtable type
   */
  private async upsertCountOnBulk(insertData: BulkData[]) {

    // Get distinct rows
    const distinctRows = insertData
      .map((insertSingleData) => insertSingleData.row)
      .filter((item, pos, rowArray) => rowArray.indexOf(item) === pos);

    const options = {
      keys: distinctRows,
    };
    const etl = (result: any) => result.id ? true : false;
    const distinctRowsExists = await this.scanCellsInternal(this.table, options, etl);

    const distinctRowsCount = distinctRows.length;

    const difference = distinctRowsCount - distinctRowsExists.length;
    if (difference === 0) {
      return;
    }

    await this.tableMetadata.row(COUNTS).increment(`${this.cfNameMetadata}:${COUNTS}`, difference);
  }

  /**
   * Generic insert for both row and cell
   * @param rowKey
   * @param data
   */
  private async insert(
    table: Bigtable.Table,
    cfName: string,
    rowKey: string,
    data: Bigtable.GenericObject,
  ): Promise<any> {

    if (!table || !rowKey || !data) {
      return;
    }

    const dataKeys = Object.keys(data);
    const cleanedData: Bigtable.GenericObject = {};

    dataKeys.map((key: string) => {

      const value = data[key];
      cleanedData[key] = (value !== undefined &&
                              value !== null &&
                              typeof value === "object") ?
        JSON.stringify(value) : value;
    });

    return table.insert([{
      key: rowKey,
      data: {
        [cfName]: cleanedData,
      },
    }]);
  }

  /**
   * Parsing the stored string, and return the object if it is parsable as an object
   * @param value
   */
  private getParsedValue(value: string) {

    let result = value;

    try {
      result = JSON.parse(value);
    } catch (error) {
      // Do Nothing
    }

    if (typeof result === "object") {
      return result;
    }

    return value;
  }

  /**
   * Generic retrieve for both row and cell
   * @param rowKey
   * @param column
   */
  private async retrieve(table: Bigtable.Table, cfName: string, rowKey: string, column?: string, complete?: boolean):
    Promise<any> {

    if (!table || !rowKey) {
      return;
    }

    const columnName = column ? column || this.defaultColumn : null;
    const identifier = columnName ? `${cfName}:${columnName}` : undefined;

    const row = table.row(rowKey + "");

    const result: Bigtable.GenericObject = {};
    let rowGet = null;

    try {
      rowGet = await row.get(identifier ? [identifier] : undefined);
    } catch (error) {

      if (!error.message.startsWith("Unknown row")) {
        throw error;
      }

      // Set the result to null if it throws at row.get - Error: Unknown row
      return null;
    }

    if (!rowGet) {
      return null;
    }

    if (rowGet && columnName) {
      const singleResult = rowGet[0] &&
        rowGet[0][cfName] &&
        rowGet[0][cfName][columnName] &&
        rowGet[0][cfName][columnName][0];

      return complete ? singleResult : singleResult.value;
    }

    if (
      rowGet &&
      rowGet[0] &&
      rowGet[0].data &&
      rowGet[0].data[cfName]
    ) {
      const rowData = rowGet[0].data[cfName];
      Object.keys(rowData).forEach((columnKey: string) => {
        if (rowData[columnKey] && rowData[columnKey][0]) {
          result[columnKey] = complete ?
            rowData[columnKey][0] :
            this.getParsedValue(rowData[columnKey][0].value);
        }
      });
    }

    return result;
  }

  /**
   * Get the current working table
   */
  public getTable() {
    return this.table;
  }

  /**
   * Scan and return cells based on filters
   * @param table
   * @param filter
   * @param etl
   */
  public async scanCellsInternal(
    table: Bigtable.Table,
    options: Bigtable.GenericObject,
    etl?: (result: Bigtable.GenericObject) => any,
  ): Promise<any> {

    debug("Scanning cells via filter for", this.config.name);
    return new Promise((resolve, reject) => {

      const results: Bigtable.GenericObject[] = [];

      table.createReadStream(options)
      .on("error", (error: Error) => {
        reject(error);
      })
      .on("data", (result: Bigtable.GenericObject) => {
        if (etl) {
          if (etl(result)) {
            results.push(etl(result));
          }
        } else {
          results.push(result);
        }
      })
      .on("end", () => {
        resolve(results);
      });
    });
  }

  public async scanCells(options: Bigtable.StreamOptions, etl?: (result: Bigtable.GenericObject) => any): Promise<any> {
    return this.scanCellsInternal(this.table, options, etl);
  }

  /**
   * Initialization function for the client
   */
  public async init() {

    if (this.isInitialized) {
      return;
    }

    debug("Initialising..", this.config.name);

    const {
      name,
      columnFamily = DEFAULT_COLUMN_FAMILY,
      defaultColumn = DEFAULT_COLUMN,
      maxVersions = DEFAULT_MAX_VERSIONS,
      maxAgeSecond,
    } = this.config;

    this.defaultColumn = defaultColumn;

    const rule: RuleColumnFamily = {
      versions: maxVersions,
    };

    if (maxAgeSecond) {
      rule.age = {
        seconds: maxAgeSecond,
      };
      rule.union = true;
    }

    this.table = this.instance.table(name);
    const tableExists = await this.table.exists();
    if (!tableExists || !tableExists[0]) {
      await this.table.create(name);
    }

    const cFamily = this.table.family(columnFamily);
    const cFamilyExists = await cFamily.exists();
    if (!cFamilyExists || !cFamilyExists[0]) {
      await cFamily.create({
        rule,
      });
    }

    this.tableMetadata = this.instance.table(`${name}_metadata`);
    const tableMetadataExists = await this.tableMetadata.exists();
    if (!tableMetadataExists || !tableMetadataExists[0]) {
      await this.tableMetadata.create(name);
    }

    const cFamilyMetadata = this.tableMetadata.family(`${columnFamily}_metadata`);
    const cFamilyMetadataExists = await cFamilyMetadata.exists();
    if (!cFamilyMetadataExists || !cFamilyMetadataExists[0]) {
      await cFamilyMetadata.create({
        rule,
      });
    }

    this.tableTTLReference = this.instance.table(`${name}_ttl_reference`);
    const tableTTLReferenceExists = await this.tableTTLReference.exists();
    if (!tableTTLReferenceExists || !tableTTLReferenceExists[0]) {
      await this.tableTTLReference.create(name);
    }

    const cFamilyTTLReference = this.tableTTLReference.family(`${columnFamily}_ttl_reference`);
    const cFamilyTTLReferenceExists = await cFamilyTTLReference.exists();
    if (!cFamilyTTLReferenceExists || !cFamilyTTLReferenceExists[0]) {
      await cFamilyTTLReference.create({
        rule,
      });
    }

    this.cfName = cFamily.id;
    this.cfNameMetadata = cFamilyMetadata.id;
    this.cfNameTTLReference = cFamilyTTLReference.id;
    this.isInitialized = true;

    if (this.minJitterMs && this.maxJitterMs) {
      const deltaJitterMs = parseInt((Math.random() * (this.maxJitterMs - this.minJitterMs)).toFixed(0), 10);
      const startJitterMs = this.minJitterMs + deltaJitterMs;
      debug("TTL Job started with jitter %s ms", startJitterMs);
      this.tov = setTimeout(() => this.job.run(), startJitterMs);
    } else {
      debug("TTL Job started");
      this.job.run();
    }

    debug("Initialised.", this.config.name);
  }

  /**
   * Add (or minus) the whole row
   * @param filter
   * @param etl
   */
  public async multiAdd(rowKey: string, data: Bigtable.GenericObject, ttl?: number) {

    debug("Multi-adding cells for", this.config.name, rowKey, ttl);

    const row = this.table.row(rowKey + "");
    const insertPromises: Array<Promise<any>> = [];
    const columnNames = Object.keys(data);

    if (!columnNames.length) {
      return;
    }

    if (!data) {
      return;
    }

    if (ttl) {
      const ttlRowKey = this.getTTLRowKey(ttl);
      const { ttlData, ttlReferenceData } = await this.getMutateArrayForBulk(rowKey, ttl, columnNames, ttlRowKey);

      if (ttlReferenceData) {
        insertPromises.push(this.tableTTLReference.insert(ttlReferenceData));
      }

      if (ttlData) {
        insertPromises.push(
          this.insert(this.tableMetadata, this.cfNameMetadata, ttlRowKey, ttlData),
        );
      }
    }

    const rules = columnNames
      .map((key: string) => {

        const value = data[key];
        if (typeof value !== "number") {
          return;
        }

        return {
          column: `${this.cfName}:${key}`,
          increment: (value || 0),
        };
      })
      .filter(
        (rule: Bigtable.GenericObject | undefined) => !!rule && rule.increment !== 0,
      ) as Bigtable.RowRule[];

    if (rules.length > 0) {
      insertPromises.push(
        row.createRules(rules),
      );
    }

    return Promise.all(insertPromises);
  }

  /**
   * Set or append a value of cell
   * @param rowKey
   * @param value
   * @param ttl in Seconds
   * @param column
   */
  public async set(rowKey: string, value: string | number, ttl?: number, column?: string): Promise<any> {

    debug("Setting cell for", this.config.name, rowKey, column, value, ttl);
    const columnName = column ? column : this.defaultColumn;
    const data = {
      [columnName]: value,
    };

    const insertPromises: Array<Promise<any>> = [];

    if (ttl) {
      const ttlRowKey = this.getTTLRowKey(ttl);
      const columnQualifier = `${this.cfName}#${rowKey}#${columnName}`;

      // Check if the ttlReference exists in the ttl reference table
      const ttlReference = await this
        .retrieve(this.tableTTLReference, this.cfNameTTLReference, columnQualifier);

      // If exists delete the corresponding cell
      if (ttlReference && ttlReference.ttlKey) {
        const row = this.tableMetadata.row(ttlReference.ttlKey);
        await row.deleteCells([`${this.cfNameMetadata}:${columnQualifier}`]);
      }

      const ttlData = {
        [columnQualifier] : ttl,
      };

      insertPromises.push(
        this.insert(this.tableTTLReference, this.cfNameTTLReference, columnQualifier, {ttlKey: ttlRowKey}),
        this.insert(this.tableMetadata, this.cfNameMetadata, ttlRowKey, ttlData),
      );
    }

    const rowExists = await this.table.row(rowKey + "").exists();
    if (!rowExists || !rowExists[0]) {
      insertPromises.push(
        this.tableMetadata.row(COUNTS)
          .increment(`${this.cfNameMetadata}:${COUNTS}`, 1),
      );
    }

    insertPromises.push(
      this.insert(this.table, this.cfName, rowKey, data),
    );

    return Promise.all(insertPromises);
  }

  /**
   * Get a value of cell
   * @param rowKey
   * @param column
   */
  public get(rowKey: string, column?: string): Promise<any> | void {

    if (!rowKey) {
      return;
    }

    debug("Getting cell for", this.config.name, rowKey, column);
    const columnName = column || this.defaultColumn || "";
    return this.retrieve(this.table, this.cfName, rowKey, columnName);
  }

  /**
   * Delete values from table, it uses mutate method from official bigtable client
   * @param rowKey
   * @param column
   */
  public async multiDelete(mutateRules: any[], isMetadata?: boolean, table?: Bigtable.Table) {

    if (!mutateRules || !mutateRules.length) {
      return;
    }

    const deleteRules = mutateRules
      .filter((mutateRule) => !!mutateRule)
      .map((mutateRule) => {
        mutateRule.method = "delete";
        return mutateRule;
      });

    const workingTable = table ? table :
      (isMetadata ? this.tableMetadata : this.table);

    await workingTable.mutate(deleteRules);

    if (!isMetadata) {
      const distinctRows = deleteRules
        .map((deleteRule) => deleteRule.key)
        .filter((item, pos, rowArray) => rowArray.indexOf(item) === pos);

      const options = {
        keys: distinctRows,
      };

      const etl = (result: any) => result.id ? true : false;

      const rowExists = await this.scanCellsInternal(this.table, options, etl);

      // Would be negative int if some rows are empty
      const difference = rowExists.length - distinctRows.length;

      if (difference !== 0) {
        await this.tableMetadata.row(COUNTS).increment(`${this.cfNameMetadata}:${COUNTS}`, difference);
      }
    }
  }

  /**
   * Delete a value of cell
   * @param rowKey
   * @param column
   */
  public async delete(rowKey: string, column?: string) {

    debug("Deleting for", this.config.name, rowKey, column);

    if (!rowKey) {
      return;
    }

    const row = this.table.row(rowKey + "");
    const columnName = column || this.defaultColumn || "";

    await row.deleteCells([`${this.cfName}:${columnName}`]);

    const rowExists = await this.table.row(rowKey + "").exists();
    if (!rowExists || !rowExists[0]) {
        await this.tableMetadata.row(COUNTS)
          .increment(`${this.cfNameMetadata}:${COUNTS}`, -1);
    }
  }
  /**
   * Bulk insert the value into table, it should follow the custom rules of bulkInsert
   * @param insertData BulkData as array, take a look at Bigtable type
   */
  public async bulkInsert(insertData: BulkData[], ttl?: number) {

    if (!insertData || !insertData.length) {
      return;
    }

    debug("Bulk insert for", insertData.length, "data");

    const insertRules = insertData
      .filter((insertSingleData) => !!insertSingleData)
      .map((insertSingleData) => ({
        key: insertSingleData.row,
        data: {
          [insertSingleData.family || this.cfName]: {
            [insertSingleData.column]: insertSingleData.data,
          },
        },
      }));

    const insertDataWithTTLExists = insertData
      .map((insertSingleData) => insertSingleData.ttl)
      .filter((insertSingleData) => !!insertSingleData)
      .length || ttl;

    const insertDataLength = insertData.length;

    if (insertDataLength > this.insertBulkLimit) {
      throw new Error(`Bulk insert limit exceeded, please insert less than ${this.insertBulkLimit} cells`);
    }

    if (insertDataWithTTLExists && insertDataLength > this.insertBulkLimitTTL) {
      throw new Error(`Bulk insert limit with TTL exceeded, please insert less than ${this.insertBulkLimitTTL} cells`);
    }

    // Push all promises into single Promise.all
    await Promise.all([
      this.upsertTTLOnBulk(insertData, ttl),
      this.upsertCountOnBulk(insertData),
      this.table.insert(insertRules),
    ]);
  }

  /**
   * Set values of multiple column based on objects
   * @param rowKey
   * @param columnsObject
   * @param ttl in Seconds
   */
  public async multiSet(rowKey: string, columnsObject: Bigtable.GenericObject, ttl?: number) {

    debug("Running multi-set for", this.config.name, rowKey, ttl);

    const insertPromises: Array<Promise<any>> = [];
    const columnNames = Object.keys(columnsObject);

    if (!columnNames.length) {
      return;
    }

    if (ttl) {
      const ttlRowKey = this.getTTLRowKey(ttl);
      const { ttlData, ttlReferenceData } = await this.getMutateArrayForBulk(rowKey, ttl, columnNames, ttlRowKey);

      if (ttlReferenceData) {
        insertPromises.push(this.tableTTLReference.insert(ttlReferenceData));
      }

      if (ttlData) {
        insertPromises.push(
          this.insert(this.tableMetadata, this.cfNameMetadata, ttlRowKey, ttlData),
        );
      }
    }

    const rowExists = await this.table.row(rowKey + "").exists();
    if (!rowExists || !rowExists[0]) {
      insertPromises.push(
        this.tableMetadata.row(COUNTS)
          .increment(`${this.cfNameMetadata}:${COUNTS}`, 1),
      );
    }

    insertPromises.push(
      this.insert(this.table, this.cfName, rowKey, columnsObject),
    );

    return Promise.all(insertPromises);
  }

  /**
   * Get the whole row values as an object
   * @param rowKey
   */
  public async getRow(rowKey: string): Promise<any> {
    debug("Getting row for", this.config.name, rowKey);
    return await this.retrieve(this.table, this.cfName, rowKey);
  }

  /**
   * Delete the whole row values as an object
   * @param rowKey
   */
  public async deleteRow(rowKey: string): Promise<any> {

    if (!rowKey) {
      return;
    }

    debug("Deleting row for", this.config.name, rowKey);

    const row = this.table.row(rowKey);

    return Promise.all([
      this.tableMetadata.row(COUNTS).increment(`${this.cfNameMetadata}:${COUNTS}`, -1),
      row.delete(),
    ]);
  }

  /**
   * Increase the value of the row by one if the value is integer
   * @param rowKey
   * @param column
   * @param ttl in Seconds
   */
  public async increase(rowKey: string, column?: string, ttl?: number): Promise<any> {

    if (!rowKey) {
      return;
    }

    debug("Increasing for", this.config.name, rowKey, column, ttl);

    const columnName = column || this.defaultColumn || "";
    const row = this.table.row(rowKey);

    const insertPromises: Array<Promise<any>> = [];

    if (ttl) {
      const ttlRowKey = this.getTTLRowKey(ttl);
      const columnQualifier = `${this.cfName}#${rowKey}#${columnName}`;

      // Check if the ttlReference exists in the ttl reference table
      const ttlReference = await this
        .retrieve(this.tableTTLReference, this.cfNameTTLReference, columnQualifier);

      // If exists delete the corresponding cell
      if (ttlReference && ttlReference.ttlKey) {
        const ttlReferenceRow = this.tableMetadata.row(ttlReference.ttlKey);
        await ttlReferenceRow.deleteCells([`${this.cfNameMetadata}:${columnQualifier}`]);
      }

      const ttlData = {
        [columnQualifier] : ttl,
      };

      insertPromises.push(
        this.insert(this.tableTTLReference, this.cfNameTTLReference, columnQualifier, {ttlKey: ttlRowKey}),
        this.insert(this.tableMetadata, this.cfNameMetadata, ttlRowKey, ttlData),
      );
    }

    const rowExists = await row.exists();
    if (!rowExists || !rowExists[0]) {
      insertPromises.push(
        this.tableMetadata.row(COUNTS)
          .increment(`${this.cfNameMetadata}:${COUNTS}`, 1),
      );
    }

    insertPromises.push(
      row.increment(`${this.cfName}:${columnName}`, 1),
    );

    return Promise.all(insertPromises);
  }

  /**
   * Decrease the value of the row by one if the value is integer
   * @param rowKey
   * @param column
   * @param ttl in Seconds
   */
  public async decrease(rowKey: string, column?: string, ttl?: number): Promise<any> {

    if (!rowKey) {
      return;
    }

    debug("Decreasing for", this.config.name, rowKey, column, ttl);

    const columnName = column || this.defaultColumn || "";
    const row = this.table.row(rowKey);

    const insertPromises: Array<Promise<any>> = [];

    if (ttl) {
      const ttlRowKey = this.getTTLRowKey(ttl);
      const columnQualifier = `${this.cfName}#${rowKey}#${columnName}`;

      // Check if the ttlReference exists in the ttl reference table
      const ttlReference = await this
        .retrieve(this.tableTTLReference, this.cfNameTTLReference, columnQualifier);

      // If exists delete the corresponding cell
      if (ttlReference && ttlReference.ttlKey) {
        const ttlReferenceRow = this.tableMetadata.row(ttlReference.ttlKey);
        await ttlReferenceRow.deleteCells([`${this.cfNameMetadata}:${columnQualifier}`]);
      }

      const ttlData = {
        [columnQualifier] : ttl,
      };

      insertPromises.push(
        this.insert(this.tableTTLReference, this.cfNameTTLReference, columnQualifier, {ttlKey: ttlRowKey}),
        this.insert(this.tableMetadata, this.cfNameMetadata, ttlRowKey, ttlData),
      );
    }

    const rowExists = await row.exists();
    if (!rowExists || !rowExists[0]) {
      insertPromises.push(
        this.tableMetadata.row(COUNTS)
          .increment(`${this.cfNameMetadata}:${COUNTS}`, 1),
      );
    }

    insertPromises.push(
      row.increment(`${this.cfName}:${columnName}`, -1),
    );

    return Promise.all(insertPromises);
  }

  /**
   * Get a count of a table
   */
  public async count(): Promise<any> {
    debug("Checking count for", this.config.name);
    const counts = await this.retrieve(this.tableMetadata, this.cfNameMetadata, COUNTS, COUNTS);
    return counts || 0;
  }

  /**
   * Get a TTL of a cell
   * @param rowKey
   * @param column
   */
  public async ttl(rowKey: string, column?: string): Promise<any> {

    const columnIdentifier = column || this.defaultColumn;
    debug("Checking ttl for", `row: ${rowKey}`, `column: ${columnIdentifier}`);

    const fullQualifier = `${this.cfName}#${rowKey}#${column}`;
    const ttlKey = await this.retrieve(this.tableTTLReference, this.cfNameTTLReference, fullQualifier, "ttlKey");

    if (!ttlKey) {
      return -1;
    }

    const remainingTime = (Number(ttlKey.split("#")[2]) - Date.now()) / 1000;

    return Number(remainingTime.toFixed());
  }

  public close(retry?: boolean) {

    debug("Closing job..", this.config.name);

    if (this.tov) {
      clearTimeout(this.tov as NodeJS.Timer);
    }

    this.job.close();
  }

  public cleanUp() {
    debug("Cleaning up, deleting table and metadata..", this.config.name);
    return Promise.all([
      this.table.delete(),
      this.tableMetadata.delete(),
      this.tableTTLReference.delete(),
    ]);
  }

}
