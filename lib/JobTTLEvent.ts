import { BigtableClient } from "./BigtableClient";
import * as Debug from "debug";

const debug = Debug("yildiz:bigtable:jobttl");
const IS_METADATA = true;

export interface ExpiredTTL {
  ttlKey: string;
  cellQualifiers: string[];
}

export interface Qualifier {
  family: string;
  row: string;
  column: string;
}

export class JobTTLEvent {

  private btClient: BigtableClient;
  private intervalInMs: number;
  private tov!: NodeJS.Timer;

  constructor(btClient: BigtableClient, intervalInMs: number) {
    this.btClient = btClient;
    this.intervalInMs = intervalInMs;
  }

  private arrayToChunks(array: ExpiredTTL[], size: number) {

    const result: ExpiredTTL[][] = [];
    for (let i = 0; i < array.length; i += size) {
        const chunk = array.slice(i, i + size);
        result.push(chunk);
    }
    return result;
  }

  /**
   * Main function to run job
   */
  public run() {

    this.tov = setTimeout(() => {

      this.deleteExpiredData()
        .then(() => {
          this.run();
        })
        .catch((error: Error) => {
          debug(error);
          this.run();
        });

    }, this.intervalInMs);
  }

  /**
   * Get the expired ttls with the range that is defined by the expired timestamp
   */
  private async getExpiredTTLs(): Promise<ExpiredTTL[] | null> {

    const currentTimestamp = Date.now();
    const ranges = [];
    const etl = (result: any) => ({
      ttlKey: result.id,
      cellQualifiers: Object.keys(result.data[this.btClient.cfNameMetadata]),
    });

    for (let i = 0; i < this.btClient.clusterCount; i++) {
      ranges.push({
        start: `ttl#${i}#0`,
        end: `ttl#${i}#${currentTimestamp}`,
      });
    }

    const options = {
      ranges,
      limit: this.btClient.ttlBatchSize,
    };

    const expiredTTLs = await this.btClient.scanCellsInternal(this.btClient.tableMetadata, options, etl);
    debug(`Range scan calls takes ${Date.now() - currentTimestamp} ms`);

    return expiredTTLs;
  }

  /**
   * Actual execution of the deletion of the TTLs
   * @param expiredTTLs an Array of the object that contains details of the deletion
   */
  private async deleteExecutionTTLs(expiredTTLs: ExpiredTTL[]) {

    const startTimestamp = Date.now();

    const cleanExpiredTTLs = expiredTTLs.filter((expiredTTL) => !!expiredTTL);

    // Batching the delete
    const chunksDeletions = this.arrayToChunks(cleanExpiredTTLs, this.btClient.ttlBatchSize);

    for (const chunksDeletion of chunksDeletions) {

      // Map and flatten before generating cellEntries
      const qualifiers = ([] as string[]).concat(
        ...chunksDeletion
          .map((expiredTTL: ExpiredTTL) => expiredTTL.cellQualifiers),
        )
        .map((qualifier: string) => {
          const splitQualifiers = qualifier.split("#");
          const family = splitQualifiers[0];
          const row = splitQualifiers[1];
          const column = splitQualifiers[2];

          return {
            family,
            row,
            column,
          };
        });

      const cellEntries = qualifiers
        .map((qualifier: Qualifier) => ({
          key: qualifier.row,
          data: [ `${qualifier.family}:${qualifier.column}`],
        }));

      const ttlEntries = chunksDeletion
        .map((expiredTTL: ExpiredTTL) => ({
          key: expiredTTL.ttlKey,
        }));

      const ttlReferenceEntries = qualifiers
        .map((qualifier: Qualifier) => ({
          key: `${qualifier.family}#${qualifier.row}#${qualifier.column}`,
        }));

      try {
        await Promise.all([
          this.btClient.multiDelete(cellEntries),
          this.btClient.multiDelete(ttlEntries, IS_METADATA),
          this.btClient.multiDelete(ttlReferenceEntries, IS_METADATA, this.btClient.tableTTLReference),
        ]);
      } catch (error) {
        debug(error);
      }

      qualifiers
        .forEach((qualifier: Qualifier) => {
          this.btClient.emit("expired", {
            row: qualifier.row,
            column: qualifier.column,
          });
        });
    }

    debug(`Execution deletion calls takes ${Date.now() - startTimestamp} ms`);
  }

  /**
   * Delete the expired cells after it is resolved, the job will run again
   */
  private async deleteExpiredData() {

    debug("running Job");

    const expiredTTLs = await this.getExpiredTTLs();

    // Return early
    if (!expiredTTLs || !expiredTTLs.length) {
      return;
    }

    return await this.deleteExecutionTTLs(expiredTTLs);
  }

  public close() {
    if (this.tov) {
      debug("Stopping job..");
      clearTimeout(this.tov);
    }
  }
}
