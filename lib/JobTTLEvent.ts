import { BigtableClient } from "./BigtableClient";
import * as Debug from "debug";

const debug = Debug("yildiz:bigtable:jobttl");

export class JobTTLEvent {

  private btClient: BigtableClient;
  private intervalInMs: number;
  private tov: any;

  constructor(btClient: BigtableClient, intervalInMs: number) {
    this.btClient = btClient;
    this.intervalInMs = intervalInMs;
  }

  private arrayToChunks(array: any[], size: number) {

    const result: any[] = [];
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
   * Delete the expired cells after it is resolved, the job will be run again
   */
  private async deleteExpiredData() {

    const etl = (result: any) => {
      const ttl: any = result.data[this.btClient.cfNameMetadata].ttl;

      if (!ttl) {
        return null;
      }

      const value = ttl[0].value;
      const timestamp = ttl[0].timestamp;

      // Not returning anything if timestamp is not expired
      if ((timestamp / 1000) >= (Date.now() - (value * 1000))) {
        return null;
      }

      return result.id;
    };

    const filteredRowKeys = await this.btClient.scanCellsInternal(this.btClient.tableMetadata, [], etl);

    debug("Expired keys found", filteredRowKeys.length);

    const chunksDeletions = this.arrayToChunks(filteredRowKeys, 100);

    // Batching the delete
    for (const chunksDeletion of chunksDeletions) {
      try {
        await Promise.all(
          chunksDeletion.map((rowKeyTTL: string) => {

            const key = rowKeyTTL.split("#")[0];
            const column = rowKeyTTL.split(":")[1];

            this.btClient.emit("expired", {key, column});

            // Delete both the entry in metadata table and the origin table
            return [
              this.btClient.tableMetadata.row(rowKeyTTL).delete(),
              this.btClient.delete(key, column),
            ];
          }).reduce((prev: any, next: any) => prev.concat(next), []),
        );
        debug("Deleted %s keys", chunksDeletion.length);

        const deletedChunks = chunksDeletion.map((rowKeyTTL: string) =>
          ({
            key: rowKeyTTL.split("#")[0],
            column: rowKeyTTL.split(":")[1],
          }),
        );

        // Emit if it is completed
        this.btClient.emit("expired", deletedChunks);
      } catch (error) {
        debug(error);
      }

    }
  }

  public close() {
    if (this.tov) {
      debug("Stopping job..");
      clearTimeout(this.tov);
    }
  }
}
