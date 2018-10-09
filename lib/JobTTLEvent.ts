import { BigtableClient } from "./BigtableClient";
import * as Debug from "debug";

const debug = Debug("yildiz:bigtable:jobttl");

export class JobTTLEvent {

  private btClient: BigtableClient;
  private intervalInMs: number;
  private tov!: NodeJS.Timer;

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
   * Delete the expired cells after it is resolved, the job will run again
   */
  private async deleteExpiredData() {

    const etl = (result: any) => {
      return {
        [result.id]: Object.keys(result.data[this.btClient.cfNameMetadata]),
      };
    };

    const ranges = [];
    const nowTimestamp = Date.now();

    for (let i = 0; i < this.btClient.clusterCount; i++) {
      ranges.push({
        start: `ttl#${i}#0`,
        end: `ttl#${i}#${nowTimestamp}`,
      });
    }

    const options = {
      ranges,
      limit: this.btClient.ttlBatchSize,
    };

    const filteredTTL = await this.btClient.scanCellsInternal(this.btClient.tableMetadata, options, etl);

    // Return early
    if (!filteredTTL || !filteredTTL.length) {
      return;
    }

    // Batching the delete
    const chunksDeletions = this.arrayToChunks(filteredTTL, 100);

    for (const chunksDeletion of chunksDeletions) {
      try {
        await Promise.all(
          chunksDeletion.map((ttlData: any) => {

            const rowKeyTTL = Object.keys(ttlData)[0];
            const qualifiers = ttlData[rowKeyTTL];

            // Delete both the entry in metadata table and the origin table
            return [
              this.btClient.tableMetadata.row(rowKeyTTL).delete().catch((error) =>
                debug("Error during TTL deletion on Metadata table", error.message)),

              ...(
                qualifiers
                  .map((qualifier: string) => {

                  const splitQualifier = qualifier.split("#");
                  const row = splitQualifier[1];
                  const column = splitQualifier[2];

                  return this.btClient.delete(row, column)
                    // Emit an event if it is deleted
                    .then((result) => {
                      this.btClient.emit("expired", {row, column});
                    })
                    .catch((error) => debug("Error during TTL deletion on data table", error.message));
              })),
            ];
          }).reduce((prev: any, next: any) => prev.concat(next), []),
        );

        debug("Deleted %s rows from Metadata table", chunksDeletion.length);

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
