import * as Debug from "debug";

import { BigtableFactory } from "..";

const debug = Debug("yildiz:bigtable:example");

const config = {
  projectId: "my-project-1",
  instanceName: "my-bigtable-cluster",
  keyFilename: "keyfile.json",
  ttlScanIntervalMs: 2000,
  minJitterMs: 2000,
  maxJitterMs: 5000,
};

const myTableConfig = {
  name: "mytable",
  columnFamily: "myfamily",
  defaultColumn: "default",
  maxVersions: 1,
};

const sleep = (ms: any) => {
  return new Promise((resolve) => {
      setTimeout(resolve, ms);
  });
};

(async () => {

    // the goal of this api is to take away the complexity of the original big table api
    // by encapsulating columnFamilies and multi cell rows, via default values
    // and also adding ttl and metadata with sub millisecond access per default

    const bigtableFactory = new BigtableFactory(config);
    await bigtableFactory.init();

    const myInstance = await bigtableFactory.get(myTableConfig);

    const rowKey = "myrowkey";

    myInstance.on("expired", (data: any) => {
      debug("expired row:", data);
    });

    // when column is null, the default column from the config is used
    // ttl only works on cell level, if a row has no more cells, bigtable will delete the row automatically

    // we create a ${config.name}_metadata table for every table we create
    // we use it to keep track of cell ttls and scan them
    // we use it to keep track of the current count (size) of the table

    // you can append by calling set with different column values
    const value = "myvalue";
    await myInstance.set(rowKey, value);
    await myInstance.set(rowKey, value, 7, "newColumn");

    await myInstance.multiSet(rowKey, {testColumn: "hello", anotherColumn: "yes"});
    await myInstance.multiSet(rowKey, {testColumn: "hello", anotherColumn: "yes"}, 5);

    await myInstance.increase(rowKey, "numberColumn");
    await myInstance.increase(rowKey, "numberColumn");
    await myInstance.decrease(rowKey, "numberColumn");

    await myInstance.bulkInsert([
      {
        row: "jean-paul",
        column: "sartre",
        data: "france",
      },
      {
        row: "emmanuel",
        column: "kant",
        data: "germany",
      },
      {
        row: "albert",
        column: "camus",
        data: "france",
      },
    ], 5);

    await myInstance.bulkInsert([
      {
        row: "thomas",
        column: "hobbes",
        data: "england",
      },
      {
        row: "friedrich",
        column: "nietzche",
        data: "germany",
      },
    ]);

    await myInstance.multiAdd(rowKey, {foo: 1, bar: -5}, 5);

    debug(await myInstance.get(rowKey));
    debug(await myInstance.get(rowKey, "numberColumn"));

    debug("waiting...");
    await sleep(9000);

    await myInstance.set("rowKey1", value);
    debug("counts :", await myInstance.count());
    await myInstance.delete(rowKey);
    await myInstance.delete(rowKey, "foo");

    debug(await myInstance.getRow(rowKey));
    await myInstance.deleteRow(rowKey);
    await myInstance.deleteRow("rowKey1");

    debug("counts :", await myInstance.count());
    bigtableFactory.close();

})().catch(console.error);
