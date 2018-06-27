"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const __1 = require("..");
const config = {
    // projectId: "my-project-1",
    // instanceName: "my-bigtable-cluster",
    projectId: "rd-bigdata-prd-v002",
    instanceName: "plat-ca-1",
    // keyFilename: "keyfile.json",
    keyFilename: "/home/rjmasikome/.config/gcloud/application_default_credentials.json",
    ttlScanIntervalMs: 2000,
};
const myTableConfig = {
    name: "mytable",
    columnFamily: "myfamily",
    defaultColumn: "default",
    defaultValue: "",
    maxVersions: 1,
};
const sleep = (ms) => {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
};
(async () => {
    // the goal of this api is to take away the complexity of the original big table api
    // by encapsulating columnFamilies and multi cell rows, via default values
    // and also adding ttl and metadata with sub millisecond access per default
    const bigtableFactory = new __1.BigtableFactory(config);
    await bigtableFactory.init();
    const myInstance = await bigtableFactory.get(myTableConfig);
    const rowKey = "myrowkey";
    const ttl = 15000;
    // when column is null, the default column from the config is used
    // ttl only works on cell level, if a row has no more cells, bigtable will delete the row automatically
    // we create a ${config.name}_metadata table for every table we create
    // we use it to keep track of cell ttls and scan them
    // we use it to keep track of the current count (size) of the table
    // you can append by calling set with different column values
    const value = "myvalue";
    await myInstance.set(rowKey, value);
    await myInstance.set(rowKey, value, 10, "newColumn");
    await myInstance.multiSet(rowKey, { testColumn: "hello", anotherColumn: "yes" });
    await myInstance.multiSet(rowKey, { testColumn: "hello", anotherColumn: "yes" }, 5);
    await myInstance.increase(rowKey, "numberColumn");
    await myInstance.increase(rowKey, "numberColumn");
    await myInstance.decrease(rowKey, "numberColumn");
    await myInstance.multiAdd(rowKey, { foo: 1, bar: -5 }, 7);
    console.log(await myInstance.get(rowKey));
    console.log(await myInstance.get(rowKey, "numberColumn"));
    console.log(`ttl ${rowKey} `, await myInstance.ttl(rowKey));
    console.log(`ttl ${rowKey}:newColumn `, await myInstance.ttl(rowKey, "newColumn"));
    console.log("waiting...");
    await sleep(7000);
    console.log("counts :", await myInstance.count());
    await myInstance.delete(rowKey);
    await myInstance.delete(rowKey, "foo");
    console.log(await myInstance.getRow(rowKey));
    await myInstance.deleteRow(rowKey);
    console.log("counts :", await myInstance.count());
    myInstance.close();
})().catch(console.error);
//# sourceMappingURL=index.js.map