# Mockup of this clients features

```javascript
    const {BigtableFactory} = require("bigtable");

    const config = {
        projectId: "my-project-1",
        instanceName: "my-bigtable-cluster",
        keyFilename: "/bla/keyfile.json",
        ttlScanIntervalMs: 3000
    };

    const myTableConfig = {
        name: "mytable",
        columnFamily: "myfamily",
        defaultColumn: "default",
        defaultValue: "",
        maxVersions: 1
    };

    (async () => {

        // the goal of this api is to take away the complexity of the original big table api
        // by encapsulating columnFamilies and multi cell rows, via default values
        // and also adding ttl and metadata with sub millisecond access per default

        const bigtableFactory = new BigtableFactory(config);
        const myInstance = await bigtableFactory.get(myTableConfig);

        const rowKey = "myrowkey";
        const value = "myvalue";
        const ttl = 15000;

        // when column is null, the default column from the config is used
        // ttl only works on cell level, if a row has no more cells, bigtable will delete the row automatically

        // we create a ${config.name}_metadata table for every table we create
        // we use it to keep track of cell ttls and scan them
        // we use it to keep track of the current count (size) of the table

        await myInstance.set(rowKey, value, ttl = null, column = null); // <- you can append by calling set with different column values
        await myInstance.multiSet(rowKey, columnsObject, ttl);

        await myInstance.increase(rowKey, column = null);
        await myInstance.decrease(rowKey, column = null);

        await myInstance.get(rowKey, column = null);
        await myInstance.delete(rowKey, column = null);

        await myInstance.getRow(rowKey);
        await myInstance.deleteRow(rowKey);
        
        await myInstance.ttl(rowKey, column = null); // <- answered by metadata table
        await myInstance.count(); // <- answered by metadata table

    })().catch(console.error);
```