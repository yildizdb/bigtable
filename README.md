# bigtable

`yarn add bigtable`

## Intro

This is a TypeScript Bigtable client, it acts as wrapper around the official 
Google package [@google-cloud/bigtable](https://github.com/googleapis/nodejs-bigtable).
When working with Bigtable we almost always had the urge to wrap the API to add a pinch
of convenience to it, as well implement a way to get TTL (per cell basis), as well as metadata
information such as a simple count more efficiently.

This client automatically manages a `metadata` table and `ttl jobs` for every table that you manage
through it. Additionally it aims to mimic a simple CRUD interface, that is offered by a lot of redis packages
like `ioredis` for example.

Additionally the setup and all operation (except for scan) are optimized for sub-millisecond response times
(depending on your Google Cloud Bigtable Instance configuration), which helps you to develop real-time
applications based on this Bigtable. This client is not ment to be used for analytical purposes, although
it is fairly possible through scan operations.

## Using

Using it is fairly simple:

First, you have to setup a factory instance, which gets the general
configuration to connect to your Bigtable instance.
NOTE: If the instance you describe does not exist, it will be created.

```javascript
const bigtableFactory = new BigtableFactory({

  projectId: "my-project-1", // -> see @google-cloud/bigtable configuration
  instanceName: "my-bigtable-cluster", // -> see @google-cloud/bigtable configuration
  //keyFilename: "keyfile.json", // -> see @google-cloud/bigtable configuration

  // optional:
  ttlScanIntervalMs: 5000,
  minJitterMs: 2000,
  maxJitterMs: 30000,
});
await bigtableFactory.init();
```

Then, using the factory you can create handles for you tables very easily.
You can see that we are taking away the complexity of handling columnFamilies and
columns in general, by assuming default values in the API that can be set via config optionally.
However the API always allows you to access cells (by passing a column name as parameter) directly,
as well as accessing and deleting whole rows.

```javascript
const myTable = await bigtableFactory.get({
  name: "mytable",

  // optional:
  columnFamily: "myfamily",
  defaultColumn: "default",
  maxVersions: 1,
});

const rowKey = "myrowkey";
const value = "myvalue";

await myTable.set(rowKey, value);
await myTable.set(rowKey, value, 10, "newColumn");

await myTable.multiSet(rowKey, {testColumn: "hello", anotherColumn: "yes"}, 5);

await myTable.increase(rowKey);
await myTable.decrease(rowKey);

await myTable.multiAdd(rowKey, {foo: 1, bar: -5}, 7);
await myTable.get(rowKey);

await myTable.ttl(rowKey);
await myTable.count();

await myTable.getRow(rowKey)
await myTable.deleteRow(rowKey);

myTable.close(); // or bigtableFactory.close();
```

You can also scan tables (be carefull as these operations are slow).

```javascript
const filters = [
    {
        // -> check out the official api for bigtable filters: https://cloud.google.com/nodejs/docs/reference/bigtable/0.13.x/Filter#interleave
    }
];

const etl = (row) => {
    return row.id || null;
};

const cells = await myTable.scanCells(filters, etl);
```

You can activate debug logs via env variable `DEBUG=yildiz:bigtable:*`.

You can find additional implementation examples here:
* [Example File](example/index.ts)
* [Integration Test](test/int/Service.test.js)

# License

License is MIT