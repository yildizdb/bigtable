"use strict";

const assert = require("assert");
const {spawn, spawnSync} = require("child_process");

const {BigtableFactory} = require("../../dist/lib/BigtableFactory.js");
const testName = "Bigtable Client Test";

const config = {
  projectId: "my-project-1",
  instanceName: "my-bigtable-cluster",
  keyFilename: "",
  ttlScanIntervalMs: 500,
  minJitterMs: 30,
  maxJitterMs: 50,
};

const configClient = {
  name: "mytable",
  columnFamily: "myfamily",
  defaultColumn: "default",
  defaultValue: "",
  maxVersions: 1,
};

const waitForSeconds = async (seconds) => await new Promise(resolve => setTimeout(resolve, seconds * 1000));

let btClient = null;

let expiredData = {};
let emitCounts = 0;

let emulatorProcess;

describe(testName, () => {

  before("start the BigTable emulator", () => {
    process.env["BIGTABLE_EMULATOR_HOST"] = "127.0.0.1:8086";
    emulatorProcess = spawn("sh", [
      "-c",
      "gcloud beta emulators bigtable start",
    ]);
  });

  after("stop the BigTable emulator", () => {
    if (emulatorProcess) {
      emulatorProcess.kill("SIGINT");
    }

    spawnSync("sh", [
      "-c",
      "kill $(ps aux | grep '[c]btemulator' | awk '{print $2}')",
    ]);

    delete process.env["BIGTABLE_EMULATOR_HOST"];
  });

  before(async () => {
    const btFactory = new BigtableFactory(config);
    await btFactory.init(false);
    btClient = await btFactory.get(configClient);
    btClient.on("expired", (data) => {
      expiredData = data;
      emitCounts++;
    })
  });

  after(() => {
    btClient.close();
  });

  it("should be able to do simple set on a client", async () => {
    const rowKey = "aRowKey";
    const value = "a string value"
    await btClient.set(rowKey, value);
    const retrievedValue = await btClient.get(rowKey);

    assert.equal(retrievedValue, value);
  });

  it("should be able to do simple set on specified column", async () => {
    const rowKey = "anotherRowKey";
    const value = "string value in new column"
    const newColumn = "newColumn";

    await btClient.set(rowKey, value, null, newColumn);
    const retrievedValue = await btClient.get(rowKey, newColumn);

    assert.equal(retrievedValue, value);
  });

  it("should be able to do a multiset", async () => {
    const rowKey = "multiSetRowKey";
    const columnsObject = {firstColumn: "yeah", secondColumn: "this works"};

    await btClient.multiSet(rowKey, columnsObject);
    const retrievedObject = await btClient.getRow(rowKey);

    assert.deepEqual(columnsObject, retrievedObject);
  });

  it("should be able to do increase", async () => {
    const rowKey = "increasinNumberRowKey";
    const numberColumn = "numberColumn";
    await btClient.increase(rowKey, numberColumn);
    await btClient.increase(rowKey, numberColumn);

    const retrievedValue = await btClient.get(rowKey, numberColumn);

    assert.equal(retrievedValue, 2);
  });

  it("should be able to do decrease", async () => {
    const rowKey = "decreasingNumberRowKey";
    const numberColumn = "numberColumn";
    await btClient.decrease(rowKey, numberColumn);
    await btClient.decrease(rowKey, numberColumn);

    const retrievedValue = await btClient.get(rowKey, numberColumn);

    assert.equal(retrievedValue, -2);
  });

  it("should be able to do additional integer operation per row", async () => {
    const rowKey = "intOpsNumberRowKey";

    await btClient.multiAdd(rowKey, {foo: 3, bar: 2});
    await btClient.multiAdd(rowKey, {foo: -2, bar: 8});

    const retrievedObject = await btClient.getRow(rowKey);

    assert.equal(retrievedObject.foo, 1);
    assert.equal(retrievedObject.bar, 10);
  });

  it("should be able to count", async () => {
    await btClient.set("countRowKey", "dummy value");
    const retrievedValue = await btClient.count();

    assert.equal(retrievedValue >= 1, true);
    await btClient.set("anotherCountRowKey", "another dummy value");
    const updatedValue = await btClient.count();
    assert.equal(updatedValue, retrievedValue + 1);
  });

  it("should be able to delete as single cell", async () => {
    const rowKey = "singleCellDeleteRowKey";
    const columnToBeDeleted = "deleteMe";
    const columnsObject = {[columnToBeDeleted]: "yes", doNotDeleteMe: "please"};
    await btClient.multiSet(rowKey, columnsObject);

    await btClient.delete(rowKey, columnToBeDeleted);
    const retrievedValue = await btClient.get(rowKey, columnToBeDeleted);
    assert.equal(retrievedValue, null);

    const retrievedRow = await btClient.getRow(rowKey);
    assert.deepEqual(retrievedRow, {doNotDeleteMe: "please"});
  });

  it("should be able to delete a row and count correctly", async () => {
    const rowKey = "deleteRowKey";
    const columnsObject = {deleteMe: "yes", deleteMeToo: "please"};
    await btClient.multiSet(rowKey, columnsObject);

    const retrievedValueBefore = await btClient.count();

    await btClient.deleteRow(rowKey);
    const retrievedObject = await btClient.getRow(rowKey);

    const retrievedValueAfter = await btClient.count();

    assert.equal(retrievedObject, null);
    assert.equal(retrievedValueAfter, retrievedValueBefore - 1);
  });

  it("should be able to set a TTL on a single cell", async () => {
    const rowKey = "singleCellTTLRowKey";
    const column = "ttlColumn";
    const value = "to be deleted";

    await btClient.set(rowKey, value, 3, column);
    const resultBeforeTTL = await btClient.get(rowKey, column);
    await waitForSeconds(5);
    const resultAfterTTL = await btClient.get(rowKey, column);

    assert.equal(resultBeforeTTL, value);
    assert.equal(resultAfterTTL, null);
  });

  it.only("should be able to set a TTL during a multi set", async () => {
    const rowKey = "mutliSetTTLRowKey";
    const column = "noTTLColumn";
    const value = "not to be deleted";
    const ttlColumns = {deleteMe: "yes", deleteMeToo: "please"};

    btClient.on("expired", ({row, column}) => {
      console.log({row, column});
    });

    await btClient.set(rowKey, value, undefined, column);
    const ttl1 = await btClient.ttl(rowKey, column);
    console.log({ttl1});
    await btClient.multiSet(rowKey, ttlColumns, 3);

    const ttl2 = await btClient.ttl(rowKey, column);
    console.log({ttl2});
    const ttl3 = await btClient.ttl(rowKey, "deleteMe");
    console.log({ttl3});
    await waitForSeconds(5);
    const result = await btClient.getRow(rowKey);
    const result2 = await btClient.get(rowKey, column);
    console.log(result2);

    assert.deepEqual(result, {[column]: value});
  });

  it.skip("should be able to set TTL on various cases", async () => {

    await btClient.set(rowKey, value, 9, "newColumn");
    await btClient.multiSet(rowKey, {testColumn: "hello", anotherColumn: "yes"}, 8);
    await btClient.increase(rowKey, "numberColumn", 2);
    await btClient.multiAdd(rowKey, {foo: 1, bar: -5}, 7);

    await btClient.bulkInsert([
      {
        row: "jean-paul",
        column: "sartre",
        data: "france",
        ttl: 3,
      },
      {
        row: "emmanuel",
        column: "kant",
        data: "germany",
      },
      {
        row: "baruch",
        column: "spinoza",
        data: "netherland",
      },
    ], 3);

    await btClient.bulkInsert([
      {
        row: "jean-paul",
        column: "sartre",
        data: "france",
        ttl: 7,
      },
      {
        row: "emmanuel",
        column: "kant",
        data: "germany",
        ttl: 8,
      },
    ], 6);
  });

  it.skip("should wait for 4 seconds", (done) => {
    setTimeout(done, 4000);
  });

  it.skip("should be able to delete expired data based on TTL Job", async () => {
    const retrievedValue = await btClient.get(rowKey, "numberColumn");
    const retrievedValueBulk = await btClient.get("baruch", "spinoza");

    assert.equal(retrievedValue, null);
    assert.equal(retrievedValueBulk, null);
  });

  it.skip("should be able to bump TTL on Bulk", async () => {
    const retrievedValueBulk1 = await btClient.get("emmanuel", "kant");
    const retrievedValueBulk2 = await btClient.get("jean-paul", "sartre");

    assert.ok(retrievedValueBulk1);
    assert.ok(retrievedValueBulk2);
  });

  it.skip("should be able to emit the correct data on expiration", async () => {
    assert.ok(expiredData.row);
    assert.ok(expiredData.column);
  });

  it.skip("should wait for 4 seconds", (done) => {
    setTimeout(done, 4000);
  });

  it.skip("should delete the remainder data", async () => {
    await btClient.deleteRow("newRowKey1");
    await btClient.deleteRow("newRowKey2");
  });

  it.skip("should wait for 3 seconds", (done) => {
    setTimeout(done, 3000);
  });

  it.skip("should be able to emit correct number of times", async () => {
    assert.equal(emitCounts, 9);
  });

  it.skip("should delete all the value based on the ttl set", async () => {
    const retrievedObject = await btClient.getRow(rowKey);

    assert.equal(retrievedObject, null);
  });

  it.skip("should be able to do clean up", async () => {

    btClient.close();
    await btClient.cleanUp();
  });

});
