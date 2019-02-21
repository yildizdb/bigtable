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

let expiredData = [];

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
      expiredData.push(data);
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
    expiredData = [];
    const rowKey = "singleCellTTLRowKey";
    const column = "ttlColumn";
    const value = "to be deleted";

    await btClient.set(rowKey, value, 1, column);
    const resultBeforeTTL = await btClient.get(rowKey, column);
    await waitForSeconds(2);
    const resultAfterTTL = await btClient.get(rowKey, column);

    assert.equal(resultBeforeTTL, value);
    assert.equal(resultAfterTTL, null);
    assert.equal(expiredData.length, 1);
    assert.deepEqual(expiredData, [{row: rowKey, column}])
  });

  it("should be able to set a TTL during a multi add", async () => {
    const rowKey = "mutliAddTTLRowKey";
    const ttlColumns = {deleteMe: "yes", deleteMeToo: "please"};

    await btClient.multiAdd(rowKey, ttlColumns, 1);
    await waitForSeconds(2);

    const result = await btClient.getRow(rowKey);
    assert.deepEqual(result, null);
  });

  it("should be able to set a TTL during a multi set", async () => {
    const rowKey = "mutliSetTTLRowKey";
    const column = "noTTLColumn";
    const value = "not to be deleted";
    const ttlColumns = {deleteMe: "yes", deleteMeToo: "please"};

    await btClient.set(rowKey, value, undefined, column);
    await btClient.multiSet(rowKey, ttlColumns, 1);
    await waitForSeconds(2);

    const result = await btClient.getRow(rowKey);
    assert.deepEqual(result, {[column]: value});
  });

  it("should not emit an already deleted cell when expired", async () => {
    expiredData = [];
    const rowKey = "deletedBeforeTTLRowKey";
    const ttlColumn = "deleteMeAfterTTL"
    const columnToBeDeletedEarly = "deleteMeEarly";
    const ttlColumns = {[ttlColumn]: "yes", [columnToBeDeletedEarly]: "dont tell anyone"};

    await btClient.multiSet(rowKey, ttlColumns, 1);
    await btClient.delete(rowKey, columnToBeDeletedEarly);
    await waitForSeconds(2);

    assert.deepEqual(expiredData[0], {row: rowKey, column: ttlColumn});
    assert.equal(expiredData.length, 1);
  });

  it("should be able to set a TTL during an increase", async () => {
    const rowKey = "increaseTTLColumn";
    const column = "numberColumn";

    await btClient.increase(rowKey, column, 1);
    await waitForSeconds(2);

    const result = await btClient.get(rowKey, column);

    assert.equal(result, null);
  });

  it("should be able to set a TTL during a bulk insert", async () => {
    const rowKey = "bulkInsertTTLColumn";
    const earlierColumn = "sartre";
    const laterColumn = "kant";
    const laterColumValue = "germany"

    await btClient.bulkInsert([
      {
        row: rowKey,
        column: earlierColumn,
        data: "france",
        ttl: 1,
      },
      {
        row: rowKey,
        column: laterColumn,
        data: laterColumValue,
      },
    ], 3);
    await waitForSeconds(2);

    const earlierColumnAfterTTL = await btClient.get(rowKey, earlierColumn);
    const laterColumnBeforeTTL = await btClient.get(rowKey, laterColumn);

    await waitForSeconds(2);

    const laterColumnAfterTTL = await btClient.get(rowKey, laterColumn);

    assert.equal(earlierColumnAfterTTL, null);
    assert.equal(laterColumnBeforeTTL, laterColumValue);
    assert.equal(laterColumnAfterTTL, null);
  });

  it("should be able to clean up", async () => {

    btClient.close();
    await btClient.cleanUp();
  });

});
