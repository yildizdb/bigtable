"use strict";

const assert = require("assert");

const { BigtableFactory } = require("./../../dist/lib/BigtableFactory.js");
const testName = "Bigtable Client Test";

const config = {
  projectId: "my-project-1",
  instanceName: "my-bigtable-cluster",
  keyFilename: "keyfile.json",
  ttlScanIntervalMs: 2000,
  minJitterMs: 2000,
  maxJitterMs: 5000,
};

const configClient = {
  name: "mytable",
  columnFamily: "myfamily",
  defaultColumn: "default",
  defaultValue: "",
  maxVersions: 1,
};

let btClient = null;
const rowKey = "myrowkey";
const value = "myvalue";

let expiredData = {};
let emitCounts = 0;

describe(testName, () => {


    before(async () => {
      const btFactory = new BigtableFactory(config);
      await btFactory.init();
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
      await btClient.set(rowKey, value);
      const retrievedValue = await btClient.get(rowKey);

      assert.equal(retrievedValue, value);
    });

    it("should be able to do simple set on specified column", async () => {
      const newColumn = "newColumn";

      await btClient.set(rowKey, value, null, newColumn);
      const retrievedValue = await btClient.get(rowKey, newColumn);

      assert.equal(retrievedValue, value);
    });

    it("should be able to do multiset", async () => {

      await btClient.multiSet(rowKey, {testColumn: "hello", anotherColumn: "yes"});
      const retrievedObject = await btClient.getRow(rowKey);

      assert.equal(retrievedObject.testColumn, "hello");
      assert.equal(retrievedObject.anotherColumn, "yes");
    });

    it("should be able to do increase", async () => {
      const numberColumn = "numberColumn";
      await btClient.increase(rowKey, numberColumn);
      await btClient.increase(rowKey, numberColumn);

      const retrievedValue = await btClient.get(rowKey, numberColumn);

      assert.equal(retrievedValue, 2);
    });

    it("should be able to do decrease", async () => {

      const numberColumn = "numberColumn";
      await btClient.decrease(rowKey, numberColumn);

      const retrievedValue = await btClient.get(rowKey, numberColumn);

      assert.equal(retrievedValue, 1);
    });

    it("should be able to do additional integer operation per row", async () => {

      await btClient.multiAdd(rowKey, {foo: 3, bar: 2});
      await btClient.multiAdd(rowKey, {foo: -2, bar: 8});

      const retrievedObject = await btClient.getRow(rowKey);

      assert.equal(retrievedObject.foo, 1);
      assert.equal(retrievedObject.bar, 10);
    });

    it("should be able to do count", async () => {

      const retrievedValue = await btClient.count();

      assert.equal(retrievedValue, 1);
    });

    it("should be able to do count correctly on add", async () => {

      await btClient.set("newRowKey1", "sweden");
      await btClient.set("newRowKey2", "stockholm");
      const retrievedValue = await btClient.count();

      assert.equal(retrievedValue, 3);
    });

    it("should be able to do single deletion", async () => {

      await btClient.delete(rowKey);
      const retrievedValue = await btClient.get(rowKey);

      assert.equal(retrievedValue, null);
    });

    it("should be able to do deletion on specified column", async () => {

      await btClient.delete(rowKey, "newColumn");
      const retrievedValue = await btClient.get(rowKey, "newColumn");

      assert.equal(retrievedValue, null);
    });

    it("should be able to do deletion on row", async () => {

      await btClient.deleteRow(rowKey);
      const retrievedObject = await btClient.getRow(rowKey);

      assert.equal(retrievedObject, null);
    });

    it("should be able to do count correctly after deletion", async () => {

      const retrievedValue = await btClient.count();

      assert.equal(retrievedValue, 2);
    });

    it("should be able to set TTL on various cases", async () => {

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
          ttl: 4,
        },
        {
          row: "emmanuel",
          column: "kant",
          data: "germany",
          ttl: 7,
        },
      ], 6);
    });

    it("should wait for 4 seconds", (done) => {
      setTimeout(done, 4000);
    });

    it("should be able to delete expired data based on TTL Job", async () => {
      const retrievedValue = await btClient.get(rowKey, "numberColumn");
      const retrievedValueBulk = await btClient.get("baruch", "spinoza");

      assert.equal(retrievedValue, null);
      assert.equal(retrievedValueBulk, null);
    });

    it("should be able to bump TTL on Bulk", async () => {
      const retrievedValueBulk1 = await btClient.get("emmanuel", "kant");
      const retrievedValueBulk2 = await btClient.get("jean-paul", "sartre");

      assert.ok(retrievedValueBulk1);
      assert.ok(retrievedValueBulk2);
    });

    it("should be able to emit the correct data on expiration", async () => {
      assert.ok(expiredData.row);
      assert.ok(expiredData.column);
    });

    it("should wait for 4 seconds", (done) => {
      setTimeout(done, 4000);
    });

    it("should delete the remainder data", async () => {
      await btClient.deleteRow("newRowKey1");
      await btClient.deleteRow("newRowKey2");
    });

    it("should wait for 3 seconds", (done) => {
      setTimeout(done, 3000);
    });

    it("should be able to emit correct number of times", async () => {
      assert.equal(emitCounts, 9);
    });

    it("should delete all the value based on the ttl set", async () => {
      const retrievedObject = await btClient.getRow(rowKey);

      assert.equal(retrievedObject, null);
    });

    it("should be able to do clean up", async () => {

      btClient.close();
      await btClient.cleanUp();
    });

});