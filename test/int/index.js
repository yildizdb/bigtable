"use strict";

const assert = require("assert");

const { BigtableFactory } = require("./../../dist/lib/BigtableFactory.js");
const testName = "Bigtable Client Test";

const config = {
  projectId: "my-project-1",
  instanceName: "my-bigtable-cluster",
  keyFilename: "keyfile.json",
  ttlScanIntervalMs: 2000,
  minJitterMs: 500,
  maxJitterMs: 1000,
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

describe(testName, () => {


    before(async () => {
      const btFactory = new BigtableFactory(config);
      await btFactory.init();
      btClient = await btFactory.get(configClient);
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
      await btClient.increase(rowKey, "numberColumn", 3);
      await btClient.multiAdd(rowKey, {foo: 1, bar: -5}, 7);
    });

    it("should wait for 4 seconds", (done) => {
      setTimeout(done, 4000);
    });

    it("should be able to delete expired data based on TTL Job", async () => {
      const retrievedValue = await btClient.get(rowKey, "numberColumn");

      assert.equal(retrievedValue, 1);
    });

    it("should wait for 4 seconds", (done) => {
      setTimeout(done, 4000);
    });

    it("should get the remaining time based on the ttl", async () => {
      const ttl = await btClient.ttl(rowKey, "newColumn");

      assert.ok((ttl < 3));
    });

    it("should wait for 3 seconds", (done) => {
      setTimeout(done, 3000);
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