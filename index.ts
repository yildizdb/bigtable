/// <reference path="./lib/declarations/GoogleBigtable.d.ts"/>
import { BigtableFactory } from "./lib/BigtableFactory";
import { BigtableClient } from "./lib/BigtableClient";

export default BigtableFactory;
export { BigtableFactory, BigtableClient }; // faked object export
export { BigtableFactoryConfig, BigtableClientConfig } from "./lib/interfaces/BigtableConfig";
