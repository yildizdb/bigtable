export interface BigtableFactoryConfig {
  projectId: string;
  instanceName: string;
  keyFilename: string;
  ttlScanIntervalMs: number;
  minJitterMs: number;
  maxJitterMs: number;
}

export interface BigtableClientConfig {
  name: string;
  columnFamily: string;
  defaultColumn: string;
  defaultValue?: string;
  maxVersions?: number;
}
