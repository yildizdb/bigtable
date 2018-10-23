export interface BigtableFactoryConfig {
  projectId: string;
  instanceName: string;
  keyFilename: string;
  ttlScanIntervalMs: number;
  minJitterMs: number;
  maxJitterMs: number;
  clusterCount?: number;
  murmurSeed?: number;
  ttlBatchSize?: number;
}

export interface BigtableClientConfig {
  name: string;
  columnFamily: string;
  defaultColumn: string;
  defaultValue?: string;
  maxVersions?: number;
  maxAgeSecond?: number;
}

export interface RuleColumnFamily {
  versions: number;
  age?: {
    seconds: number;
  };
  union?: boolean;
}

export interface BulkData {
  family?: string;
  row: string;
  column: string;
  data: any;
  ttl?: number;
}
