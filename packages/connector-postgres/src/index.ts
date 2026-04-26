export { PostgresConnector, postgresConnector } from './connector.js';
export type {
  PostgresConnectorConfig,
  PartitionStrategy,
  NaturalPartitionStrategy,
  RangePartitionStrategy,
  WatermarkConfig,
} from './connector.js';
export type {
  PostgresConnectorAuth,
  ConnectionStringAuth,
  SecretsManagerAuth,
  IamAuth,
} from './auth.js';
export { streamQuery } from './streaming.js';
export type { CursorStreamOptions } from './streaming.js';
