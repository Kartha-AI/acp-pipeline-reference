export interface SourceConnector {
  /** Connect to source. Fetch credentials from Secrets Manager. */
  connect(): Promise<void>;

  /**
   * List partitions for this run.
   * @param since - For incremental mode, only partitions with changes since this watermark
   */
  listPartitions(since?: string): Promise<Partition[]>;

  /**
   * Stream rows for a single partition. Must be memory-bounded.
   * Use server-side cursors, batch pagination, or native streaming primitives.
   */
  extractPartition(
    partition: Partition,
    since?: string,
  ): AsyncIterable<SourceRow>;

  /** Get current watermark for next-run incremental. Connector-defined format. */
  getCurrentWatermark(): Promise<string>;

  /** Clean up. Always called in finally. */
  disconnect(): Promise<void>;
}

export interface Partition {
  id: string;                                // Used in SQS messages, logs
  sourceFilter: Record<string, unknown>;     // Connector-specific
  estimatedRows?: number;
}

export type SourceRow = Record<string, unknown>;

export interface RowTransformer<TSource extends SourceRow = SourceRow> {
  readonly targetContextType: string;
  toStaging(row: TSource): StagingRow;
}

export interface StagingRow {
  subtype: string;
  source: string;
  source_ref: string;
  canonical_name: string;
  context: SevenDimensions;
}

export interface SevenDimensions {
  attributes: Record<string, unknown>;
  measures: Record<string, unknown>;
  actors: Record<string, unknown>;
  temporals: Record<string, unknown>;
  locations: Record<string, unknown>;
  intents: Record<string, unknown>;
  processes: Record<string, unknown>;
}

export interface PipelineConfig {
  name: string;                              // Becomes staging table suffix
  source: SourceConnector;
  transformer: RowTransformer;
  target: string;                            // ACP context type
  schedule?: string;                         // Cron for incremental
  initial?: {
    estimatedTotalRows?: number;
    extractorConcurrency?: number;
  };
  incremental?: {
    bulkBatchSize?: number;                  // Default 500
    drainConcurrency?: number;               // Default 4
  };
}

export type PipelineMode = 'initial' | 'incremental';

export const PIPELINE_DEFAULTS = {
  incremental: {
    bulkBatchSize: 500,
    drainConcurrency: 4,
  },
} as const;

// Postgres NAMEDATALEN limits identifiers to 63 bytes; the staging table is
// created as `acp_staging.<name>` so the suffix must be a safe SQL identifier.
const PIPELINE_NAME_PATTERN = /^[a-z][a-z0-9_]{0,62}$/;

const SOURCE_METHODS = [
  'connect',
  'listPartitions',
  'extractPartition',
  'getCurrentWatermark',
  'disconnect',
] as const satisfies ReadonlyArray<keyof SourceConnector>;

export function definePipeline(config: PipelineConfig): PipelineConfig {
  if (typeof config.name !== 'string' || config.name.length === 0) {
    throw new Error('definePipeline: `name` is required and must be a non-empty string');
  }
  if (!PIPELINE_NAME_PATTERN.test(config.name)) {
    throw new Error(
      `definePipeline: \`name\` must match ${PIPELINE_NAME_PATTERN} ` +
        `(lowercase letter, then [a-z0-9_], max 63 chars). Got "${config.name}".`,
    );
  }

  if (config.source === null || typeof config.source !== 'object') {
    throw new Error('definePipeline: `source` connector is required');
  }
  for (const method of SOURCE_METHODS) {
    const fn = (config.source as unknown as Record<string, unknown>)[method];
    if (typeof fn !== 'function') {
      throw new Error(`definePipeline: \`source.${method}\` must be a function`);
    }
  }

  if (config.transformer === null || typeof config.transformer !== 'object') {
    throw new Error('definePipeline: `transformer` is required');
  }
  if (typeof config.transformer.toStaging !== 'function') {
    throw new Error('definePipeline: `transformer.toStaging` must be a function');
  }
  if (
    typeof config.transformer.targetContextType !== 'string' ||
    config.transformer.targetContextType.length === 0
  ) {
    throw new Error('definePipeline: `transformer.targetContextType` is required');
  }

  if (typeof config.target !== 'string' || config.target.length === 0) {
    throw new Error('definePipeline: `target` context type is required');
  }

  if (config.schedule !== undefined && typeof config.schedule !== 'string') {
    throw new Error('definePipeline: `schedule` must be a string when provided');
  }

  const incremental = {
    bulkBatchSize:
      config.incremental?.bulkBatchSize ?? PIPELINE_DEFAULTS.incremental.bulkBatchSize,
    drainConcurrency:
      config.incremental?.drainConcurrency ?? PIPELINE_DEFAULTS.incremental.drainConcurrency,
  };
  if (!Number.isInteger(incremental.bulkBatchSize) || incremental.bulkBatchSize <= 0) {
    throw new Error('definePipeline: `incremental.bulkBatchSize` must be a positive integer');
  }
  if (!Number.isInteger(incremental.drainConcurrency) || incremental.drainConcurrency <= 0) {
    throw new Error('definePipeline: `incremental.drainConcurrency` must be a positive integer');
  }

  if (config.initial !== undefined) {
    const { estimatedTotalRows, extractorConcurrency } = config.initial;
    if (
      estimatedTotalRows !== undefined &&
      (!Number.isInteger(estimatedTotalRows) || estimatedTotalRows < 0)
    ) {
      throw new Error('definePipeline: `initial.estimatedTotalRows` must be a non-negative integer');
    }
    if (
      extractorConcurrency !== undefined &&
      (!Number.isInteger(extractorConcurrency) || extractorConcurrency <= 0)
    ) {
      throw new Error('definePipeline: `initial.extractorConcurrency` must be a positive integer');
    }
  }

  const resolved: PipelineConfig = {
    name: config.name,
    source: config.source,
    transformer: config.transformer,
    target: config.target,
    incremental,
  };
  if (config.schedule !== undefined) resolved.schedule = config.schedule;
  if (config.initial !== undefined) resolved.initial = config.initial;
  return resolved;
}
