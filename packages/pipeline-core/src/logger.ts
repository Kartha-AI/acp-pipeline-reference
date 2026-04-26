import { pino } from 'pino';
import type { Logger as PinoLogger, LoggerOptions } from 'pino';

import type { PipelineMode } from './types.js';

export type Logger = PinoLogger;

export interface PipelineLoggerContext {
  pipelineName?: string;
  batchId?: string;
  mode?: PipelineMode;
  partitionId?: string;
  /** Lifecycle phase: 'trigger' | 'extract' | 'stage' | 'promote' | 'drain' | etc. */
  phase?: string;
  [key: string]: unknown;
}

const baseOptions: LoggerOptions = {
  level: process.env['LOG_LEVEL'] ?? 'info',
  base: { service: 'acp-pipeline' },
  // Emit { "level": "info" } instead of the default numeric level — easier to grep
  // and matches conventions used elsewhere in the ACP platform.
  formatters: {
    level: (label) => ({ level: label }),
  },
  timestamp: pino.stdTimeFunctions.isoTime,
};

export const rootLogger: Logger = pino(baseOptions);

export function createPipelineLogger(
  context: PipelineLoggerContext,
  parent: Logger = rootLogger,
): Logger {
  return parent.child(context);
}
