/**
 * Local pipeline runner: trigger -> extractor -> promoter (initial mode)
 *                       trigger -> extractor -> drainer  (incremental mode)
 *
 * Same handlers run as Lambdas / ECS tasks in production. This script is
 * just a different *wiring* — no SQS, no Lambda invoke, no Step Functions.
 *
 * Usage:
 *   pnpm pipeline:run <pipeline-name> --mode <initial|incremental> [--since <iso8601>]
 *
 * Environment:
 *   ACP_PG_URL         Postgres URL for the ACP database (staging + context_objects).
 *                      Default: postgres://acp:localdev@localhost:5432/acp
 *   BULK_API_URL       Base URL of /v1/objects/bulk. If unset and incremental mode
 *                      is used, the script auto-starts a local stub server on
 *                      127.0.0.1:3000 backed by the ACP_PG_URL pool.
 *   BULK_API_TOKEN     Bearer token for /bulk. Optional; matched against the stub
 *                      if both are set.
 *   LOG_LEVEL          pino log level. Default 'info'.
 */

import { pathToFileURL } from 'node:url';
import { resolve as resolvePath } from 'node:path';

import {
  BulkClient,
  createAcpPool,
  createPipelineLogger,
  runDrainer,
  runExtractor,
  runPromote,
  runTrigger,
  startBulkStub,
  WatermarkStore,
} from '@acp/pipeline-core';
import type {
  BulkStubHandle,
  Logger,
  PipelineConfig,
  PipelineMode,
} from '@acp/pipeline-core';

interface CliArgs {
  pipelineName: string;
  mode: PipelineMode;
  since: string | undefined;
}

function parseArgs(argv: ReadonlyArray<string>): CliArgs {
  let pipelineName: string | undefined;
  let mode: PipelineMode | undefined;
  let since: string | undefined;
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--mode') {
      const v = argv[++i];
      if (v !== 'initial' && v !== 'incremental') {
        throw new Error(`--mode must be 'initial' or 'incremental'; got "${v ?? ''}"`);
      }
      mode = v;
    } else if (arg === '--since') {
      since = argv[++i];
      if (since === undefined) throw new Error('--since requires a value');
    } else if (arg === '--help' || arg === '-h') {
      printUsage();
      process.exit(0);
    } else if (arg !== undefined && !arg.startsWith('--')) {
      if (pipelineName !== undefined) {
        throw new Error(`unexpected positional argument "${arg}"`);
      }
      pipelineName = arg;
    } else if (arg !== undefined) {
      throw new Error(`unknown flag "${arg}"`);
    }
  }
  if (pipelineName === undefined) {
    printUsage();
    throw new Error('pipeline name is required');
  }
  if (mode === undefined) {
    throw new Error('--mode is required');
  }
  return { pipelineName, mode, since };
}

function printUsage(): void {
  process.stdout.write(
    [
      'Usage: pnpm pipeline:run <pipeline-name> --mode <initial|incremental> [--since <iso8601>]',
      '  pipeline-name   Subdirectory under pipelines/ (e.g. tpch-customer)',
      '  --mode          initial | incremental',
      '  --since         Override watermark for incremental (otherwise read from pipeline_runs)',
      '',
    ].join('\n'),
  );
}

async function loadPipeline(pipelineName: string): Promise<PipelineConfig> {
  const path = resolvePath(
    process.cwd(),
    'pipelines',
    pipelineName,
    'pipeline.config.ts',
  );
  const url = pathToFileURL(path).href;
  const mod = (await import(url)) as {
    default?: PipelineConfig;
    pipeline?: PipelineConfig;
  };
  const pipeline = mod.default ?? mod.pipeline;
  if (pipeline === undefined) {
    throw new Error(
      `pipelines/${pipelineName}/pipeline.config.ts must export the pipeline (default or named "pipeline")`,
    );
  }
  return pipeline;
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const logger = createPipelineLogger({
    component: 'pipeline-run',
    pipelineName: args.pipelineName,
    mode: args.mode,
  });
  const startedAt = Date.now();

  const pipeline = await loadPipeline(args.pipelineName);
  if (pipeline.name !== args.pipelineName.replace(/-/g, '_')) {
    // Pipeline names are SQL identifiers (underscores), directory names
    // typically use kebab-case. Tolerant comparison so customers don't have
    // to keep them in lockstep, but warn if they diverge unexpectedly.
    logger.debug(
      { configName: pipeline.name, dirName: args.pipelineName },
      'pipeline name does not match directory name (case/separator)',
    );
  }

  const acpPool = await createAcpPool();
  const watermarkStore = new WatermarkStore(acpPool);

  let bulkStub: BulkStubHandle | undefined;
  try {
    const trigger = await runTrigger(
      {
        pipelineName: pipeline.name,
        mode: args.mode,
        ...(args.since !== undefined ? { since: args.since } : {}),
      },
      { pipeline, acpPool, watermarkStore, logger },
    );

    if (trigger.partitions.length === 0) {
      logger.info({ batchId: trigger.batchId }, 'no partitions to extract');
    }

    const extractTotals = await runAllPartitions(trigger, pipeline, acpPool, logger);

    if (args.mode === 'initial') {
      const promote = await runPromote(
        {
          pipelineName: pipeline.name,
          batchId: trigger.batchId,
          runId: trigger.runId,
          ...(trigger.watermarkAtStart !== undefined
            ? { watermarkAtStart: trigger.watermarkAtStart }
            : {}),
        },
        { acpPool, watermarkStore, logger },
      );
      logger.info(
        {
          batchId: trigger.batchId,
          runId: trigger.runId,
          mode: 'initial',
          extracted: extractTotals.extracted,
          staged: extractTotals.staged,
          transformFailed: extractTotals.transformFailed,
          inserted: promote.insertedCount,
          rejected: promote.rejectedCount,
          totalDurationMs: Date.now() - startedAt,
        },
        'pipeline:run initial complete',
      );
      return;
    }

    bulkStub = await maybeStartLocalStub(acpPool, logger);
    const bulkClient = new BulkClient({
      apiUrl: bulkStub?.url ?? process.env['BULK_API_URL']!,
      ...(process.env['BULK_API_TOKEN'] !== undefined
        ? { bearerToken: process.env['BULK_API_TOKEN'] }
        : {}),
      logger,
    });

    const drain = await runDrainer(
      {
        pipelineName: pipeline.name,
        batchId: trigger.batchId,
        runId: trigger.runId,
        ...(trigger.watermarkAtStart !== undefined
          ? { watermarkAtStart: trigger.watermarkAtStart }
          : {}),
      },
      {
        acpPool,
        bulkClient,
        bulkBatchSize: pipeline.incremental?.bulkBatchSize ?? 500,
        watermarkStore,
        logger,
      },
    );
    logger.info(
      {
        batchId: trigger.batchId,
        runId: trigger.runId,
        mode: 'incremental',
        extracted: extractTotals.extracted,
        staged: extractTotals.staged,
        transformFailed: extractTotals.transformFailed,
        rowsPosted: drain.rowsPosted,
        rowsCreated: drain.rowsCreated,
        rowsUpdated: drain.rowsUpdated,
        rowsUnchanged: drain.rowsUnchanged,
        rowsRejected: drain.rowsRejected,
        batchesPosted: drain.batchesPosted,
        totalDurationMs: Date.now() - startedAt,
      },
      'pipeline:run incremental complete',
    );
  } finally {
    if (bulkStub !== undefined) {
      await bulkStub.close().catch(() => undefined);
    }
    await acpPool.end().catch(() => undefined);
  }
}

interface ExtractTotals {
  extracted: number;
  staged: number;
  transformFailed: number;
}

async function runAllPartitions(
  trigger: { batchId: string; mode: PipelineMode; partitions: import('@acp/pipeline-core').Partition[]; watermarkUsed: string | undefined },
  pipeline: PipelineConfig,
  acpPool: import('pg').Pool,
  logger: Logger,
): Promise<ExtractTotals> {
  const totals: ExtractTotals = { extracted: 0, staged: 0, transformFailed: 0 };
  for (const partition of trigger.partitions) {
    const result = await runExtractor(
      {
        pipelineName: pipeline.name,
        mode: trigger.mode,
        batchId: trigger.batchId,
        partition,
        ...(trigger.watermarkUsed !== undefined ? { since: trigger.watermarkUsed } : {}),
      },
      { pipeline, acpPool, logger },
    );
    totals.extracted += result.rowsExtracted;
    totals.staged += result.rowsStaged;
    totals.transformFailed += result.rowsTransformFailed;
  }
  return totals;
}

async function maybeStartLocalStub(
  pool: import('pg').Pool,
  logger: Logger,
): Promise<BulkStubHandle | undefined> {
  const explicit = process.env['BULK_API_URL'];
  if (explicit !== undefined && explicit.length > 0) {
    if (!/^https?:\/\/(localhost|127\.0\.0\.1)/.test(explicit)) {
      // Pointing at a real ACP — drainer will hit it directly.
      return undefined;
    }
    // Even with an explicit localhost URL, start the stub so the test loop
    // works without separately running it.
    const port = Number(new URL(explicit).port) || 3000;
    return startBulkStub({ pool, port, logger });
  }
  const stub = await startBulkStub({ pool, port: 3000, logger });
  process.env['BULK_API_URL'] = stub.url;
  return stub;
}

main().catch((err: unknown) => {
  process.stderr.write(
    `[pipeline-run] FAILED: ${(err as Error).stack ?? String(err)}\n`,
  );
  process.exit(1);
});
