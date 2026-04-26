import { createEcsExtractorEntrypoint } from '@acp/pipeline-core';
import pipeline from '@acp/pipeline-tpch-customer';

const main = createEcsExtractorEntrypoint(pipeline);

main().catch((err: unknown) => {
  // eslint-disable-next-line no-console -- this runs as the container PID 1
  process.stderr.write(
    `[ecs-extractor] FAILED: ${(err as Error).stack ?? String(err)}\n`,
  );
  process.exit(1);
});
