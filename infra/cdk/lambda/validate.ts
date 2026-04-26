import { createValidateHandler } from '@acp/pipeline-core';
import pipeline from '@acp/pipeline-tpch-customer';

export const handler = createValidateHandler(pipeline);
