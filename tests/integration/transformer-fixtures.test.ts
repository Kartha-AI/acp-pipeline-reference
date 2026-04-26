import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

import { describe, expect, test } from 'vitest';

import { tpchCustomerTransformer } from '@acp/pipeline-tpch-customer/transformer';
import type { TpchCustomerSourceRow } from '@acp/pipeline-tpch-customer/transformer';
import type { StagingRow } from '@acp/pipeline-core';

interface FixtureFile {
  rows: Array<{ name: string; input: TpchCustomerSourceRow; expected: StagingRow }>;
}

const fixturePath = resolve(
  fileURLToPath(new URL('../fixtures/sample-rows.json', import.meta.url)),
);
const fixture = JSON.parse(readFileSync(fixturePath, 'utf8')) as FixtureFile;

describe('tpchCustomerTransformer fixture parity', () => {
  for (const row of fixture.rows) {
    test(`${row.name}`, () => {
      const out = tpchCustomerTransformer.toStaging(row.input);
      expect(out).toEqual(row.expected);
    });
  }
});
