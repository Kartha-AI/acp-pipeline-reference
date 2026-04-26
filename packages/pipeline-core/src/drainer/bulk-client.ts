import { setTimeout as delay } from 'node:timers/promises';

import { BulkApiError } from '../errors.js';
import type { Logger } from '../logger.js';
import { createPipelineLogger } from '../logger.js';
import type { StagingRow } from '../types.js';

/** Per-row outcome returned by /v1/objects/bulk. */
export type BulkRowOutcome = 'created' | 'updated' | 'unchanged' | 'rejected';

export interface BulkRowResult {
  index: number;
  outcome: BulkRowOutcome;
  object_id?: string;
  error?: string;
}

export interface BulkResponse {
  results: BulkRowResult[];
}

export interface BulkRequest {
  rows: StagingRow[];
}

export interface BulkClientConfig {
  /** Base URL of ACP API, e.g. `https://acp.example.com`. */
  apiUrl: string;
  /** OAuth bearer / API key. Sent as `Authorization: Bearer <token>`. */
  bearerToken?: string;
  /** Max attempts when /bulk responds 5xx or the network errors. Default 3. */
  maxRetries?: number;
  /** Initial backoff in ms; doubled per attempt. Default 200. */
  baseBackoffMs?: number;
  /** Per-request timeout. Default 60_000. */
  requestTimeoutMs?: number;
  logger?: Logger;
}

export interface BulkPostContext {
  pipelineName: string;
  batchId: string;
}

/**
 * Typed client for the ACP `/v1/objects/bulk` endpoint.
 *
 *   * 2xx → returns `{ results: [...] }` as-is. Caller decides per-row.
 *   * 5xx / network errors → exponential-backoff retry up to maxRetries.
 *   * 4xx → throws BulkApiError immediately. Caller inspects status/body.
 *
 * Uses the platform `fetch` (Node 20+); no HTTP framework dep needed.
 */
export class BulkClient {
  private readonly apiUrl: string;
  private readonly bearerToken: string | undefined;
  private readonly maxRetries: number;
  private readonly baseBackoffMs: number;
  private readonly requestTimeoutMs: number;
  private readonly logger: Logger;

  constructor(config: BulkClientConfig) {
    this.apiUrl = config.apiUrl.replace(/\/+$/, '');
    this.bearerToken = config.bearerToken;
    this.maxRetries = config.maxRetries ?? 3;
    this.baseBackoffMs = config.baseBackoffMs ?? 200;
    this.requestTimeoutMs = config.requestTimeoutMs ?? 60_000;
    this.logger = (config.logger ?? createPipelineLogger({})).child({
      component: 'bulk-client',
    });
  }

  async post(req: BulkRequest, ctx: BulkPostContext): Promise<BulkResponse> {
    const url = `${this.apiUrl}/v1/objects/bulk`;
    const body = JSON.stringify(req);
    const headers: Record<string, string> = {
      'content-type': 'application/json',
      accept: 'application/json',
    };
    if (this.bearerToken !== undefined) {
      headers['authorization'] = `Bearer ${this.bearerToken}`;
    }

    let attempt = 0;
    while (true) {
      attempt++;
      const requestStart = Date.now();
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), this.requestTimeoutMs);
      try {
        const res = await fetch(url, {
          method: 'POST',
          headers,
          body,
          signal: controller.signal,
        });
        const status = res.status;

        if (status >= 200 && status < 300) {
          const parsed = (await res.json()) as BulkResponse;
          if (!Array.isArray(parsed.results) || parsed.results.length !== req.rows.length) {
            throw new BulkApiError(
              `/bulk response did not include one result per row (got ${parsed.results?.length ?? 0}, expected ${req.rows.length})`,
              { statusCode: status, endpoint: url, body: parsed, ...ctx },
            );
          }
          this.logger.debug(
            { rowCount: req.rows.length, durationMs: Date.now() - requestStart, attempt },
            '/bulk OK',
          );
          return parsed;
        }

        const text = await res.text().catch(() => '');
        if (status >= 500 && attempt < this.maxRetries) {
          const wait = this.baseBackoffMs * 2 ** (attempt - 1);
          this.logger.warn(
            { statusCode: status, attempt, wait, body: text.slice(0, 200) },
            '/bulk 5xx, retrying',
          );
          await delay(wait);
          continue;
        }
        throw new BulkApiError(
          `/bulk responded with ${status}`,
          { statusCode: status, endpoint: url, body: text, ...ctx },
        );
      } catch (cause) {
        if (cause instanceof BulkApiError) throw cause;
        const isAbort =
          cause instanceof Error && (cause.name === 'AbortError' || cause.name === 'TimeoutError');
        if (attempt < this.maxRetries) {
          const wait = this.baseBackoffMs * 2 ** (attempt - 1);
          this.logger.warn(
            { err: { name: (cause as Error).name, message: (cause as Error).message }, attempt, wait, isAbort },
            '/bulk network error, retrying',
          );
          await delay(wait);
          continue;
        }
        throw new BulkApiError(
          `/bulk request failed after ${attempt} attempts`,
          { endpoint: url, ...ctx },
          { cause },
        );
      } finally {
        clearTimeout(timer);
      }
    }
  }
}
