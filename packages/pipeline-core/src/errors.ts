export interface BaseErrorContext {
  pipelineName?: string;
  batchId?: string;
  partitionId?: string;
}

export interface ConnectorErrorContext extends BaseErrorContext {
  source?: string;
  operation?: string;
}

export interface TransformErrorContext extends BaseErrorContext {
  sourceRef?: string;
  rowSnapshot?: Record<string, unknown>;
}

export interface StagingErrorContext extends BaseErrorContext {
  table?: string;
  rowsAttempted?: number;
}

export interface PromoteErrorContext extends BaseErrorContext {
  table?: string;
}

export interface BulkApiErrorContext extends BaseErrorContext {
  statusCode?: number;
  endpoint?: string;
  body?: unknown;
}

export type PipelineErrorCode =
  | 'CONNECTOR_ERROR'
  | 'TRANSFORM_ERROR'
  | 'STAGING_ERROR'
  | 'PROMOTE_ERROR'
  | 'BULK_API_ERROR';

export interface SerializedPipelineError {
  name: string;
  code: PipelineErrorCode;
  message: string;
  context: BaseErrorContext;
  cause?: unknown;
}

export abstract class PipelineError<
  TContext extends BaseErrorContext = BaseErrorContext,
> extends Error {
  abstract readonly code: PipelineErrorCode;
  readonly context: Readonly<TContext>;

  constructor(message: string, context: TContext, options?: ErrorOptions) {
    super(message, options);
    this.name = new.target.name;
    this.context = Object.freeze({ ...context });
  }

  toJSON(): SerializedPipelineError {
    return {
      name: this.name,
      code: this.code,
      message: this.message,
      context: this.context,
      cause: this.cause,
    };
  }
}

/** Source-system failure: connection refused, query error, auth, etc. */
export class ConnectorError extends PipelineError<ConnectorErrorContext> {
  readonly code = 'CONNECTOR_ERROR';
}

/** Source row could not be mapped to a StagingRow (missing required fields, type mismatch). */
export class TransformError extends PipelineError<TransformErrorContext> {
  readonly code = 'TRANSFORM_ERROR';
}

/** COPY into staging table failed (constraint violation, network, disk). */
export class StagingError extends PipelineError<StagingErrorContext> {
  readonly code = 'STAGING_ERROR';
}

/** acp_promote_staging_initial() raised an exception or returned an unexpected shape. */
export class PromoteError extends PipelineError<PromoteErrorContext> {
  readonly code = 'PROMOTE_ERROR';
}

/** /v1/objects/bulk returned a non-2xx status or partial-success body indicating fatal errors. */
export class BulkApiError extends PipelineError<BulkApiErrorContext> {
  readonly code = 'BULK_API_ERROR';
}
