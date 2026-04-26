# Deploying

First-time AWS deployment of one pipeline. Subsequent deploys are
`cdk deploy` from this directory.

## Prerequisites

| Item | Notes |
| --- | --- |
| AWS CLI configured | `aws sts get-caller-identity` returns the target account. |
| CDK bootstrapped in target account/region | `cdk bootstrap aws://<account>/<region>` once per region. |
| ACP platform deployed and reachable | Pipeline writes into ACP's `context_objects` and `change_log`; ensure those tables exist in the target RDS. |
| VPC with private subnets that have NAT egress | Lambda functions and ECS tasks need outbound HTTPS to AWS APIs and the ACP API. |
| Three Secrets Manager secrets | See **Secrets**, below. |

## Secrets

The CDK stack expects three secrets to exist. It does **not** create them —
that's the platform team's responsibility.

| Context key | Secret payload |
| --- | --- |
| `acpPostgresSecretArn` | JSON: `{ "host", "port", "username", "password", "dbname" }`. The pipeline DB role needs SELECT/INSERT/UPDATE on `acp_staging.*`, EXECUTE on the migration functions, and SELECT/INSERT on `pipeline_runs`/`acp_rejected_rows`. |
| `sourcePostgresSecretArn` | Same JSON shape. SELECT-only role on the source schema. |
| `acpBulkApiTokenSecretArn` | SecretString *is* the bearer token (no JSON wrapping). |

Create them once per environment:

```bash
aws secretsmanager create-secret --name acp/postgres \
  --secret-string '{"host":"...","port":5432,"username":"acp_pipeline","password":"...","dbname":"acp"}'
aws secretsmanager create-secret --name acp/source/tpch \
  --secret-string '{"host":"...","port":5432,"username":"reader","password":"...","dbname":"tpch"}'
aws secretsmanager create-secret --name acp/bulk-api-token --secret-string "$(cat token.txt)"
```

Capture the resulting ARNs — `cdk.json` references them.

## Configure CDK context

Edit `infra/cdk/cdk.json` (or pass `-c key=value` on the command line):

```json
{
  "context": {
    "pipelineName": "tpch_customer",
    "stagePrefix": "acp",
    "vpcId": "vpc-0abc...",
    "privateSubnetIds": "subnet-1,subnet-2,subnet-3",
    "availabilityZones": "us-east-1a,us-east-1b,us-east-1c",
    "acpPostgresSecretArn": "arn:aws:secretsmanager:...:secret:acp/postgres-AbCdEf",
    "sourcePostgresSecretArn": "arn:aws:secretsmanager:...:secret:acp/source/tpch-GhIjKl",
    "acpBulkApiUrl": "https://acp.example.com",
    "acpBulkApiTokenSecretArn": "arn:aws:secretsmanager:...:secret:acp/bulk-api-token-MnOpQr",
    "incrementalScheduleCron": "cron(0 */6 * * ? *)",
    "logRetentionDays": 30
  }
}
```

`pipelineName` must match the `name` field in `pipelines/<dir>/pipeline.config.ts`
(Postgres identifier — letters, digits, underscore). The deployed stack
names use kebab-case for AWS compatibility; underscores in `pipelineName`
get translated automatically.

## Apply database migrations

The pipeline migrations target ACP's RDS instance, not the staging
instance. Apply once per environment, in order:

```bash
psql "$ACP_PG_URL" -f migrations/001_staging_template.sql
psql "$ACP_PG_URL" -f migrations/002_promote_function.sql
psql "$ACP_PG_URL" -f migrations/003_helper_functions.sql
psql "$ACP_PG_URL" -f migrations/004_dead_letter.sql
psql "$ACP_PG_URL" -f migrations/005_pipeline_runs.sql
```

Do **not** apply `local-dev/00_acp_stub.sql` against production — those
tables are owned by the ACP platform.

## Deploy

From the repo root:

```bash
pnpm install
pnpm -r build

cd infra/cdk
pnpm exec cdk diff   # review what's about to be created
pnpm exec cdk deploy --all
```

Expected stacks (using `stagePrefix=acp` and `pipelineName=tpch_customer`):

```
✅  acp-network-tpch-customer
✅  acp-pipeline-tpch-customer
✅  acp-monitoring-tpch-customer
```

## Verify

```bash
# Lambdas exist:
aws lambda list-functions --query 'Functions[?starts_with(FunctionName, `acp-pipeline-tpch-customer-`)].FunctionName'

# State machine is ready:
aws stepfunctions list-state-machines --query 'stateMachines[?contains(name, `acp-tpch-customer`)]'

# Smoke test: invoke the trigger directly to enumerate partitions
aws lambda invoke --function-name acp-pipeline-tpch-customer-TriggerFn... \
  --payload '{"pipelineName":"tpch_customer","mode":"incremental"}' \
  /tmp/trigger.out && cat /tmp/trigger.out
```

The trigger payload should return `{ batchId, runId, partitions: [...] }`.
A `partitions: []` is fine if there are no source changes since the last
successful run.

## Common deploy errors

| Symptom | Cause | Fix |
| --- | --- | --- |
| `secretCompleteArn does not appear to be complete` | ARN missing the trailing 6-char suffix Secrets Manager appends. | Use the full ARN; `aws secretsmanager describe-secret --secret-id <name>` will give it. |
| `CDK context "vpcId" is required` | `cdk.json` still has `REPLACE-ME` placeholders. | Set actual values or pass `-c` flags. |
| `User: ... is not authorized to perform: ecr:GetAuthorizationToken` during ECS image asset push | CDK bootstrap is missing or stale. | `cdk bootstrap aws://<account>/<region>`. |
| Lambda `Init failed: Cannot find module 'pg'` | Native module not installed at deploy time. | Confirm `infra/cdk/package.json` lists `pg`, `pg-cursor`, etc., and that `nodeModules` in `pipeline-stack.ts` includes them. |
| Step Functions Map state failing immediately | `Subnets` or `SecurityGroups` substitution empty. | Confirm `privateSubnetIds` is set in `cdk.json` and resolves to non-empty IDs. |

## Rollback

CDK rollbacks remove stack resources. Two things to know:

* **Volumes survive** — `acp_staging.<pipeline>` table is in RDS, which CDK
  doesn't manage. Rolling back the CDK stack does not drop staging.
* **EventBridge schedule disables itself** when the state machine is
  removed. No further runs trigger. Existing runs continue to completion.
