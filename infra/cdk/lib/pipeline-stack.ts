import { fileURLToPath } from 'node:url';
import { dirname, resolve as resolvePath } from 'node:path';

import { Duration, Fn, RemovalPolicy, Stack } from 'aws-cdk-lib';
import type { StackProps } from 'aws-cdk-lib';
import { RuleTargetInput, Schedule } from 'aws-cdk-lib/aws-events';
import { Rule } from 'aws-cdk-lib/aws-events';
import { SfnStateMachine } from 'aws-cdk-lib/aws-events-targets';
import { Cluster, ContainerImage, FargateTaskDefinition, LogDriver } from 'aws-cdk-lib/aws-ecs';
import type { ICluster } from 'aws-cdk-lib/aws-ecs';
import type { IVpc, ISecurityGroup, SubnetSelection } from 'aws-cdk-lib/aws-ec2';
import { SubnetType } from 'aws-cdk-lib/aws-ec2';
import { Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { Architecture, Runtime } from 'aws-cdk-lib/aws-lambda';
import type { IFunction } from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction, OutputFormat } from 'aws-cdk-lib/aws-lambda-nodejs';
import { LogGroup, RetentionDays } from 'aws-cdk-lib/aws-logs';
import { Secret } from 'aws-cdk-lib/aws-secretsmanager';
import type { ISecret } from 'aws-cdk-lib/aws-secretsmanager';
import { Queue, QueueEncryption } from 'aws-cdk-lib/aws-sqs';
import { DefinitionBody, StateMachine } from 'aws-cdk-lib/aws-stepfunctions';
import type { Construct } from 'constructs';

import { readFileSync } from 'node:fs';

export interface PipelineStackProps extends StackProps {
  pipelineName: string;
  stagePrefix: string;
  vpc: IVpc;
  lambdaSecurityGroup: ISecurityGroup;
  ecsSecurityGroup: ISecurityGroup;
  acpPostgresSecretArn: string;
  sourcePostgresSecretArn: string;
  acpBulkApiUrl: string;
  acpBulkApiTokenSecretArn: string;
  logRetentionDays: number;
  incrementalScheduleCron: string;
}

/**
 * Lambdas + ECS task + SQS + Step Functions for one pipeline. Wired so the
 * same handler code that runs locally via `pnpm pipeline:run` runs in
 * production via Step Functions invocations.
 *
 * Lambda bundles are produced by NodejsFunction (esbuild). Native deps that
 * misbehave when bundled (pg, pg-cursor, pino, etc.) are kept external and
 * installed at deploy time via `nodeModules`.
 */
export class PipelineStack extends Stack {
  readonly partitionQueue: Queue;
  readonly partitionDlq: Queue;
  readonly triggerFn: IFunction;
  readonly extractorFn: IFunction;
  readonly promoterFn: IFunction;
  readonly drainerFn: IFunction;
  readonly validateFn: IFunction;
  readonly cleanupFn: IFunction;
  readonly cluster: ICluster;
  readonly extractorTaskDef: FargateTaskDefinition;
  readonly initialStateMachine: StateMachine;
  readonly incrementalStateMachine: StateMachine;

  constructor(scope: Construct, id: string, props: PipelineStackProps) {
    super(scope, id, props);

    const acpSecret = Secret.fromSecretCompleteArn(
      this,
      'AcpPostgresSecret',
      props.acpPostgresSecretArn,
    );
    const sourceSecret = Secret.fromSecretCompleteArn(
      this,
      'SourcePostgresSecret',
      props.sourcePostgresSecretArn,
    );
    const bulkTokenSecret = Secret.fromSecretCompleteArn(
      this,
      'BulkApiTokenSecret',
      props.acpBulkApiTokenSecretArn,
    );

    // SQS for partition fan-out.
    this.partitionDlq = new Queue(this, 'PartitionDlq', {
      queueName: `${props.stagePrefix}-${props.pipelineName}-partitions-dlq`,
      retentionPeriod: Duration.days(14),
      encryption: QueueEncryption.SQS_MANAGED,
    });
    this.partitionQueue = new Queue(this, 'PartitionQueue', {
      queueName: `${props.stagePrefix}-${props.pipelineName}-partitions`,
      visibilityTimeout: Duration.minutes(15),
      retentionPeriod: Duration.days(4),
      encryption: QueueEncryption.SQS_MANAGED,
      deadLetterQueue: { queue: this.partitionDlq, maxReceiveCount: 3 },
    });

    const baseEnv: Record<string, string> = {
      ACP_POSTGRES_SECRET: props.acpPostgresSecretArn,
      SOURCE_POSTGRES_SECRET: props.sourcePostgresSecretArn,
      SQS_URL: this.partitionQueue.queueUrl,
      LOG_LEVEL: 'info',
      NODE_OPTIONS: '--enable-source-maps',
    };

    const drainerEnv: Record<string, string> = {
      ...baseEnv,
      ACP_BULK_API_URL: props.acpBulkApiUrl,
      ACP_BULK_API_TOKEN_SECRET: props.acpBulkApiTokenSecretArn,
    };

    const cdkRoot = dirname(fileURLToPath(import.meta.url));
    const lambdaDir = resolvePath(cdkRoot, '..', 'lambda');

    this.triggerFn = this.createLambda('TriggerFn', {
      entry: resolvePath(lambdaDir, 'trigger.ts'),
      props,
      env: baseEnv,
    });
    this.extractorFn = this.createLambda('ExtractorFn', {
      entry: resolvePath(lambdaDir, 'extractor.ts'),
      props,
      env: baseEnv,
      memoryMB: 1024,
      timeout: Duration.minutes(15),
    });
    this.promoterFn = this.createLambda('PromoterFn', {
      entry: resolvePath(lambdaDir, 'promoter.ts'),
      props,
      env: baseEnv,
      timeout: Duration.minutes(15),
    });
    this.drainerFn = this.createLambda('DrainerFn', {
      entry: resolvePath(lambdaDir, 'drainer.ts'),
      props,
      env: drainerEnv,
      memoryMB: 1024,
      timeout: Duration.minutes(15),
      reservedConcurrent: 4,
    });
    this.validateFn = this.createLambda('ValidateFn', {
      entry: resolvePath(lambdaDir, 'validate.ts'),
      props,
      env: baseEnv,
    });
    this.cleanupFn = this.createLambda('CleanupFn', {
      entry: resolvePath(lambdaDir, 'cleanup.ts'),
      props,
      env: baseEnv,
    });

    // Permissions: every Lambda reaches the ACP secret + DB; the trigger
    // additionally writes SQS; the drainer additionally reads the bulk-token
    // secret.
    for (const fn of [
      this.triggerFn,
      this.extractorFn,
      this.promoterFn,
      this.drainerFn,
      this.validateFn,
      this.cleanupFn,
    ]) {
      acpSecret.grantRead(fn);
    }
    sourceSecret.grantRead(this.triggerFn);
    sourceSecret.grantRead(this.extractorFn);
    this.partitionQueue.grantSendMessages(this.triggerFn);
    bulkTokenSecret.grantRead(this.drainerFn);

    // Promoter is least-privilege: only execute the promote function. No SQS,
    // no source secret, no /bulk credentials.
    // (Implicit via shared ACP secret read above — additional restrictions
    // would happen at the Postgres role level, not in IAM.)

    // ECS cluster + task definition for the initial-mode extractor.
    this.cluster = new Cluster(this, 'AcpCluster', {
      vpc: props.vpc,
      clusterName: `${props.stagePrefix}-${props.pipelineName}`,
      enableFargateCapacityProviders: true,
    });

    this.extractorTaskDef = new FargateTaskDefinition(this, 'ExtractorTaskDef', {
      // CLAUDE.md: ECS task is configured with 1GB memory.
      memoryLimitMiB: 1024,
      cpu: 512,
    });
    const containerLogGroup = new LogGroup(this, 'ExtractorLogs', {
      logGroupName: `/aws/ecs/${props.stagePrefix}-${props.pipelineName}-extractor`,
      retention: this.toRetention(props.logRetentionDays),
      removalPolicy: RemovalPolicy.DESTROY,
    });
    this.extractorTaskDef.addContainer('extractor', {
      containerName: 'extractor',
      image: ContainerImage.fromAsset(resolvePath(cdkRoot, '..'), {
        file: 'ecs/Dockerfile',
      }),
      memoryLimitMiB: 1024,
      environment: {
        ACP_POSTGRES_SECRET: props.acpPostgresSecretArn,
        SOURCE_POSTGRES_SECRET: props.sourcePostgresSecretArn,
        LOG_LEVEL: 'info',
      },
      logging: LogDriver.awsLogs({
        streamPrefix: 'extractor',
        logGroup: containerLogGroup,
      }),
    });
    acpSecret.grantRead(this.extractorTaskDef.taskRole);
    sourceSecret.grantRead(this.extractorTaskDef.taskRole);

    const subnets: SubnetSelection = props.vpc.selectSubnets({
      subnetType: SubnetType.PRIVATE_WITH_EGRESS,
    });

    // State machines. ASL JSON files use ${VarName} placeholders that we
    // substitute at synth time via Fn.sub.
    const aslDir = resolvePath(cdkRoot, '..', '..', 'stepfunctions');
    const subs: Record<string, string> = {
      TriggerLambdaArn: this.triggerFn.functionArn,
      ExtractorLambdaArn: this.extractorFn.functionArn,
      PromoterLambdaArn: this.promoterFn.functionArn,
      DrainerLambdaArn: this.drainerFn.functionArn,
      ValidateLambdaArn: this.validateFn.functionArn,
      CleanupLambdaArn: this.cleanupFn.functionArn,
      EcsClusterArn: this.cluster.clusterArn,
      ExtractorTaskDefArn: this.extractorTaskDef.taskDefinitionArn,
      Subnets: subnets.subnets?.map((s) => s.subnetId).join(',') ?? '',
      SecurityGroups: props.ecsSecurityGroup.securityGroupId,
      PipelineName: props.pipelineName,
      DrainerConcurrency: '4',
    };

    this.initialStateMachine = new StateMachine(this, 'InitialStateMachine', {
      stateMachineName: `${props.stagePrefix}-${props.pipelineName}-initial`,
      definitionBody: DefinitionBody.fromString(
        Fn.sub(readFileSync(resolvePath(aslDir, 'initial-load.asl.json'), 'utf8'), subs),
      ),
      timeout: Duration.hours(12),
    });

    this.incrementalStateMachine = new StateMachine(this, 'IncrementalStateMachine', {
      stateMachineName: `${props.stagePrefix}-${props.pipelineName}-incremental`,
      definitionBody: DefinitionBody.fromString(
        Fn.sub(readFileSync(resolvePath(aslDir, 'incremental-sync.asl.json'), 'utf8'), subs),
      ),
      timeout: Duration.hours(2),
    });

    // Step Functions needs permission to invoke the Lambdas it calls and to
    // run the ECS task. CDK doesn't auto-wire these for fromString
    // definitions, so we grant explicitly.
    for (const fn of [
      this.triggerFn,
      this.extractorFn,
      this.promoterFn,
      this.drainerFn,
      this.validateFn,
      this.cleanupFn,
    ]) {
      fn.grantInvoke(this.initialStateMachine);
      fn.grantInvoke(this.incrementalStateMachine);
    }

    // Allow Step Functions to RunTask and pass the task role + execution role.
    const runTaskPolicy = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['ecs:RunTask', 'ecs:StopTask', 'ecs:DescribeTasks'],
      resources: ['*'],
    });
    const passRolePolicy = new PolicyStatement({
      effect: Effect.ALLOW,
      actions: ['iam:PassRole'],
      resources: [
        this.extractorTaskDef.taskRole.roleArn,
        this.extractorTaskDef.executionRole?.roleArn ?? '*',
      ],
    });
    this.initialStateMachine.addToRolePolicy(runTaskPolicy);
    this.initialStateMachine.addToRolePolicy(passRolePolicy);
    // Step Functions uses an EventBridge-managed rule for sync ECS RunTask;
    // it needs to be allowed to put events into the SFN execution.
    this.initialStateMachine.addToRolePolicy(
      new PolicyStatement({
        effect: Effect.ALLOW,
        actions: ['events:PutTargets', 'events:PutRule', 'events:DescribeRule'],
        resources: ['*'],
      }),
    );

    // Schedule incremental sync.
    new Rule(this, 'IncrementalSchedule', {
      ruleName: `${props.stagePrefix}-${props.pipelineName}-incremental`,
      schedule: Schedule.expression(props.incrementalScheduleCron),
      targets: [
        new SfnStateMachine(this.incrementalStateMachine, {
          // The state machine reads `since` from $.since (optional). Empty
          // input lets the trigger Lambda fall back to pipeline_runs.
          input: RuleTargetInput.fromObject({}),
        }),
      ],
    });
  }

  private createLambda(
    constructId: string,
    args: {
      entry: string;
      props: PipelineStackProps;
      env: Record<string, string>;
      memoryMB?: number;
      timeout?: Duration;
      reservedConcurrent?: number;
    },
  ): NodejsFunction {
    // Pre-create the log group so the Lambda's IAM role can be scoped to it
    // and we can apply explicit retention without using the deprecated
    // FunctionOptions#logRetention shim.
    const logGroup = new LogGroup(this, `${constructId}Logs`, {
      retention: this.toRetention(args.props.logRetentionDays),
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const fn = new NodejsFunction(this, constructId, {
      entry: args.entry,
      handler: 'handler',
      runtime: Runtime.NODEJS_20_X,
      architecture: Architecture.X86_64,
      memorySize: args.memoryMB ?? 512,
      timeout: args.timeout ?? Duration.minutes(5),
      vpc: args.props.vpc,
      vpcSubnets: { subnetType: SubnetType.PRIVATE_WITH_EGRESS },
      securityGroups: [args.props.lambdaSecurityGroup],
      environment: args.env,
      logGroup,
      bundling: {
        format: OutputFormat.ESM,
        target: 'node20',
        sourceMap: true,
        // Native + thread-based deps: ship from node_modules instead of
        // bundling via esbuild. esbuild's bundling of `pino` with thread
        // workers and `pg`'s native bindings produces broken output otherwise.
        nodeModules: [
          'pg',
          'pg-cursor',
          'pg-copy-streams',
          'pino',
          'thread-stream',
          'uuid',
          '@aws-sdk/client-secrets-manager',
          '@aws-sdk/client-sqs',
          '@aws-sdk/rds-signer',
        ],
      },
      ...(args.reservedConcurrent !== undefined
        ? { reservedConcurrentExecutions: args.reservedConcurrent }
        : {}),
    });
    return fn;
  }

  private toRetention(days: number): RetentionDays {
    // Map common values to enum members; default to ONE_MONTH.
    switch (days) {
      case 1: return RetentionDays.ONE_DAY;
      case 7: return RetentionDays.ONE_WEEK;
      case 14: return RetentionDays.TWO_WEEKS;
      case 30: return RetentionDays.ONE_MONTH;
      case 60: return RetentionDays.TWO_MONTHS;
      case 90: return RetentionDays.THREE_MONTHS;
      case 180: return RetentionDays.SIX_MONTHS;
      case 365: return RetentionDays.ONE_YEAR;
      default: return RetentionDays.ONE_MONTH;
    }
  }
}
