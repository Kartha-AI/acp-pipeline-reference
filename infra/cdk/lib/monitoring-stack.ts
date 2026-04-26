import { Duration, Stack } from 'aws-cdk-lib';
import type { StackProps } from 'aws-cdk-lib';
import { Alarm, ComparisonOperator, Dashboard, GraphWidget, Metric, TextWidget, TreatMissingData } from 'aws-cdk-lib/aws-cloudwatch';
import type { Construct } from 'constructs';

import type { PipelineStack } from './pipeline-stack.js';

export interface MonitoringStackProps extends StackProps {
  pipelineName: string;
  stagePrefix: string;
  pipelineStack: PipelineStack;
}

/**
 * CloudWatch dashboard + alarms for the pipeline. Sources metrics from the
 * resources created in PipelineStack — Lambda errors, queue depth, ECS
 * failures, Step Functions execution status.
 *
 * Custom application metrics (e.g., `acp.pipeline.RowsStaged`) are emitted
 * via CloudWatch EMF in pipeline-core's logger output and surface here as
 * named metric expressions; the namespace is `ACP/Pipeline/<pipelineName>`.
 */
export class MonitoringStack extends Stack {
  constructor(scope: Construct, id: string, props: MonitoringStackProps) {
    super(scope, id, props);

    const { pipelineStack: ps, pipelineName, stagePrefix } = props;
    const dashboard = new Dashboard(this, 'PipelineDashboard', {
      dashboardName: `${stagePrefix}-${pipelineName}`,
    });

    dashboard.addWidgets(
      new TextWidget({
        markdown: `# ACP Pipeline: ${pipelineName}\n\nLambda + ECS + Step Functions health.`,
        width: 24,
        height: 2,
      }),
    );

    const lambdas = [
      { name: 'trigger', fn: ps.triggerFn },
      { name: 'extractor', fn: ps.extractorFn },
      { name: 'promoter', fn: ps.promoterFn },
      { name: 'drainer', fn: ps.drainerFn },
      { name: 'validate', fn: ps.validateFn },
      { name: 'cleanup', fn: ps.cleanupFn },
    ];

    dashboard.addWidgets(
      new GraphWidget({
        title: 'Lambda invocations',
        width: 12,
        left: lambdas.map(({ name, fn }) =>
          new Metric({
            namespace: 'AWS/Lambda',
            metricName: 'Invocations',
            statistic: 'Sum',
            dimensionsMap: { FunctionName: fn.functionName },
            period: Duration.minutes(5),
            label: name,
          }),
        ),
      }),
      new GraphWidget({
        title: 'Lambda errors',
        width: 12,
        left: lambdas.map(({ name, fn }) =>
          new Metric({
            namespace: 'AWS/Lambda',
            metricName: 'Errors',
            statistic: 'Sum',
            dimensionsMap: { FunctionName: fn.functionName },
            period: Duration.minutes(5),
            label: name,
          }),
        ),
      }),
    );

    dashboard.addWidgets(
      new GraphWidget({
        title: 'Lambda duration p95',
        width: 12,
        left: lambdas.map(({ name, fn }) =>
          new Metric({
            namespace: 'AWS/Lambda',
            metricName: 'Duration',
            statistic: 'p95',
            dimensionsMap: { FunctionName: fn.functionName },
            period: Duration.minutes(5),
            label: name,
          }),
        ),
      }),
      new GraphWidget({
        title: 'Partition queue depth',
        width: 12,
        left: [
          new Metric({
            namespace: 'AWS/SQS',
            metricName: 'ApproximateNumberOfMessagesVisible',
            statistic: 'Maximum',
            dimensionsMap: { QueueName: ps.partitionQueue.queueName },
            period: Duration.minutes(1),
            label: 'visible',
          }),
          new Metric({
            namespace: 'AWS/SQS',
            metricName: 'ApproximateNumberOfMessagesVisible',
            statistic: 'Maximum',
            dimensionsMap: { QueueName: ps.partitionDlq.queueName },
            period: Duration.minutes(1),
            label: 'dlq',
          }),
        ],
      }),
    );

    dashboard.addWidgets(
      new GraphWidget({
        title: 'Step Functions executions',
        width: 12,
        left: [
          new Metric({
            namespace: 'AWS/States',
            metricName: 'ExecutionsSucceeded',
            statistic: 'Sum',
            dimensionsMap: { StateMachineArn: ps.initialStateMachine.stateMachineArn },
            period: Duration.minutes(5),
            label: 'initial succeeded',
          }),
          new Metric({
            namespace: 'AWS/States',
            metricName: 'ExecutionsFailed',
            statistic: 'Sum',
            dimensionsMap: { StateMachineArn: ps.initialStateMachine.stateMachineArn },
            period: Duration.minutes(5),
            label: 'initial failed',
          }),
          new Metric({
            namespace: 'AWS/States',
            metricName: 'ExecutionsSucceeded',
            statistic: 'Sum',
            dimensionsMap: { StateMachineArn: ps.incrementalStateMachine.stateMachineArn },
            period: Duration.minutes(5),
            label: 'incremental succeeded',
          }),
          new Metric({
            namespace: 'AWS/States',
            metricName: 'ExecutionsFailed',
            statistic: 'Sum',
            dimensionsMap: { StateMachineArn: ps.incrementalStateMachine.stateMachineArn },
            period: Duration.minutes(5),
            label: 'incremental failed',
          }),
        ],
      }),
      new GraphWidget({
        title: 'ECS extractor failures',
        width: 12,
        left: [
          new Metric({
            namespace: 'AWS/ECS',
            metricName: 'CPUUtilization',
            statistic: 'Maximum',
            dimensionsMap: {
              ClusterName: ps.cluster.clusterName,
              TaskDefinitionFamily: ps.extractorTaskDef.family,
            },
            period: Duration.minutes(5),
            label: 'CPU max',
          }),
        ],
      }),
    );

    // Alarms — keep the set small; each alarm should be actionable.
    new Alarm(this, 'PromoterErrors', {
      alarmName: `${stagePrefix}-${pipelineName}-promoter-errors`,
      metric: ps.promoterFn.metricErrors({ period: Duration.minutes(5) }),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
      alarmDescription: 'Promoter Lambda raised an error; pipeline run is stuck without manual intervention.',
    });

    new Alarm(this, 'DrainerErrorRate', {
      alarmName: `${stagePrefix}-${pipelineName}-drainer-errors`,
      metric: ps.drainerFn.metricErrors({ period: Duration.minutes(15) }),
      threshold: 3,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
      alarmDescription: '3+ drainer failures in 15 min — likely /bulk outage or auth.',
    });

    new Alarm(this, 'PartitionDlqDepth', {
      alarmName: `${stagePrefix}-${pipelineName}-partition-dlq`,
      metric: new Metric({
        namespace: 'AWS/SQS',
        metricName: 'ApproximateNumberOfMessagesVisible',
        statistic: 'Maximum',
        dimensionsMap: { QueueName: ps.partitionDlq.queueName },
        period: Duration.minutes(5),
      }),
      threshold: 1,
      evaluationPeriods: 2,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
      alarmDescription: 'Partitions are landing in DLQ — extractor is failing repeatedly.',
    });

    new Alarm(this, 'InitialMachineFailed', {
      alarmName: `${stagePrefix}-${pipelineName}-initial-sfn-failed`,
      metric: new Metric({
        namespace: 'AWS/States',
        metricName: 'ExecutionsFailed',
        statistic: 'Sum',
        dimensionsMap: { StateMachineArn: ps.initialStateMachine.stateMachineArn },
        period: Duration.minutes(5),
      }),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
      alarmDescription: 'Initial-mode state machine execution failed.',
    });

    new Alarm(this, 'IncrementalMachineFailed', {
      alarmName: `${stagePrefix}-${pipelineName}-incremental-sfn-failed`,
      metric: new Metric({
        namespace: 'AWS/States',
        metricName: 'ExecutionsFailed',
        statistic: 'Sum',
        dimensionsMap: { StateMachineArn: ps.incrementalStateMachine.stateMachineArn },
        period: Duration.minutes(5),
      }),
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING,
      alarmDescription: 'Incremental-mode state machine execution failed.',
    });
  }
}
