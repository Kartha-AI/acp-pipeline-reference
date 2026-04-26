#!/usr/bin/env node
import 'source-map-support/register.js';

import { App } from 'aws-cdk-lib';

import { MonitoringStack } from '../lib/monitoring-stack.js';
import { NetworkStack } from '../lib/network-stack.js';
import { PipelineStack } from '../lib/pipeline-stack.js';

const app = new App();

const env = {
  account: process.env['CDK_DEFAULT_ACCOUNT'] ?? process.env['AWS_ACCOUNT_ID'],
  region: process.env['CDK_DEFAULT_REGION'] ?? process.env['AWS_REGION'] ?? 'us-east-1',
};

const pipelineName = ctxString(app, 'pipelineName');
const stagePrefix = ctxString(app, 'stagePrefix');
const vpcId = ctxString(app, 'vpcId');
const privateSubnetIds = ctxString(app, 'privateSubnetIds').split(',');
const availabilityZones = ctxString(app, 'availabilityZones').split(',');
const acpPostgresSecretArn = ctxString(app, 'acpPostgresSecretArn');
const sourcePostgresSecretArn = ctxString(app, 'sourcePostgresSecretArn');
const acpBulkApiUrl = ctxString(app, 'acpBulkApiUrl');
const acpBulkApiTokenSecretArn = ctxString(app, 'acpBulkApiTokenSecretArn');
const incrementalScheduleCron = ctxString(app, 'incrementalScheduleCron');
const logRetentionDays = Number(app.node.tryGetContext('logRetentionDays') ?? 30);

const network = new NetworkStack(app, `${stagePrefix}-network-${stackSlug(pipelineName)}`, {
  env,
  vpcId,
  privateSubnetIds,
  availabilityZones,
});

const pipeline = new PipelineStack(app, `${stagePrefix}-pipeline-${stackSlug(pipelineName)}`, {
  env,
  pipelineName,
  stagePrefix,
  vpc: network.vpc,
  lambdaSecurityGroup: network.lambdaSecurityGroup,
  ecsSecurityGroup: network.ecsSecurityGroup,
  acpPostgresSecretArn,
  sourcePostgresSecretArn,
  acpBulkApiUrl,
  acpBulkApiTokenSecretArn,
  logRetentionDays,
  incrementalScheduleCron,
});
pipeline.addDependency(network);

const monitoring = new MonitoringStack(app, `${stagePrefix}-monitoring-${stackSlug(pipelineName)}`, {
  env,
  pipelineName,
  stagePrefix,
  pipelineStack: pipeline,
});
monitoring.addDependency(pipeline);

/** Stack names disallow underscores; pipeline names use them as SQL idents. */
function stackSlug(name: string): string {
  return name.replace(/_/g, '-');
}

function ctxString(a: App, key: string): string {
  const value = a.node.tryGetContext(key);
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`CDK context "${key}" is required (set in cdk.json or via -c ${key}=...)`);
  }
  return value;
}
