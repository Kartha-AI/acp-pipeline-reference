import { Stack } from 'aws-cdk-lib';
import type { StackProps } from 'aws-cdk-lib';
import { Peer, Port, SecurityGroup, Vpc } from 'aws-cdk-lib/aws-ec2';
import type { IVpc } from 'aws-cdk-lib/aws-ec2';
import type { Construct } from 'constructs';

export interface NetworkStackProps extends StackProps {
  /** ID of the existing VPC (e.g. `vpc-0abc...`). */
  vpcId: string;
  /** Private subnets with NAT egress (for Lambda + ECS). */
  privateSubnetIds: string[];
  /** Availability zones matching the subnet order. */
  availabilityZones: string[];
}

/**
 * Imports an existing VPC and provisions security groups for:
 *   * Lambda functions reaching RDS (and the ACP API over HTTPS egress)
 *   * ECS tasks reaching RDS and external sources (Snowflake, source PG)
 *
 * Does NOT create the VPC, RDS, NAT Gateways, or any expensive infrastructure
 * — those are assumed to live in the customer's existing landing zone.
 */
export class NetworkStack extends Stack {
  readonly vpc: IVpc;
  readonly lambdaSecurityGroup: SecurityGroup;
  readonly ecsSecurityGroup: SecurityGroup;

  constructor(scope: Construct, id: string, props: NetworkStackProps) {
    super(scope, id, props);

    // Static attribute import avoids the synth-time AWS lookup so CDK can
    // synth offline. Customers who prefer dynamic discovery can swap this for
    // `Vpc.fromLookup({ vpcId })` once AWS credentials are configured.
    this.vpc = Vpc.fromVpcAttributes(this, 'AcpVpc', {
      vpcId: props.vpcId,
      availabilityZones: props.availabilityZones,
      privateSubnetIds: props.privateSubnetIds,
    });

    this.lambdaSecurityGroup = new SecurityGroup(this, 'LambdaSg', {
      vpc: this.vpc,
      description: 'Egress for ACP pipeline Lambda functions (RDS + ACP API)',
      allowAllOutbound: true,
    });

    this.ecsSecurityGroup = new SecurityGroup(this, 'EcsSg', {
      vpc: this.vpc,
      description: 'ACP pipeline extractor ECS tasks',
      allowAllOutbound: true,
    });

    // Optional: Postgres-on-RDS often lives behind a separately-managed SG.
    // Customers wire ingress on that SG to allow these two SGs:
    //
    //   rdsSg.addIngressRule(Peer.securityGroupId(lambdaSecurityGroup.id), Port.tcp(5432));
    //   rdsSg.addIngressRule(Peer.securityGroupId(ecsSecurityGroup.id),    Port.tcp(5432));
    //
    // We don't manage the RDS SG here because that's owned by the platform team.

    // Self-ref ingress so multiple ECS tasks of the same definition can talk
    // to each other if needed (e.g., shared cache; not used today but
    // commonly required as soon as you add one).
    this.ecsSecurityGroup.addIngressRule(
      Peer.securityGroupId(this.ecsSecurityGroup.securityGroupId),
      Port.allTraffic(),
      'self',
    );
  }
}
