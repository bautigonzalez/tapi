import { readFileSync } from 'node:fs';
import { App, Aspects } from 'aws-cdk-lib';
import { Annotations, Match, Template } from 'aws-cdk-lib/assertions';
import { AwsSolutionsChecks } from 'cdk-nag';
import { describe, expect, it } from 'vitest';
import { readConfig } from '../infra/lib/config';
import { ConsultasDiariasStack } from '../infra/lib/consultas-stack';

const cdkJson = JSON.parse(readFileSync('cdk.json', 'utf8')) as { context: Record<string, unknown> };
// Sin bundling: el test valida la infraestructura, no el código empaquetado.
const app = new App({ context: { ...cdkJson.context, 'aws:cdk:bundling-stacks': [] } });
const stack = new ConsultasDiariasStack(app, 'Test', {
  env: { account: '123456789012', region: 'us-east-1' },
  config: readConfig(app),
});
Aspects.of(stack).add(new AwsSolutionsChecks());
const template = Template.fromStack(stack);

type Statement = { Effect: string; Action: string | string[]; Resource: unknown; Condition?: unknown };
const asArray = <T>(v: T | T[]) => (Array.isArray(v) ? v : [v]);

const ALLOWED_ON_WILDCARD = new Set([
  'xray:PutTraceSegments',
  'xray:PutTelemetryRecords',
  'ec2:CreateNetworkInterface',
  'ec2:DescribeNetworkInterfaces',
  'ec2:DescribeSubnets',
  'ec2:DeleteNetworkInterface',
  'ec2:AssignPrivateIpAddresses',
  'ec2:UnassignPrivateIpAddresses',
]);

describe('infraestructura', () => {
  it('cdk-nag (AwsSolutions) no tiene hallazgos sin justificar', () => {
    const errors = Annotations.fromStack(stack).findError('*', Match.stringLikeRegexp('AwsSolutions-.*'));
    expect(errors.map((e) => e.entry.data)).toEqual([]);
  });

  it('"*" como recurso solo aparece con acciones de X-Ray y de ENI', () => {
    const offenders: string[] = [];
    for (const policy of Object.values(template.findResources('AWS::IAM::Policy'))) {
      for (const st of policy.Properties.PolicyDocument.Statement as Statement[]) {
        if (st.Effect !== 'Allow' || !asArray(st.Resource).includes('*')) continue;
        offenders.push(...asArray(st.Action).filter((a) => !ALLOWED_ON_WILDCARD.has(a)));
      }
    }
    expect(offenders).toEqual([]);
  });

  it('las Lambdas usan roles propios, sin políticas administradas', () => {
    const roles = template.findResources('AWS::IAM::Role', { Properties: { Description: Match.stringLikeRegexp('^Rol de ') } });
    expect(Object.keys(roles)).toHaveLength(3);
    for (const role of Object.values(roles)) expect(role.Properties.ManagedPolicyArns).toBeUndefined();
  });

  it('el código de la función no puede usar los permisos de ENI', () => {
    template.hasResourceProperties('AWS::IAM::Policy', {
      PolicyDocument: {
        Statement: Match.arrayWith([
          Match.objectLike({ Effect: 'Deny', Condition: { ArnEquals: { 'lambda:SourceFunctionArn': Match.anyValue() } } }),
        ]),
      },
    });
  });

  it('todas las colas están cifradas con la clave propia', () => {
    for (const queue of Object.values(template.findResources('AWS::SQS::Queue'))) {
      expect(queue.Properties.KmsMasterKeyId).toBeDefined();
    }
  });

  it('solo el Dispatcher puede encolar trabajos y solo el Worker puede consumirlos', () => {
    const jobsQueueId = Object.keys(template.findResources('AWS::SQS::Queue', { Properties: { QueueName: 'consultas-jobs-dev.fifo' } }))[0];
    const policies = Object.values(template.findResources('AWS::SQS::QueuePolicy')).filter((p) =>
      JSON.stringify(p.Properties.Queues).includes(String(jobsQueueId)),
    );
    const denies = policies.flatMap((p) => p.Properties.PolicyDocument.Statement as Statement[]).filter((s) => s.Effect === 'Deny' && s.Condition);
    const deniedActions = (action: string) => denies.filter((s) => asArray(s.Action).includes(action));
    expect(deniedActions('sqs:SendMessage')).toHaveLength(1);
    expect(deniedActions('sqs:ReceiveMessage')).toHaveLength(1);
    expect(JSON.stringify(deniedActions('sqs:SendMessage')[0]?.Condition)).toContain('DispatcherRole');
    expect(JSON.stringify(deniedActions('sqs:ReceiveMessage')[0]?.Condition)).toContain('WorkerRole');
  });

  it('el worker procesa de a un mensaje, con fallas parciales y concurrencia acotada', () => {
    template.hasResourceProperties('AWS::Lambda::EventSourceMapping', {
      BatchSize: 1,
      FunctionResponseTypes: ['ReportBatchItemFailures'],
      ScalingConfig: { MaximumConcurrency: 20 },
    });
  });

  it('nunca hay dos dispatchers a la vez y un tick demorado se descarta', () => {
    template.hasResourceProperties('AWS::Lambda::Function', { FunctionName: 'consultas-dispatcher-dev', ReservedConcurrentExecutions: 1 });
    template.hasResourceProperties('AWS::Lambda::EventInvokeConfig', { MaximumEventAgeInSeconds: 60, MaximumRetryAttempts: 0 });
  });

  it('la cola FIFO cumple la cadena de timeouts: visibilidad 240 s y DLQ a las 1.000 recepciones', () => {
    template.hasResourceProperties('AWS::SQS::Queue', {
      FifoQueue: true,
      VisibilityTimeout: 240,
      RedrivePolicy: Match.objectLike({ maxReceiveCount: 1_000 }),
    });
  });

  it('las claves KMS solo se usan a través de los servicios', () => {
    const kmsStatements: Statement[] = [];
    for (const policy of Object.values(template.findResources('AWS::IAM::Policy'))) {
      for (const st of policy.Properties.PolicyDocument.Statement as Statement[]) {
        if (asArray(st.Action).some((a) => a.startsWith('kms:'))) kmsStatements.push(st);
      }
    }
    expect(kmsStatements.length).toBeGreaterThan(0);
    for (const st of kmsStatements) expect(JSON.stringify(st.Condition)).toContain('kms:ViaService');
  });

  it('cada Lambda tiene alarma de heartbeat o de errores', () => {
    // 11 de negocio y colas + errores y throttles de las 3 Lambdas + 2 heartbeats.
    template.resourceCountIs('AWS::CloudWatch::Alarm', 19);
    template.hasResourceProperties('AWS::CloudWatch::Alarm', {
      AlarmDescription: 'El Dispatcher no corrió en 5 minutos',
      TreatMissingData: 'breaching',
      ComparisonOperator: 'LessThanThreshold',
    });
  });
});
