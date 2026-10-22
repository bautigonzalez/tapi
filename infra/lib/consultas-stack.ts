import * as path from 'node:path';
import { Annotations, CfnOutput, Duration, RemovalPolicy, Stack, type StackProps } from 'aws-cdk-lib';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as cwActions from 'aws-cdk-lib/aws-cloudwatch-actions';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as nodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as scheduler from 'aws-cdk-lib/aws-scheduler';
import * as targets from 'aws-cdk-lib/aws-scheduler-targets';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subscriptions from 'aws-cdk-lib/aws-sns-subscriptions';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { NagSuppressions } from 'cdk-nag';
import type { Construct } from 'constructs';
import type { StackConfig } from './config';

const ROOT = path.join(__dirname, '..', '..');
const NAMESPACE = 'ConsultasDiarias';
/** El runtime de Lambda trae el bundle de CA de RDS: no hay que descargarlo ni empaquetarlo. */
const RUNTIME_CA_BUNDLE = '/var/runtime/ca-cert.pem';
/** Acciones de ENI que necesita una Lambda en VPC (las usa el servicio, no el código). */
const ENI_ACTIONS = [
  'ec2:CreateNetworkInterface',
  'ec2:DescribeNetworkInterfaces',
  'ec2:DescribeSubnets',
  'ec2:DeleteNetworkInterface',
  'ec2:AssignPrivateIpAddresses',
  'ec2:UnassignPrivateIpAddresses',
];

export interface ConsultasDiariasStackProps extends StackProps {
  config: StackConfig;
}

/**
 * Infraestructura de docs/ARQUITECTURA.md §2: Scheduler → Dispatcher → SQS FIFO → Worker → API interna,
 * con DynamoDB para cursores, ledger y resultados. Todo cifrado con una clave KMS propia.
 */
export class ConsultasDiariasStack extends Stack {
  constructor(scope: Construct, id: string, props: ConsultasDiariasStackProps) {
    super(scope, id, props);
    const { config } = props;
    const name = (suffix: string) => `consultas-${suffix}-${config.stage}`;
    const prod = config.stage === 'prod';

    // ---------- Red: VPC existente, sin salida a internet ----------
    const vpc = ec2.Vpc.fromVpcAttributes(this, 'Vpc', {
      vpcId: config.vpcId,
      vpcCidrBlock: config.vpcCidr,
      availabilityZones: config.availabilityZones,
      privateSubnetIds: config.privateSubnetIds,
    });
    const lambdaSg = new ec2.SecurityGroup(this, 'LambdaSg', {
      vpc,
      description: 'Lambdas de consultas diarias: sin entrada; salida solo a la base y a endpoints',
      allowAllOutbound: false,
    });
    const dbSg = ec2.SecurityGroup.fromSecurityGroupId(this, 'DbSg', config.dbSecurityGroupId, { mutable: config.manageDbIngress });
    lambdaSg.addEgressRule(dbSg, ec2.Port.tcp(config.dbPort), 'PostgreSQL (reader)');
    if (config.manageDbIngress) dbSg.addIngressRule(lambdaSg, ec2.Port.tcp(config.dbPort), 'Lambdas de consultas diarias');

    if (config.endpointsSecurityGroupId) {
      const endpointsSg = ec2.SecurityGroup.fromSecurityGroupId(this, 'EndpointsSg', config.endpointsSecurityGroupId, { mutable: false });
      lambdaSg.addEgressRule(endpointsSg, ec2.Port.tcp(443), 'HTTPS hacia VPC endpoints de interfaz');
      config.gatewayPrefixListIds.forEach((pl, i) => lambdaSg.addEgressRule(ec2.Peer.prefixList(pl), ec2.Port.tcp(443), `Gateway endpoint ${i + 1}`));
    } else {
      // Sin los ids de los endpoints, el 443 sale a cualquier destino: solo es seguro si la VPC no tiene ruta a internet.
      lambdaSg.addEgressRule(ec2.Peer.anyIpv4(), ec2.Port.tcp(443), 'HTTPS (la VPC no tiene ruta a internet)');
      Annotations.of(lambdaSg).addWarningV2(
        'consultas:egress-abierto',
        'Configurar endpointsSecurityGroupId y gatewayPrefixListIds para acotar el egress 443 fuera de dev.',
      );
    }
    const subnets = { subnets: vpc.privateSubnets };

    // ---------- Cifrado ----------
    const key = new kms.Key(this, 'Key', {
      alias: `alias/${name('datos')}`,
      description: 'Cifra cola, ledger, resultados y alarmas de consultas diarias',
      enableKeyRotation: true,
      removalPolicy: RemovalPolicy.RETAIN,
    });
    // CloudWatch publica alarmas en el tópico SNS cifrado.
    key.addToResourcePolicy(
      new iam.PolicyStatement({
        principals: [new iam.ServicePrincipal('cloudwatch.amazonaws.com')],
        actions: ['kms:Decrypt', 'kms:GenerateDataKey*'],
        resources: ['*'],
        conditions: {
          StringEquals: { 'aws:SourceAccount': this.account },
          ArnLike: { 'aws:SourceArn': `arn:${this.partition}:cloudwatch:${this.region}:${this.account}:alarm:*` },
        },
      }),
    );

    // ---------- Datos ----------
    const table = new dynamodb.TableV2(this, 'Executions', {
      tableName: name('executions'),
      partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'SK', type: dynamodb.AttributeType.STRING },
      billing: dynamodb.Billing.onDemand(),
      encryption: dynamodb.TableEncryptionV2.customerManagedKey(key),
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      deletionProtection: true,
      timeToLiveAttribute: 'ttl',
      removalPolicy: RemovalPolicy.RETAIN,
      globalSecondaryIndexes: [
        {
          // Ejecuciones no terminales por proveedor: admisión y barrido.
          indexName: 'open',
          partitionKey: { name: 'openProvider', type: dynamodb.AttributeType.STRING },
          sortKey: { name: 'turno', type: dynamodb.AttributeType.NUMBER },
          projectionType: dynamodb.ProjectionType.INCLUDE,
          nonKeyAttributes: ['status'],
        },
        {
          // Lista de proveedores para el Dispatcher.
          indexName: 'providers',
          partitionKey: { name: 'entity', type: dynamodb.AttributeType.STRING },
          sortKey: { name: 'proveedor', type: dynamodb.AttributeType.STRING },
          projectionType: dynamodb.ProjectionType.ALL,
        },
      ],
    });
    const index = (indexName: string) => `${table.tableArn}/index/${indexName}`;

    const accessLogs = new s3.Bucket(this, 'AccessLogs', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
      minimumTLSVersion: 1.2,
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      lifecycleRules: [{ expiration: Duration.days(90) }],
      removalPolicy: RemovalPolicy.RETAIN,
    });
    const results = new s3.Bucket(this, 'Results', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.KMS,
      encryptionKey: key,
      bucketKeyEnabled: true,
      enforceSSL: true,
      minimumTLSVersion: 1.2,
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      serverAccessLogsBucket: accessLogs,
      serverAccessLogsPrefix: 'results/',
      lifecycleRules: [{ expiration: Duration.days(90) }],
      removalPolicy: RemovalPolicy.RETAIN,
    });

    // ---------- Colas ----------
    // Red de seguridad: recibe el mensaje completo si supera 1.000 recepciones. Solo la leen roles de operación.
    const jobsDlq = new sqs.Queue(this, 'JobsDlq', {
      queueName: `${name('jobs-dlq')}.fifo`,
      fifo: true,
      encryption: sqs.QueueEncryption.KMS,
      encryptionMasterKey: key,
      retentionPeriod: Duration.days(4),
      enforceSSL: true,
    });
    // Un mensaje vive como máximo ~25 h (turno + barrido): 2 días alcanzan y acotan la retención de datos personales.
    const jobs = new sqs.Queue(this, 'Jobs', {
      queueName: `${name('jobs')}.fifo`,
      fifo: true,
      contentBasedDeduplication: false,
      visibilityTimeout: Duration.seconds(240),
      retentionPeriod: Duration.days(2),
      encryption: sqs.QueueEncryption.KMS,
      encryptionMasterKey: key,
      dataKeyReuse: Duration.minutes(5),
      enforceSSL: true,
      deadLetterQueue: { queue: jobsDlq, maxReceiveCount: 1_000 },
    });
    // Cola de agotados: solo ids, la revisa una persona o la herramienta de redrive.
    const exhausted = new sqs.Queue(this, 'Exhausted', {
      queueName: name('agotados'),
      encryption: sqs.QueueEncryption.KMS,
      encryptionMasterKey: key,
      retentionPeriod: Duration.days(14),
      enforceSSL: true,
    });

    // ---------- Lambdas ----------
    const baseEnv = { TABLE_NAME: table.tableName, NODE_OPTIONS: '--enable-source-maps' };
    const dbEnv = {
      DB_HOST: config.dbHost,
      DB_PORT: String(config.dbPort),
      DB_NAME: config.dbName,
      DB_USER: config.dbUser,
      DB_CA_PATH: RUNTIME_CA_BUNDLE,
    };
    const fn = (id: string, entry: string, props: Partial<nodejs.NodejsFunctionProps> & { functionName: string }) => {
      const logGroup = new logs.LogGroup(this, `${id}Logs`, {
        logGroupName: `/aws/lambda/${props.functionName}`,
        retention: logs.RetentionDays.ONE_MONTH,
        removalPolicy: prod ? RemovalPolicy.RETAIN : RemovalPolicy.DESTROY,
      });
      // Rol propio en lugar de las políticas administradas: logs solo en su log group y ENIs solo para el servicio.
      const role = new iam.Role(this, `${id}Role`, {
        assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
        description: `Rol de ${props.functionName}`,
      });
      logGroup.grantWrite(role);
      role.addToPrincipalPolicy(new iam.PolicyStatement({ actions: ENI_ACTIONS, resources: ['*'] }));
      // El código de la función no puede usar los permisos de ENI: Lambda agrega lambda:SourceFunctionArn a sus credenciales.
      role.addToPrincipalPolicy(
        new iam.PolicyStatement({
          effect: iam.Effect.DENY,
          actions: ENI_ACTIONS,
          resources: ['*'],
          conditions: {
            ArnEquals: { 'lambda:SourceFunctionArn': `arn:${this.partition}:lambda:${this.region}:${this.account}:function:${props.functionName}` },
          },
        }),
      );
      const created = new nodejs.NodejsFunction(this, id, {
        entry: path.join(ROOT, 'src', 'entrypoints', entry),
        projectRoot: ROOT,
        depsLockFilePath: path.join(ROOT, 'package-lock.json'),
        runtime: lambda.Runtime.NODEJS_24_X,
        architecture: lambda.Architecture.ARM_64,
        role,
        vpc,
        vpcSubnets: subnets,
        securityGroups: [lambdaSg],
        tracing: lambda.Tracing.ACTIVE,
        logGroup,
        bundling: {
          minify: true,
          sourceMap: true,
          target: 'node22',
          // Se empaqueta todo, incluido el SDK: versiones fijas y probadas. pg-native es opcional.
          externalModules: ['pg-native'],
        },
        ...props,
      });
      return { fn: created, role };
    };

    const dispatcher = fn('Dispatcher', 'dispatcher.ts', {
      functionName: name('dispatcher'),
      description: 'Encola los registros de cada minuto, por proveedor y con admisión',
      timeout: Duration.seconds(50),
      memorySize: 512,
      reservedConcurrentExecutions: 1,
      environment: { ...baseEnv, ...dbEnv, JOBS_QUEUE_URL: jobs.queueUrl, EXHAUSTED_QUEUE_URL: exhausted.queueUrl },
    });
    // Scheduler invoca en forma asíncrona: un tick demorado se descarta y el siguiente retoma desde el cursor.
    dispatcher.fn.configureAsyncInvoke({ maxEventAge: Duration.seconds(60), retryAttempts: 0 });

    const worker = fn('Worker', 'worker.ts', {
      functionName: name('worker'),
      description: 'Llama a la API interna por cada consulta, clasifica el resultado y lo guarda',
      timeout: Duration.seconds(40),
      memorySize: 256,
      environment: {
        ...baseEnv,
        JOBS_QUEUE_URL: jobs.queueUrl,
        EXHAUSTED_QUEUE_URL: exhausted.queueUrl,
        RESULTS_BUCKET: results.bucketName,
        KMS_KEY_ID: key.keyArn,
        API_BASE_URL: `https://${config.apiId}.execute-api.${this.region}.amazonaws.com/${config.apiStage}`,
        LEASE_IN_API: String(config.leaseInApi),
      },
    });
    // Event source mapping explícito: SqsEventSource agregaría un kms:Decrypt sin kms:ViaService.
    new lambda.EventSourceMapping(this, 'WorkerJobs', {
      target: worker.fn,
      eventSourceArn: jobs.queueArn,
      batchSize: 1,
      reportBatchItemFailures: true,
      maxConcurrency: config.workerMaxConcurrency,
    });
    worker.role.addToPrincipalPolicy(
      new iam.PolicyStatement({
        actions: ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:ChangeMessageVisibility', 'sqs:GetQueueAttributes', 'sqs:GetQueueUrl'],
        resources: [jobs.queueArn],
      }),
    );

    const verifier = fn('Verifier', 'verifier.ts', {
      functionName: name('verifier'),
      description: 'Calcula la capacidad por proveedor y el SLI de solapamiento',
      timeout: Duration.minutes(5),
      memorySize: 512,
      reservedConcurrentExecutions: 1,
      environment: { ...baseEnv, ...dbEnv, ...(config.leasesTableName ? { LEASES_TABLE_NAME: config.leasesTableName } : {}) },
    });
    verifier.fn.configureAsyncInvoke({ maxEventAge: Duration.minutes(30), retryAttempts: 2 });

    // ---------- Permisos de mínimo privilegio ----------
    const allow = (role: iam.Role, actions: string[], resources: string[]) =>
      role.addToPrincipalPolicy(new iam.PolicyStatement({ actions, resources }));
    const kmsVia = (role: iam.Role, services: string[]) =>
      role.addToPrincipalPolicy(
        new iam.PolicyStatement({
          actions: ['kms:Decrypt', 'kms:Encrypt', 'kms:GenerateDataKey*', 'kms:DescribeKey'],
          resources: [key.keyArn],
          // La clave solo se usa a través de los servicios: el código no puede llamar a Decrypt directo.
          conditions: { StringEquals: { 'kms:ViaService': services.map((s) => `${s}.${this.region}.amazonaws.com`) } },
        }),
      );
    const dbConnect = `arn:${this.partition}:rds-db:${this.region}:${this.account}:dbuser:${config.dbResourceId}/${config.dbUser}`;

    allow(dispatcher.role, ['dynamodb:Query'], [table.tableArn, index('open'), index('providers')]);
    allow(dispatcher.role, ['dynamodb:BatchGetItem', 'dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:UpdateItem'], [table.tableArn]);
    allow(dispatcher.role, ['rds-db:connect'], [dbConnect]);
    allow(dispatcher.role, ['sqs:SendMessage'], [jobs.queueArn]);
    kmsVia(dispatcher.role, ['sqs', 'dynamodb']);

    allow(worker.role, ['dynamodb:BatchGetItem', 'dynamodb:GetItem', 'dynamodb:UpdateItem'], [table.tableArn]);
    allow(worker.role, ['sqs:SendMessage'], [exhausted.queueArn]);
    allow(worker.role, ['s3:PutObject'], [results.arnForObjects('resultados/*')]);
    allow(
      worker.role,
      ['execute-api:Invoke'],
      config.apiPathPrefixes.flatMap((prefix) => {
        const base = `arn:${this.partition}:execute-api:${this.region}:${this.account}:${config.apiId}/${config.apiStage}/POST/${prefix}`;
        return [base, `${base}/*`];
      }),
    );
    kmsVia(worker.role, ['sqs', 'dynamodb', 's3']);

    allow(verifier.role, ['dynamodb:Query'], [index('providers')]);
    allow(verifier.role, ['dynamodb:GetItem', 'dynamodb:PutItem', 'dynamodb:UpdateItem'], [table.tableArn]);
    allow(verifier.role, ['rds-db:connect'], [dbConnect]);
    if (config.leasesTableName) {
      allow(verifier.role, ['dynamodb:Query'], [`arn:${this.partition}:dynamodb:${this.region}:${this.account}:table/${config.leasesTableName}`]);
    }
    kmsVia(verifier.role, ['dynamodb']);

    // ---------- Políticas de recurso de las colas ----------
    // Aunque otro rol de la cuenta tenga sqs:* por IAM, no puede inyectar trabajos (que el worker firmaría
    // con su identidad hacia la API) ni leer mensajes con datos personales.
    const operators = config.operatorRoleArns;
    const denyExcept = (queue: sqs.Queue, actions: string[], allowed: string[]) =>
      queue.addToResourcePolicy(
        new iam.PolicyStatement({
          effect: iam.Effect.DENY,
          principals: [new iam.AnyPrincipal()],
          actions,
          resources: [queue.queueArn],
          ...(allowed.length > 0 ? { conditions: { ArnNotEquals: { 'aws:PrincipalArn': allowed } } } : {}),
        }),
      );
    denyExcept(jobs, ['sqs:SendMessage'], [dispatcher.role.roleArn]);
    denyExcept(jobs, ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:ChangeMessageVisibility', 'sqs:PurgeQueue'], [worker.role.roleArn, ...operators]);
    denyExcept(exhausted, ['sqs:SendMessage'], [worker.role.roleArn]);
    denyExcept(exhausted, ['sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:PurgeQueue'], operators);
    denyExcept(jobsDlq, ['sqs:SendMessage', 'sqs:ReceiveMessage', 'sqs:DeleteMessage', 'sqs:PurgeQueue', 'sqs:ChangeMessageVisibility'], operators);

    // ---------- Reloj ----------
    const scheduleGroup = new scheduler.ScheduleGroup(this, 'Schedules', { scheduleGroupName: name('schedules') });
    new scheduler.Schedule(this, 'DispatcherTick', {
      description: 'Despacho de consultas, cada minuto',
      scheduleGroup,
      schedule: scheduler.ScheduleExpression.rate(Duration.minutes(1)),
      target: new targets.LambdaInvoke(dispatcher.fn, { maxEventAge: Duration.minutes(1), retryAttempts: 0 }),
    });
    new scheduler.Schedule(this, 'VerifierDaily', {
      description: 'Capacidad y SLI de solapamiento, antes de que empiece el día UTC',
      scheduleGroup,
      schedule: scheduler.ScheduleExpression.cron({ minute: '0', hour: '23' }),
      target: new targets.LambdaInvoke(verifier.fn, { maxEventAge: Duration.hours(1), retryAttempts: 3 }),
    });

    // ---------- Alarmas ----------
    const topic = new sns.Topic(this, 'Alarms', { topicName: name('alarmas'), masterKey: key, enforceSSL: true });
    topic.addToResourcePolicy(
      new iam.PolicyStatement({
        principals: [new iam.ServicePrincipal('cloudwatch.amazonaws.com')],
        actions: ['sns:Publish'],
        resources: [topic.topicArn],
        conditions: {
          StringEquals: { 'aws:SourceAccount': this.account },
          ArnLike: { 'aws:SourceArn': `arn:${this.partition}:cloudwatch:${this.region}:${this.account}:alarm:*` },
        },
      }),
    );
    if (config.alarmEmail) topic.addSubscription(new subscriptions.EmailSubscription(config.alarmEmail));

    const alarm = (
      id: string,
      metric: cloudwatch.IMetric,
      threshold: number,
      description: string,
      opts: { below?: boolean; missingIsBreaching?: boolean } = {},
    ) =>
      new cloudwatch.Alarm(this, id, {
        metric,
        threshold,
        evaluationPeriods: 1,
        comparisonOperator: opts.below
          ? cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD
          : cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
        treatMissingData: opts.missingIsBreaching ? cloudwatch.TreatMissingData.BREACHING : cloudwatch.TreatMissingData.NOT_BREACHING,
        alarmDescription: description,
      }).addAlarmAction(new cwActions.SnsAction(topic));
    const custom = (metricName: string, period = Duration.minutes(5)) =>
      new cloudwatch.Metric({ namespace: NAMESPACE, metricName, statistic: 'Sum', period });

    alarm('SolapamientosAlarm', custom('SolapamientosTotal', Duration.days(1)), 1, 'Hubo dos llamadas simultáneas a un proveedor (SLO: 0)');
    alarm('InviablesAlarm', custom('ProveedoresInviables', Duration.days(1)), 1, 'Un proveedor no entra en 24 h (ρ > 1)');
    alarm('InvalidosRegistrosAlarm', custom('RegistrosInvalidosTotal', Duration.days(1)), 1, 'Hay registros que nunca se van a despachar: corregirlos en origen');
    alarm('FallasApiAlarm', custom('FallasDeApi'), 5, 'La API interna está fallando: despacho en pausa');
    alarm('PresupuestoAlarm', custom('PresupuestoDeColaExcedido'), 1, 'Los topes no entran en la cota de 60.000: repartir proveedores en más colas');
    alarm('InvalidosAlarm', custom('MensajesInvalidos'), 1, 'Llegaron mensajes que no pasan la validación');
    alarm('ErroresProveedorAlarm', custom('ErroresPorProveedor'), 5, 'El Dispatcher falla de forma repetida con algún proveedor');
    alarm('SinConfigurarAlarm', custom('ProveedoresSinConfigurar', Duration.days(1)), 1, 'Hay proveedores nuevos deshabilitados esperando configuración');
    alarm('DlqAlarm', jobsDlq.metricApproximateNumberOfMessagesVisible(), 1, 'Mensajes en la DLQ de seguridad');
    alarm('AgotadosAlarm', exhausted.metricApproximateNumberOfMessagesVisible({ period: Duration.minutes(15) }), 1, 'Consultas con los intentos agotados');
    alarm('BacklogAlarm', jobs.metricApproximateAgeOfOldestMessage({ period: Duration.minutes(5) }), 3_600, 'Hay mensajes esperando hace más de 1 h');
    for (const [id, f] of [['Dispatcher', dispatcher.fn], ['Worker', worker.fn], ['Verifier', verifier.fn]] as const) {
      alarm(`${id}ErrorsAlarm`, f.metricErrors({ period: Duration.minutes(5) }), 3, `Errores no controlados en ${id}`);
      alarm(`${id}ThrottlesAlarm`, f.metricThrottles({ period: Duration.minutes(15) }), 5, `Throttling repetido en ${id}`);
    }
    // Heartbeats: si el Dispatcher o el Verificador dejan de correr, las métricas desaparecen y nada alarma.
    alarm('DispatcherHeartbeat', dispatcher.fn.metricInvocations({ period: Duration.minutes(5) }), 1, 'El Dispatcher no corrió en 5 minutos', {
      below: true,
      missingIsBreaching: true,
    });
    alarm('VerifierHeartbeat', verifier.fn.metricInvocations({ period: Duration.days(1) }), 1, 'El Verificador no corrió en el día', {
      below: true,
      missingIsBreaching: true,
    });

    new CfnOutput(this, 'JobsQueueUrl', { value: jobs.queueUrl });
    new CfnOutput(this, 'ExhaustedQueueUrl', { value: exhausted.queueUrl });
    new CfnOutput(this, 'ExecutionsTable', { value: table.tableName });

    this.suppressReviewedFindings([dispatcher.role, worker.role, verifier.role], accessLogs, exhausted);
  }

  /** Hallazgos de cdk-nag revisados y aceptados, cada uno con su justificación. */
  private suppressReviewedFindings(roles: iam.Role[], accessLogs: s3.Bucket, exhausted: sqs.Queue): void {
    for (const role of roles) {
      NagSuppressions.addResourceSuppressions(
        role,
        [
          {
            id: 'AwsSolutions-IAM5',
            reason:
              'Las acciones de ENI (Describe y Create en subnets/SG elegidos por Lambda) y de X-Ray no admiten recursos específicos. ' +
              'El código no puede usar las de ENI: hay un Deny con lambda:SourceFunctionArn. tests/infra.test.ts verifica que "*" solo aparezca con esas acciones.',
            appliesTo: ['Resource::*', 'Action::kms:GenerateDataKey*'],
          },
          {
            id: 'AwsSolutions-IAM5',
            reason: 'Escritura en el prefijo resultados/ del bucket y en las rutas POST de los prefijos permitidos de la API (respaldo IAM de la allowlist).',
            appliesTo: [
              { regex: '/^Resource::<Results[A-Za-z0-9]+\\.Arn>/resultados/\\*$/g' },
              { regex: '/^Resource::arn:.*:execute-api:.*/POST/.*/\\*$/g' },
              { regex: '/^Resource::<[A-Za-z]+Logs[A-Za-z0-9]+\\.Arn>:\\*$/g' },
            ],
          },
        ],
        true,
      );
    }
    NagSuppressions.addStackSuppressions(this, [
      {
        id: 'AwsSolutions-IAM5',
        reason: 'El rol que crea EventBridge Scheduler invoca la función y sus versiones (`<fn>:*`); el comodín no sale de esa función.',
        appliesTo: [{ regex: '/^Resource::<(Dispatcher|Verifier)[A-Za-z0-9]+\\.Arn>:\\*$/g' }],
      },
    ]);
    NagSuppressions.addResourceSuppressions(accessLogs, [
      { id: 'AwsSolutions-S1', reason: 'Es el bucket de access logs: registrarse a sí mismo generaría un bucle.' },
    ]);
    NagSuppressions.addResourceSuppressions(exhausted, [
      { id: 'AwsSolutions-SQS3', reason: 'Es una cola terminal de revisión manual (solo ids): no la consume ningún proceso automático que pueda fallar.' },
    ]);
  }
}
