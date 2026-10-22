import type { App } from 'aws-cdk-lib';

/** Configuración por entorno: viene del contexto de CDK (cdk.json, cdk.context.json o `-c clave=valor`). */
export interface StackConfig {
  stage: string;
  vpcId: string;
  vpcCidr: string;
  availabilityZones: string[];
  privateSubnetIds: string[];
  /** SG de los VPC endpoints de interfaz. Si falta, el 443 sale a 0.0.0.0/0 (la VPC no tiene ruta a internet). */
  endpointsSecurityGroupId: string;
  /** Prefix lists de los gateway endpoints de S3 y DynamoDB (pl-xxxx). */
  gatewayPrefixListIds: string[];
  dbSecurityGroupId: string;
  /** true solo si este stack es dueño de la regla de entrada del SG de la base (normalmente la crea el equipo de datos). */
  manageDbIngress: boolean;
  dbHost: string;
  dbPort: number;
  dbName: string;
  dbUser: string;
  /** Resource ID del cluster de Aurora, para la política de autenticación IAM (rds-db:connect). */
  dbResourceId: string;
  apiId: string;
  apiStage: string;
  /** Prefijos de ruta que el worker puede invocar: acotan `execute-api:Invoke` (respaldo IAM de la allowlist). */
  apiPathPrefixes: string[];
  /** Tabla de leases del equipo de la API. Vacío hasta que exista. */
  leasesTableName: string;
  leaseInApi: boolean;
  workerMaxConcurrency: number;
  /** Roles de operación que pueden leer la DLQ y la cola de agotados (triage y redrive). */
  operatorRoleArns: string[];
  alarmEmail: string;
}

export function readConfig(app: App): StackConfig {
  const get = <T>(key: string): T => {
    const value = app.node.tryGetContext(key) as T | undefined;
    if (value === undefined || value === null) throw new Error(`Falta el contexto de CDK "${key}"`);
    return value;
  };
  const bool = (key: string) => {
    const value = get<boolean | string>(key);
    return value === true || value === 'true';
  };
  const config: StackConfig = {
    stage: get<string>('stage'),
    vpcId: get<string>('vpcId'),
    vpcCidr: get<string>('vpcCidr'),
    availabilityZones: get<string[]>('availabilityZones'),
    privateSubnetIds: get<string[]>('privateSubnetIds'),
    endpointsSecurityGroupId: get<string>('endpointsSecurityGroupId'),
    gatewayPrefixListIds: get<string[]>('gatewayPrefixListIds'),
    dbSecurityGroupId: get<string>('dbSecurityGroupId'),
    manageDbIngress: bool('manageDbIngress'),
    dbHost: get<string>('dbHost'),
    dbPort: Number(get<number>('dbPort')),
    dbName: get<string>('dbName'),
    dbUser: get<string>('dbUser'),
    dbResourceId: get<string>('dbResourceId'),
    apiId: get<string>('apiId'),
    apiStage: get<string>('apiStage'),
    apiPathPrefixes: get<string[]>('apiPathPrefixes'),
    leasesTableName: get<string>('leasesTableName'),
    leaseInApi: bool('leaseInApi'),
    workerMaxConcurrency: Number(get<number>('workerMaxConcurrency')),
    operatorRoleArns: get<string[]>('operatorRoleArns'),
    alarmEmail: get<string>('alarmEmail'),
  };
  if (!/^[a-z0-9-]{1,20}$/.test(config.stage)) throw new Error('stage inválido');
  if (config.privateSubnetIds.length !== config.availabilityZones.length) {
    throw new Error('privateSubnetIds y availabilityZones tienen que tener el mismo largo');
  }
  if (config.workerMaxConcurrency < 2 || config.workerMaxConcurrency > 1_000) {
    throw new Error('workerMaxConcurrency tiene que estar entre 2 y 1000 (límite del event source mapping)');
  }
  if (config.apiPathPrefixes.length === 0 || config.apiPathPrefixes.some((p) => !/^[A-Za-z0-9\-._~]+(\/[A-Za-z0-9\-._~]+)*$/.test(p))) {
    throw new Error('apiPathPrefixes tiene que listar rutas sin barra inicial, por ejemplo "consultas"');
  }
  if (config.stage !== 'dev' && !config.alarmEmail) {
    throw new Error('Fuera de dev, alarmEmail es obligatorio: las alarmas tienen que llegar a alguien');
  }
  return config;
}
