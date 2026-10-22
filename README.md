# Consultas diarias por proveedor

Implementación de la propuesta de [docs/ARQUITECTURA.md](docs/ARQUITECTURA.md) para el desafío técnico de tapi.

![Diagrama de arquitectura](docs/diagramas/arquitectura.png)

Cada registro `(id, proveedor, endpoint, body)` se consulta una vez por día contra la API interna, repartido a lo largo de las 24 h y **sin dos llamadas simultáneas al mismo proveedor**. La exclusión la impone SQS FIFO (un grupo por proveedor) y la refuerza un lease dentro de λ2.

## Estructura

```
docs/ARQUITECTURA.md     propuesta: diagramas, decisiones, ciclo de vida y plan
src/
  domain/                lógica pura y testeable: slots, cursor, clasificación de errores,
                         backoff, admisión, breakers, allowlist (SSRF), SLI de solapamiento
  handlers/              Dispatcher, Worker y Verificador, con dependencias inyectadas
  adapters/              DynamoDB, Postgres (Aurora), SQS, S3 y cliente HTTP firmado (SigV4)
  entrypoints/           conexión de los handlers con los adaptadores reales (una por Lambda)
  api-lease/             lease por proveedor para λ2: lo que se le entrega al equipo de la API
infra/                   AWS CDK: colas, tabla, Lambdas, Scheduler, KMS, IAM y alarmas
sql/                     índice del slot y usuario de solo lectura
tests/                   unitarios, handlers con fakes en memoria e infraestructura (cdk-nag)
```

## Cómo correrlo

Requisitos: Node 22.12 o superior.

```bash
npm ci                # instala exactamente lo del lockfile
npm run typecheck
npm test              # 123 tests: dominio, handlers, lease e infraestructura (sin Docker)
npm run synth         # CloudFormation + cdk-nag; un hallazgo sin justificar rompe el build
npm run check         # typecheck + tests + auditoría de dependencias
```

Para regenerar el lockfile desde cero hace falta npm 11 (`npx npm@11 install`): npm 10.9 tiene un bug con los peers opcionales de vitest 4.

## Entorno local (sin cuenta de AWS)

El código real (Dispatcher, Worker y adaptadores) corre contra emuladores en Docker:

| Servicio | Emulador | Qué se prueba de verdad |
|---|---|---|
| Aurora PostgreSQL | Postgres 17 | La tabla `registros`, el índice del slot, la query `LATERAL` y el body como texto |
| DynamoDB | DynamoDB Local (AWS) | Las escrituras condicionales: claim, fencing con token, cursor, ledger |
| SQS FIFO | ElasticMQ | Grupos de mensajes, retención con `ChangeMessageVisibility`, deduplicación |
| API interna | `local/mock-api.ts` (HTTPS) | Proveedores con distinta latencia, uno inestable, una ruta inexistente y el **detector de concurrencia** |

```bash
brew install colima docker docker-compose   # una sola vez (o Docker Desktop)
npm run local:up            # levanta los emuladores y genera el certificado de la API mock
npm run local:setup         # crea tabla, colas y proveedores (idempotente)
npm run test:integration    # 12 tests contra los emuladores
npm run local:simular       # simulación de punta a punta (~4 min) con verificaciones
npm run local:down          # baja todo (y `colima stop` para apagar la VM)
```

La simulación corre el reloj al final del día: despacha el día completo, unos 5.000 registros, con los handlers reales. Los pollers emulan el event source mapping de Lambda. Al final verifica:

- 0 llamadas simultáneas al mismo proveedor;
- firma SigV4 en todas las llamadas;
- body intacto;
- el registro envenenado agota sus intentos sin pausar el sistema;
- el intento de SSRF se rechaza sin llamar a la API;
- nada cae a la DLQ;
- no hay errores no controlados.

Resultado de la última corrida:

- **4.127 llamadas** a la API.
- **Máximo en vuelo por proveedor: 1, con 0 solapamientos.**
- **Las 9 verificaciones en verde.**

**Lo que el entorno local no reemplaza.** ElasticMQ emula SQS, pero la confirmación definitiva de que Lambda con FIFO retiene el grupo igual que acá es el PoC en AWS (semana 1 del plan). Tampoco se prueban IAM, KMS ni la VPC.

**Hallazgo de la simulación.** El proveedor `inestable` (15% de caídas y 5% de errores de registro) procesó ~125 consultas en 4 minutos, contra ~1.000 de cada uno de los demás. Es lo esperado: los reintentos ocurren dentro de su grupo y frenan solo a ese proveedor, nunca a los otros. Pero con datos reales en ese nivel conviene activar el reintento diferido (Anexo A).

### Configuración

Los valores de `cdk.json` son de ejemplo. Los reales van en `cdk.context.json` (ignorado por git) o por `-c clave=valor`:

| Clave | Para qué |
|---|---|
| `vpcId`, `privateSubnetIds`, `availabilityZones`, `vpcCidr` | VPC existente, sin salida a internet |
| `endpointsSecurityGroupId`, `gatewayPrefixListIds` | Acotan el egress 443 a los VPC endpoints. Si faltan, el synth avisa |
| `dbHost`, `dbName`, `dbUser`, `dbResourceId`, `dbSecurityGroupId` | Reader de Aurora con autenticación IAM (ver `sql/002_usuario_lectura.sql`) |
| `apiId`, `apiStage`, `apiPathPrefixes` | API interna privada y rutas que el worker puede invocar |
| `leaseInApi`, `leasesTableName` | Si λ2 ya tiene el lease; si no, el worker aplica la cuarentena |
| `operatorRoleArns` | Roles que pueden leer la DLQ y la cola de agotados |
| `alarmEmail` | Obligatorio fuera de `dev` |

Antes del primer despliegue, el dueño de la base corre `sql/001_indice_slot.sql` y `sql/002_usuario_lectura.sql`.

## Seguridad

El código y la infraestructura pasaron por tres revisiones independientes (aplicación, supply chain e infraestructura). Todo lo de severidad alta y media está corregido y tiene un test que lo cubre:

| Área | Control |
|---|---|
| SSRF | Allowlist por proveedor validada por segmento: sin `%`, `+`, `;`, `@`, `//` ni segmentos de puntos. Además, IAM limita `execute-api:Invoke` a las mismas rutas |
| Registro envenenado | Una falla que no viene del proveedor solo pausa el sistema si afecta a varios registros de al menos dos proveedores. Si no, el registro agota sus intentos |
| Inyección de trabajos | Políticas de recurso: solo el Dispatcher encola y solo el Worker consume |
| Exclusión | FIFO por proveedor, lease en λ2, fencing del claim con token y una sola capa de reintentos |
| IAM | Roles propios sin políticas administradas, KMS con `kms:ViaService` y un Deny para que el código no use los permisos de ENI |
| Datos | KMS en colas, tabla, bucket y tópico; los logs solo tienen ids y códigos; usuario de base de solo lectura |
| Supply chain | `npm audit` sin vulnerabilidades (runtime y dev), lockfile, Actions fijadas por SHA, gitleaks, SBOM y Dependabot |

## Pendiente antes de producción

- **PoC en una cuenta real.** Validar que `ChangeMessageVisibility` combinado con `batchItemFailures` retiene el grupo FIFO como se espera, y medir el tiempo de ocupación por grupo. Ver la semana 1 del plan.
- **Equipo de la API.** Implementar el lease en λ2 (`src/api-lease`), el header `x-error-origin` y el almacén de idempotencia.
- **Operación.**
  - Herramienta de redrive desde la cola de agotados, con una nueva generación de intentos.
  - Reglas de EventBridge sobre CloudTrail: cambios de la clave KMS, de las políticas y de los security groups.
  - Separación de administradores y usuarios de la clave KMS en prod.
- **Pipeline de despliegue.** OIDC hacia AWS y entornos con aprobación.
