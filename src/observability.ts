/**
 * Logs estructurados y métricas por EMF (Embedded Metric Format): las métricas salen en los logs de
 * Lambda, sin llamar a la API de CloudWatch ni necesitar un VPC endpoint.
 *
 * Regla: nunca se loguean bodies ni respuestas, que pueden tener datos personales. Solo ids y códigos.
 */

const NAMESPACE = 'ConsultasDiarias';
type Fields = Record<string, string | number | boolean | undefined>;

function write(level: 'INFO' | 'WARN' | 'ERROR', event: string, fields: Fields): void {
  const line = JSON.stringify({ level, event, ...fields, ts: new Date().toISOString() });
  if (level === 'ERROR') console.error(line);
  else console.log(line);
}

export const log = {
  info: (event: string, fields: Fields = {}) => write('INFO', event, fields),
  warn: (event: string, fields: Fields = {}) => write('WARN', event, fields),
  error: (event: string, fields: Fields = {}) => write('ERROR', event, fields),
};

export function metric(name: string, value: number, dimensions: Record<string, string> = {}, unit = 'Count'): void {
  console.log(
    JSON.stringify({
      _aws: {
        Timestamp: Date.now(),
        CloudWatchMetrics: [{ Namespace: NAMESPACE, Dimensions: [Object.keys(dimensions)], Metrics: [{ Name: name, Unit: unit }] }],
      },
      ...dimensions,
      [name]: value,
    }),
  );
}
