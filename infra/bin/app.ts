import { App, Aspects, Tags } from 'aws-cdk-lib';
import { AwsSolutionsChecks } from 'cdk-nag';
import { readConfig } from '../lib/config';
import { ConsultasDiariasStack } from '../lib/consultas-stack';

const app = new App();
const config = readConfig(app);

const stack = new ConsultasDiariasStack(app, `ConsultasDiarias-${config.stage}`, {
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
  config,
});
Tags.of(stack).add('proyecto', 'consultas-diarias');
Tags.of(stack).add('entorno', config.stage);

// Reglas de seguridad de AWS Solutions en cada synth: un hallazgo sin suprimir rompe el build.
Aspects.of(app).add(new AwsSolutionsChecks({ verbose: true }));
