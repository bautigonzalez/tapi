-- Usuario del Dispatcher y del Verificador: solo lectura y autenticación IAM (sin contraseña).
-- Lo crea el dueño de la base. El usuario no puede escribir aunque alguien comprometa la Lambda.
CREATE USER consultas_diarias;
GRANT rds_iam TO consultas_diarias;
GRANT CONNECT ON DATABASE registros TO consultas_diarias;
GRANT USAGE ON SCHEMA public TO consultas_diarias;
GRANT SELECT ON registros TO consultas_diarias;
ALTER ROLE consultas_diarias SET default_transaction_read_only = on;
ALTER ROLE consultas_diarias SET statement_timeout = '20s';

-- El security group de la base tiene que aceptar el 5432 desde el security group de las Lambdas
-- que crea el stack. Si el stack es dueño de esa regla, usar -c manageDbIngress=true.
