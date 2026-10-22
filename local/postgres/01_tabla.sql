-- Tabla del enunciado: id, proveedor, endpoint, body.
CREATE TABLE registros (
  id        text  PRIMARY KEY,
  proveedor text  NOT NULL,
  endpoint  text  NOT NULL,
  body      jsonb NOT NULL
);
