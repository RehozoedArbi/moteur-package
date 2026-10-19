#!/bin/bash
# Crée et remplit enrollment_schema.service_compatibility.
# Relançable sans risque : les lignes existantes sont mises à jour.
set -euo pipefail

NS="university-app"
POD="postgres-0"
DB_USER="postgres_admin"
DB_NAME="university_db"

# Versions (tags d'image) et plage de révisions Alembic supportée
V16="1.6"; V16_MIN="0001"; V16_MAX="0001"   # 1.6 ne connaît pas 0002
V17="1.7"; V17_MIN="0001"; V17_MAX="0002"   # 1.7 contient la migration 0002

# -i est indispensable pour que kubectl transmette le heredoc à psql
kubectl exec -i -n "$NS" "$POD" -- psql -v ON_ERROR_STOP=1 -U "$DB_USER" -d "$DB_NAME" <<SQL
CREATE TABLE IF NOT EXISTS enrollment_schema.service_compatibility (
    service_name    VARCHAR(100) NOT NULL,
    service_version VARCHAR(20)  NOT NULL,
    min_schema      VARCHAR(20)  NOT NULL,
    max_schema      VARCHAR(20)  NOT NULL,
    PRIMARY KEY (service_name, service_version)
);

INSERT INTO enrollment_schema.service_compatibility VALUES
    ('enrollment-service', '${V16}', '${V16_MIN}', '${V16_MAX}'),
    ('enrollment-service', '${V17}', '${V17_MIN}', '${V17_MAX}')
ON CONFLICT (service_name, service_version)
DO UPDATE SET min_schema = EXCLUDED.min_schema,
              max_schema = EXCLUDED.max_schema;

SELECT * FROM enrollment_schema.service_compatibility ORDER BY service_version;
SQL
