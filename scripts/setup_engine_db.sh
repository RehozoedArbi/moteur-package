#!/bin/bash
# Crée le rôle Postgres en LECTURE SEULE que le moteur utilise (invariant 2).
# Il ne voit que alembic_version et service_compatibility, rien d'autre.
#
# À lancer APRÈS init_compat.sh (la table service_compatibility doit exister) :
#   ENGINE_DB_PASSWORD='un-mot-de-passe' bash setup_engine_db.sh
# Relançable : le rôle est créé s'il manque, son mot de passe est remis à jour.
set -euo pipefail

: "${ENGINE_DB_PASSWORD:?Définis ENGINE_DB_PASSWORD avant de lancer ce script}"

NS="university-app"
POD="postgres-0"
ADMIN="postgres_admin"
DB="university_db"

kubectl exec -i -n "$NS" "$POD" -- \
  psql -v ON_ERROR_STOP=1 -v pw="$ENGINE_DB_PASSWORD" -U "$ADMIN" -d "$DB" <<'SQL'
SELECT format('CREATE ROLE engine_ro LOGIN PASSWORD %L', :'pw')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'engine_ro')
\gexec
ALTER ROLE engine_ro PASSWORD :'pw';
ALTER ROLE engine_ro SET default_transaction_read_only = on;
GRANT CONNECT ON DATABASE university_db TO engine_ro;
GRANT USAGE ON SCHEMA enrollment_schema TO engine_ro;
GRANT SELECT ON enrollment_schema.alembic_version,
                enrollment_schema.service_compatibility TO engine_ro;
SELECT 'rôle engine_ro prêt (lecture seule)' AS resultat;
SQL
