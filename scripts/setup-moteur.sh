#!/bin/bash
# Construit, publie et déploie le moteur de vérification.
#
# Usage (depuis n'importe où) :
#   ENGINE_DB_PASSWORD='mot-de-passe-du-rôle-engine_ro' bash scripts/setup-moteur.sh
#
# Variables facultatives :
#   TAG=0.1.0          tag de l'image (à changer à chaque nouvelle version du code)
#   DRY_RUN=true       "false" : le moteur AGIT réellement (exige aussi CONFIRM_LIVE=yes)
#   K3D_CLUSTER=nom    importe l'image dans ce cluster k3d au lieu de la pousser
#   SKIP_PUSH=1        ne pousse pas l'image (déjà disponible pour le cluster)
#   REGISTRY=docker.io/darbi
#   DB_USER=engine_ro
#
# Prérequis : le rôle engine_ro existe (setup_engine_db.sh) et docker, kubectl, helm sont installés.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NS="university-moteur"
RELEASE="university-moteur"
REGISTRY="${REGISTRY:-docker.io/darbi}"
IMAGE_NAME="university-moteur"
TAG="${TAG:-0.1.0}"
DRY_RUN="${DRY_RUN:-true}"
DB_USER="${DB_USER:-engine_ro}"
DB_SECRET="moteur-db"
IMAGE="$REGISTRY/$IMAGE_NAME:$TAG"

: "${ENGINE_DB_PASSWORD:?Définis ENGINE_DB_PASSWORD (mot de passe du rôle $DB_USER, voir setup_engine_db.sh)}"

for cmd in docker kubectl helm; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "Commande introuvable : $cmd" >&2; exit 1; }
done

case "$DRY_RUN" in
  true) ;;
  false)
    if [ "${CONFIRM_LIVE:-}" != "yes" ]; then
      echo "DRY_RUN=false : le moteur redémarrerait, redimensionnerait et restaurerait de vrais déploiements." >&2
      echo "Relance avec CONFIRM_LIVE=yes pour confirmer." >&2
      exit 1
    fi ;;
  *) echo "DRY_RUN doit valoir true ou false (reçu : $DRY_RUN)" >&2; exit 1 ;;
esac

echo "Cluster : $(kubectl config current-context)  |  image : $IMAGE  |  mode : DRY_RUN=$DRY_RUN"

echo "[1/5] Construction de l'image"
docker build -t "$IMAGE" "$ROOT/app"

echo "[2/5] Publication de l'image"
if [ -n "${K3D_CLUSTER:-}" ]; then
  k3d image import "$IMAGE" -c "$K3D_CLUSTER"
elif [ -n "${SKIP_PUSH:-}" ]; then
  echo "  push ignoré (SKIP_PUSH)"
else
  docker push "$IMAGE"
fi

echo "[3/5] Namespace $NS (Pod Security : restricted)"
kubectl apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: $NS
  labels:
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/audit: restricted
    pod-security.kubernetes.io/warn: restricted
EOF

echo "[4/5] Secret $DB_SECRET (identifiants lecture seule de la base)"
kubectl create secret generic "$DB_SECRET" -n "$NS" \
  --from-literal=username="$DB_USER" \
  --from-literal=password="$ENGINE_DB_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "[5/5] Déploiement Helm"
helm upgrade --install "$RELEASE" "$ROOT/university-moteur" -n "$NS" \
  --set-string image.registry="$REGISTRY" \
  --set-string image.tag="$TAG" \
  --set-string config.dryRun="$DRY_RUN" \
  --wait --timeout 180s

echo
echo "Moteur déployé. Pour suivre ses verdicts :"
echo "  kubectl logs -n $NS deploy/moteur -f"
if [ "$DRY_RUN" = "true" ]; then
  echo "Il est en dry-run (aucune action réelle). Pour le passer en mode réel :"
  echo "  DRY_RUN=false CONFIRM_LIVE=yes ENGINE_DB_PASSWORD=... bash scripts/setup-moteur.sh"
fi
