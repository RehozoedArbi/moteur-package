# Moteur de vérification de sûreté (paquet de déploiement)

```
app/                       code du moteur, tests, Dockerfile, requirements.txt
university-moteur/         chart Helm (Chart.yaml, values.yaml, templates/moteur.yaml)
scripts/setup-moteur.sh    construit, publie et déploie
scripts/setup_engine_db.sh crée le rôle Postgres en lecture seule (à lancer avant)
scripts/init_compat.sh     remplit la table service_compatibility
```

## Ordre d'installation
1. `bash scripts/init_compat.sh` puis `ENGINE_DB_PASSWORD='...' bash scripts/setup_engine_db.sh`
2. Avant de déployer, dans le dossier `app/` : `python3 test_moteur.py && python3 test_diagnostic.py && python3 test_executor.py && python3 test_runtime.py`
3. `helm lint university-moteur` puis `helm template university-moteur -n university-moteur | less`
4. `ENGINE_DB_PASSWORD='...' bash scripts/setup-moteur.sh` (mode dry-run par défaut)
   - cluster k3d : ajouter `K3D_CLUSTER=<nom>` pour importer l'image sans la pousser
5. `kubectl logs -n university-moteur deploy/moteur -f`
6. Mode réel (le moteur agit pour de bon) : `DRY_RUN=false CONFIRM_LIVE=yes ENGINE_DB_PASSWORD='...' bash scripts/setup-moteur.sh`

## À savoir
- Nouvelle version du code : changer `TAG` (sinon l'image n'est pas redéployée).
- Les patchs du moteur portent le field manager `moteur` : un `helm upgrade` de l'application
  peut demander `--force-conflicts`.
- Si `university-app` ou `monitoring` ont des NetworkPolicy en entrée, autoriser le namespace
  `university-moteur` vers Postgres (5432) et Prometheus (9090).
- Pour désactiver la NetworkPolicy du moteur pendant un diagnostic : `--set networkPolicy.enabled=false`.
