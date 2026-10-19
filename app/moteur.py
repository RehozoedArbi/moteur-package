"""
moteur.py — Safety Verification Engine (étapes 1 à 3)
=====================================================
Intercepte les propositions du décideur (lignes ACTION_PROPOSAL dans ses
logs), les évalue contre des invariants de sûreté, rend un verdict
ALLOW/DENY structuré, et exécute seulement si ALLOW.

Implémenté à ce stade :
- lecture et déduplication des propositions (étape 1) ;
- lecture de l'état du cluster via l'API Kubernetes (étape 2) ;
- invariants 1 (quorum), 4 (HPA au maximum), 5 (HPA au minimum) (étape 3) ;
- invariant 3 (anti-flapping, étape 4) : en dry-run, un ALLOW compte comme une
  exécution simulée, pour pouvoir observer le refus de la 2e tentative ;
- invariant 2 (compatibilité de schéma, étape 5) : lit alembic_version et
  service_compatibility en lecture seule, avec un rôle Postgres dédié ;
- invariant 6 (cause racine, étapes 6 et 7) : s'appuie sur diagnostic.py
  (détection de dégradation + graphe de dépendances). Si le diagnostic ne
  peut pas conclure (Prometheus muet...), le Restart ou le Rollback est refusé.
- exécuteur (étape 8) : Restart, ScaleUp, ScaleDown, Rollback et InvestigateDB
  via l'API Kubernetes, un seul à la fois. En dry-run (par défaut) il décrit ce
  qu'il ferait (`would_do`) sans rien modifier.

Principes de sûreté :
- Une proposition n'est traitée que si elle est postérieure au démarrage du
  moteur et assez récente : après un redémarrage, on ne rejoue pas ce qui a
  déjà été jugé (risque de double exécution).
- Une proposition n'est jugée qu'une fois (déduplication par incident et
  numéro de tentative, ou à défaut par contenu).
- Un invariant qui lève une erreur, ou qui manque de données (API Kubernetes
  injoignable, par exemple), compte comme une violation : en cas de doute,
  on refuse.
- Tous les invariants applicables sont évalués, sans s'arrêter à la première
  violation : le verdict peut en lister plusieurs (scénario 8).
- Un invariant qui ne concerne pas l'action proposée renvoie NOT_APPLICABLE :
  il n'apparaît pas dans `invariant_checked`.
"""

import os
import json
import signal
import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Callable

from diagnostic import DiagnosticUnavailable, Diagnostician, Prometheus

# ============================================================
# Constantes du protocole avec le décideur
# ============================================================
PROPOSAL_PREFIX = "ACTION_PROPOSAL:"
VERDICT_PREFIX = "VERDICT:"
ALLOWED_ACTIONS = {"Restart", "Rollback", "ScaleUp", "ScaleDown", "InvestigateDB"}
# Services déployés comme Deployment (avec un HPA du même nom)
WORKLOAD_SERVICES = {"student-service", "enrollment-service", "teacher-admin-service"}
ALLOWED_TARGETS = WORKLOAD_SERVICES | {"postgres"}
REQUIRED_FIELDS = ("timestamp", "action", "target", "namespace")

# Renvoyé par un invariant qui ne concerne pas l'action proposée
NOT_APPLICABLE = object()


# ============================================================
# Configuration
# ============================================================
def _env_bool(name: str, default: str) -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "oui")


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, str(default)))


@dataclass
class Config:
    decideur_namespace: str = "university-decideur"
    decideur_label:     str = "app.kubernetes.io/name=decideur"
    app_namespace:      str = "university-app"
    poll_interval_s:    int = 10
    # Au-delà, une proposition est ignorée (et au plus cette durée de logs relue)
    max_proposal_age_s: int = 120
    api_timeout_s:      int = 5
    # Invariant 1 : on refuse un Restart si réplicas prêts <= ce seuil
    quorum_min_ready:   int = 1
    # Invariant 3 : délai minimum (s) entre deux actions identiques sur un service
    flapping_window_s:  int = 120
    # Invariant 2 : accès Postgres en lecture seule (rôle dédié, jamais le superuser)
    db_host:            str = "postgres.university-app.svc.cluster.local"
    db_port:            int = 5432
    db_name:            str = "university_db"
    db_user:            str = ""
    db_password:        str = field(default="", repr=False)   # jamais affiché
    db_timeout_s:       int = 5
    # Invariant 6 : diagnostic (détection de dégradation et cause racine)
    prometheus_url:     str = "http://prometheus.monitoring.svc.cluster.local:9090"
    rate_window:        str = "2m"
    error_rate_degraded: float = 0.10     # taux de 5xx au-delà duquel un service est dégradé
    min_request_rate:   float = 0.5       # req/s minimum pour juger un taux d'erreur
    p95_floor_s:        float = 0.05      # plancher des seuils de la baseline
    baseline_window_s:  int = 1800        # fenêtre de la baseline (30 min)...
    baseline_gap_s:     int = 300         # ...qui s'arrête 5 min avant l'instant présent
    baseline_step_s:    int = 30
    baseline_min_samples: int = 20        # 20 points de 30 s = 10 min d'échantillons sains
    baseline_refresh_s: int = 60
    healthy_error_rate: float = 0.05      # un échantillon est sain sous ce taux d'erreur
    healthy_min_rate:   float = 2.0       # ...et si chaque service reçoit au moins ce trafic
    dep_min_share:      float = 0.2       # appels par requête pour compter une dépendance
    db_min_clients:     int = 2           # services à la fois lents vers la base = Postgres en cause
    diagnosis_cache_s:  int = 10
    alert_cooldown_s:   int = 60          # une alerte identique n'est journalisée qu'une fois par minute
    # Fichier touché à chaque cycle : la sonde liveness du déploiement vérifie sa fraîcheur
    heartbeat_file:     str = "/tmp/moteur-heartbeat"
    dry_run:            bool = True
    log_level:          str = "INFO"

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            decideur_namespace=os.environ.get("DECIDEUR_NAMESPACE", "university-decideur"),
            decideur_label=os.environ.get("DECIDEUR_LABEL", "app.kubernetes.io/name=decideur"),
            app_namespace=os.environ.get("APP_NAMESPACE", "university-app"),
            poll_interval_s=int(os.environ.get("POLL_INTERVAL_SECONDS", "10")),
            max_proposal_age_s=int(os.environ.get("MAX_PROPOSAL_AGE_SECONDS", "120")),
            api_timeout_s=int(os.environ.get("API_TIMEOUT_SECONDS", "5")),
            quorum_min_ready=int(os.environ.get("QUORUM_MIN_READY", "1")),
            flapping_window_s=int(os.environ.get("FLAPPING_WINDOW_SECONDS", "120")),
            db_host=os.environ.get("DB_HOST", "postgres.university-app.svc.cluster.local"),
            db_port=int(os.environ.get("DB_PORT", "5432")),
            db_name=os.environ.get("DB_NAME", "university_db"),
            db_user=os.environ.get("DB_USER", ""),
            db_password=os.environ.get("DB_PASSWORD", ""),
            db_timeout_s=int(os.environ.get("DB_TIMEOUT_SECONDS", "5")),
            prometheus_url=os.environ.get(
                "PROMETHEUS_URL", "http://prometheus.monitoring.svc.cluster.local:9090"),
            rate_window=os.environ.get("RATE_WINDOW", "2m"),
            error_rate_degraded=_env_float("ERROR_RATE_DEGRADED", 0.10),
            min_request_rate=_env_float("MIN_REQUEST_RATE", 0.5),
            p95_floor_s=_env_float("P95_FLOOR_SECONDS", 0.05),
            baseline_window_s=_env_int("BASELINE_WINDOW_SECONDS", 1800),
            baseline_gap_s=_env_int("BASELINE_GAP_SECONDS", 300),
            baseline_step_s=_env_int("BASELINE_STEP_SECONDS", 30),
            baseline_min_samples=_env_int("BASELINE_MIN_SAMPLES", 20),
            baseline_refresh_s=_env_int("BASELINE_REFRESH_SECONDS", 60),
            healthy_error_rate=_env_float("HEALTHY_ERROR_RATE", 0.05),
            healthy_min_rate=_env_float("HEALTHY_MIN_RATE", 2.0),
            dep_min_share=_env_float("DEP_MIN_SHARE", 0.2),
            db_min_clients=_env_int("DB_MIN_CLIENTS", 2),
            diagnosis_cache_s=_env_int("DIAGNOSIS_CACHE_SECONDS", 10),
            alert_cooldown_s=_env_int("ALERT_COOLDOWN_SECONDS", 60),
            heartbeat_file=os.environ.get("HEARTBEAT_FILE", "/tmp/moteur-heartbeat"),
            dry_run=_env_bool("DRY_RUN", "true"),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
        )


def setup_logging(level: str) -> logging.Logger:
    logging.basicConfig(
        level=level,
        format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
               '"service": "moteur", "message": %(message)s}',
        datefmt="%Y-%m-%dT%H:%M:%S+00:00",
    )
    return logging.getLogger("moteur")


logger = logging.getLogger("moteur")


def _info(msg: str) -> None:
    logger.info(json.dumps(msg))


def _warn(msg: str) -> None:
    logger.warning(json.dumps(msg))


def _error(msg: str) -> None:
    logger.error(json.dumps(msg))


def _critical(msg: str) -> None:
    logger.critical(json.dumps(msg))


# ============================================================
# Erreurs
# ============================================================
class ClusterUnavailable(Exception):
    """L'API Kubernetes est injoignable ou a répondu en erreur."""


class DataUnavailable(Exception):
    """Un invariant n'a pas les données nécessaires pour conclure."""


# ============================================================
# Lecture des propositions du décideur
# ============================================================
def parse_proposal_line(line: str) -> dict | None:
    """
    Extrait le JSON d'une ligne « ACTION_PROPOSAL:{...} ».
    None si la ligne n'est pas une proposition ou si le JSON est illisible.
    """
    idx = line.find(PROPOSAL_PREFIX)
    if idx == -1:
        return None
    try:
        proposal = json.loads(line[idx + len(PROPOSAL_PREFIX):])
    except ValueError:
        _warn(f"Proposition illisible ignorée : {line[:200]}")
        return None
    return proposal if isinstance(proposal, dict) else None


class DecideurLogReader:
    """Lit les logs des pods du décideur via l'API Kubernetes."""

    def __init__(self, core_api, cfg: Config):
        self.core = core_api
        self.cfg = cfg

    def read_lines(self) -> list[str]:
        try:
            pods = self.core.list_namespaced_pod(
                self.cfg.decideur_namespace,
                label_selector=self.cfg.decideur_label,
                _request_timeout=self.cfg.api_timeout_s,
            ).items
            lines: list[str] = []
            for pod in pods:
                text = self.core.read_namespaced_pod_log(
                    name=pod.metadata.name,
                    namespace=self.cfg.decideur_namespace,
                    since_seconds=self.cfg.max_proposal_age_s,
                    _request_timeout=self.cfg.api_timeout_s,
                )
                lines.extend(text.splitlines())
            return lines
        except Exception as e:  # toute erreur d'API = cluster indisponible
            raise ClusterUnavailable(str(e)) from e


# ============================================================
# État du cluster (étape 2)
# ============================================================
class ClusterState:
    """
    Lit l'état réel d'un service via l'API Kubernetes (source de vérité,
    plus fraîche que Prometheus).

    snapshot(service) renvoie :
      spec_replicas : réplicas désirés par le Deployment (pilotés par le HPA)
      ready         : réplicas prêts (0 si aucun)
      hpa_min / hpa_max : bornes du HPA du même nom, None s'il n'y a pas de HPA
    """

    def __init__(self, apps_api, autoscaling_api, namespace: str, timeout_s: int):
        self.apps = apps_api
        self.autoscaling = autoscaling_api
        self.namespace = namespace
        self.timeout = timeout_s

    def snapshot(self, service: str) -> dict:
        if service not in WORKLOAD_SERVICES:
            raise DataUnavailable(f"{service!r} n'est pas un Deployment suivi par le moteur")
        try:
            dep = self.apps.read_namespaced_deployment(
                service, self.namespace, _request_timeout=self.timeout)
        except Exception as e:
            raise ClusterUnavailable(f"lecture du Deployment {service} : {e}") from e

        try:
            hpa = self.autoscaling.read_namespaced_horizontal_pod_autoscaler(
                service, self.namespace, _request_timeout=self.timeout)
        except Exception as e:
            if getattr(e, "status", None) == 404:
                hpa = None  # pas de HPA : état connu, pas une erreur
            else:
                raise ClusterUnavailable(f"lecture du HPA {service} : {e}") from e

        spec_replicas = dep.spec.replicas if dep.spec.replicas is not None else 1
        ready = dep.status.ready_replicas or 0
        return {
            "spec_replicas": spec_replicas,
            "ready": ready,
            "hpa_min": (hpa.spec.min_replicas if hpa.spec.min_replicas is not None else 1)
                       if hpa else None,
            "hpa_max": hpa.spec.max_replicas if hpa else None,
        }


def _read_container_image(self, service: str) -> tuple[str, str]:
    """(nom du conteneur, image) du Deployment : sert à construire l'image d'un Rollback."""
    if service not in WORKLOAD_SERVICES:
        raise DataUnavailable(f"{service!r} n'est pas un Deployment suivi par le moteur")
    try:
        dep = self.apps.read_namespaced_deployment(
            service, self.namespace, _request_timeout=self.timeout)
    except Exception as e:
        raise ClusterUnavailable(f"lecture du Deployment {service} : {e}") from e
    containers = dep.spec.template.spec.containers
    for c in containers:
        if c.name == service:
            return c.name, c.image
    if len(containers) == 1:
        return containers[0].name, containers[0].image
    raise DataUnavailable(f"conteneur de {service} introuvable parmi {len(containers)}")


ClusterState.container_image = _read_container_image


# ============================================================
# Schéma de la base (étape 5)
# ============================================================
# Services dont le schéma est versionné par Alembic, et schéma Postgres qui
# contient alembic_version et service_compatibility. Pour les autres services,
# la compatibilité d'un rollback ne peut pas être vérifiée : refus par sûreté.
SCHEMA_OF = {"enrollment-service": "enrollment_schema"}


def _rev_key(revision: str) -> tuple:
    """Clé de comparaison de révisions : numériques ('0001' < '0002') ou texte."""
    return (0, int(revision), "") if revision.isdigit() else (1, 0, revision)


class SchemaReader:
    """
    Lit alembic_version et service_compatibility, en lecture seule, avec un
    rôle Postgres dédié (variables DB_USER et DB_PASSWORD).
    `connect` est injectable pour les tests.
    """

    def __init__(self, cfg: Config, connect: Callable | None = None):
        self.cfg = cfg
        self._connect = connect or self._default_connect

    def _default_connect(self):
        import psycopg2  # importé ici : inutile pour les tests
        return psycopg2.connect(
            host=self.cfg.db_host, port=self.cfg.db_port, dbname=self.cfg.db_name,
            user=self.cfg.db_user, password=self.cfg.db_password,
            connect_timeout=self.cfg.db_timeout_s,
            options=f"-c statement_timeout={self.cfg.db_timeout_s * 1000} "
                    f"-c default_transaction_read_only=on",
        )

    def read(self, schema: str, service: str, version: str) -> dict:
        """Révision actuelle de la base et plage de schémas supportée par `version`."""
        if not schema.replace("_", "").isalnum():
            raise DataUnavailable(f"nom de schéma invalide : {schema!r}")
        try:
            conn = self._connect()
        except Exception as e:
            raise DataUnavailable(f"base de données injoignable : {e}") from e
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT version_num FROM {schema}.alembic_version")
            revisions = [row[0] for row in cur.fetchall()]
            cur.execute(
                f"SELECT min_schema, max_schema FROM {schema}.service_compatibility "
                f"WHERE service_name = %s AND service_version = %s", (service, version))
            ranges = cur.fetchall()
        except Exception as e:
            raise DataUnavailable(f"lecture du schéma impossible : {e}") from e
        finally:
            try:
                conn.close()
            except Exception:
                pass
        if len(revisions) != 1:
            raise DataUnavailable(f"alembic_version contient {len(revisions)} révision(s), 1 attendue")
        if not ranges:
            raise DataUnavailable(
                f"version {version} de {service} absente de service_compatibility")
        return {"current": revisions[0], "min": ranges[0][0], "max": ranges[0][1]}


def check_schema_compat(reader: SchemaReader, service: str, version: str) -> dict:
    """Compatibilité de `version` avec le schéma actuel : {current, min, max, compatible}."""
    schema = SCHEMA_OF.get(service)
    if schema is None:
        raise DataUnavailable(f"aucun schéma versionné déclaré pour {service}")
    data = reader.read(schema, service, version)
    cur, lo, hi = (_rev_key(data["current"]), _rev_key(data["min"]), _rev_key(data["max"]))
    return {**data, "compatible": lo <= cur <= hi}


# ============================================================
# Historique des actions exécutées et des incidents
# ============================================================
class ActionHistory:
    """
    Actions exécutées (pour l'anti-flapping, invariant 3).
    En dry-run, un verdict ALLOW est enregistré comme exécution SIMULÉE : rien
    ne s'exécute, mais l'anti-flapping peut ainsi se démontrer sans agir.
    """

    def __init__(self):
        self._last: dict[tuple[str, str], tuple[datetime, bool]] = {}

    def record(self, target: str, action: str, when: datetime,
               simulated: bool = False) -> None:
        self._last[(target, action)] = (when, simulated)

    def last(self, target: str, action: str) -> datetime | None:
        entry = self._last.get((target, action))
        return entry[0] if entry else None

    def last_entry(self, target: str, action: str) -> tuple[datetime, bool] | None:
        """(instant, simulée ?) de la dernière action identique, ou None."""
        return self._last.get((target, action))


class IncidentTracker:
    """Regroupe les tentatives d'un même incident (details.incident_id)."""

    def __init__(self):
        self._incidents: dict[str, list[dict]] = {}

    def record(self, incident_id: str | None, attempt: int | None,
               verdict: str, when: datetime) -> None:
        if not incident_id:
            return
        self._incidents.setdefault(incident_id, []).append(
            {"attempt": attempt, "verdict": verdict, "at": when.isoformat()}
        )

    def history(self, incident_id: str | None) -> list[dict]:
        return list(self._incidents.get(incident_id or "", []))


# ============================================================
# Invariants : registre et évaluation
# ============================================================
@dataclass
class Context:
    """Ce que les invariants peuvent consulter pendant l'évaluation."""
    now: datetime
    config: Config
    history: ActionHistory
    incidents: IncidentTracker
    cluster: ClusterState | None = None
    schema: SchemaReader | None = None
    diagnostic: Diagnostician | None = None
    # Valeurs mesurées par les invariants, reprises dans le verdict
    evidence: dict = field(default_factory=dict)
    # Alertes à signaler (ex. cause racine = postgres), reprises dans le verdict
    alerts: list = field(default_factory=list)


# Un invariant est une fonction (proposal, ctx) qui renvoie :
#   NOT_APPLICABLE  si l'action proposée ne le concerne pas,
#   None            s'il est respecté,
#   {"message": str, "evidence": dict}   s'il est violé.
INVARIANTS: list[tuple[str, Callable]] = []


def invariant(name: str):
    """Décorateur : enregistre une fonction d'invariant sous ce nom."""
    def decorator(fn: Callable) -> Callable:
        INVARIANTS.append((name, fn))
        return fn
    return decorator


def evaluate(proposal: dict, ctx: Context,
             registry: list | None = None) -> tuple[list[str], list[dict]]:
    """
    Évalue TOUS les invariants applicables (pas d'arrêt à la première
    violation). Retourne (noms vérifiés, violations).
    """
    registry = INVARIANTS if registry is None else registry
    checked: list[str] = []
    violations: list[dict] = []
    for name, fn in registry:
        try:
            result = fn(proposal, ctx)
        except (DataUnavailable, ClusterUnavailable) as e:
            checked.append(name)
            violations.append({"invariant": name,
                               "message": f"Données insuffisantes : {e}",
                               "evidence": {}})
            continue
        except Exception as e:
            _error(f"Invariant {name} en erreur : {type(e).__name__}: {e}")
            checked.append(name)
            violations.append({"invariant": name,
                               "message": f"Erreur d'évaluation ({type(e).__name__}) : "
                                          f"refus par sûreté",
                               "evidence": {}})
            continue
        if result is NOT_APPLICABLE:
            continue
        checked.append(name)
        if result:
            violations.append({"invariant": name,
                               "message": result["message"],
                               "evidence": result.get("evidence", {})})
    return checked, violations


# ------------------------------------------------------------
# Invariants implémentés
# ------------------------------------------------------------
def _state(proposal: dict, ctx: Context) -> dict:
    if ctx.cluster is None:
        raise DataUnavailable("lecture de l'état du cluster non configurée")
    return ctx.cluster.snapshot(proposal["target"])


@invariant("invariant1_quorum")
def invariant_quorum(proposal: dict, ctx: Context):
    """
    Replicas_Ready(S) <= seuil  ⟹  Deny(Restart(S)) et Deny(Rollback(S)).
    Un rollback remplace les pods comme un restart : même risque pour le quorum.
    """
    action = proposal["action"]
    if action not in ("Restart", "Rollback"):
        return NOT_APPLICABLE
    service = proposal["target"]
    state = _state(proposal, ctx)
    ready, threshold = state["ready"], ctx.config.quorum_min_ready
    evidence = {"ready": ready, "spec_replicas": state["spec_replicas"],
                "seuil": threshold}
    ctx.evidence["invariant1_quorum"] = evidence
    if ready > threshold:
        return None
    if ready == 0:
        message = (f"{action} refusé — {service} n'a aucun réplica actif (ready=0). "
                   f"Cette action prolongerait l'indisponibilité.")
    else:
        message = (f"{action} refusé — {service} n'a que {ready} réplica prêt "
                   f"(seuil de quorum : {threshold}). Cette action ferait tomber le quorum.")
    return {"message": message, "evidence": evidence}


@invariant("invariant2_schema")
def invariant_schema(proposal: dict, ctx: Context):
    """Version_Cible(S) incompatible avec Schema_DB_Actuel  ⟹  Deny(Rollback(S))."""
    if proposal["action"] != "Rollback":
        return NOT_APPLICABLE
    service = proposal["target"]
    details = proposal.get("details") if isinstance(proposal.get("details"), dict) else {}
    version = details.get("target_version")
    if not version:
        raise DataUnavailable("version cible du rollback absente de la proposition")
    if ctx.schema is None:
        raise DataUnavailable("lecture du schéma de la base non configurée "
                              "(DB_USER / DB_PASSWORD)")
    result = check_schema_compat(ctx.schema, service, str(version))
    evidence = {"version_cible": str(version), "schema_actuel": result["current"],
                "plage_supportee": [result["min"], result["max"]],
                "compatible": result["compatible"]}
    ctx.evidence["invariant2_schema"] = evidence
    if result["compatible"]:
        return None
    return {"message": (f"Rollback vers {version} impossible — schéma DB actuel "
                        f"({result['current']}) incompatible avec {version} "
                        f"(cette version supporte les schémas {result['min']} à "
                        f"{result['max']})."),
            "evidence": evidence}


@invariant("invariant3_anti_flapping")
def invariant_anti_flapping(proposal: dict, ctx: Context):
    """Action identique sur S exécutée il y a < délai  ⟹  Deny(Action(S))."""
    action, service = proposal["action"], proposal["target"]
    if action == "InvestigateDB":
        return NOT_APPLICABLE  # une alerte n'agit pas sur le cluster
    window = ctx.config.flapping_window_s
    entry = ctx.history.last_entry(service, action)
    if entry is None:
        ctx.evidence["invariant3_anti_flapping"] = {"derniere_action": None,
                                                    "delai_minimum_s": window}
        return None
    last, simulated = entry
    elapsed = int((ctx.now - last).total_seconds())
    evidence = {"derniere_action": last.isoformat(), "ecoule_s": elapsed,
                "delai_minimum_s": window, "simulee": simulated}
    ctx.evidence["invariant3_anti_flapping"] = evidence
    if elapsed >= window:
        return None
    note = " (exécution simulée en dry-run)" if simulated else ""
    return {"message": (f"{action} refusé — une action identique a été exécutée sur "
                        f"{service} il y a {elapsed}s{note} (délai minimum : {window}s)."),
            "evidence": evidence}


@invariant("invariant4_hpa_max")
def invariant_hpa_max(proposal: dict, ctx: Context):
    """current_replicas >= hpa_max  ⟹  Deny(ScaleUp(S))."""
    if proposal["action"] != "ScaleUp":
        return NOT_APPLICABLE
    service = proposal["target"]
    state = _state(proposal, ctx)
    current, hpa_max = state["spec_replicas"], state["hpa_max"]
    evidence = {"current_replicas": current, "hpa_max": hpa_max}
    ctx.evidence["invariant4_hpa_max"] = evidence
    if hpa_max is None or current < hpa_max:
        return None  # pas de HPA = pas de maximum déclaré
    return {"message": (f"ScaleUp impossible — {service} est déjà au maximum de "
                        f"réplicas autorisé ({current}/{hpa_max})."),
            "evidence": evidence}


@invariant("invariant5_hpa_min")
def invariant_hpa_min(proposal: dict, ctx: Context):
    """current_replicas <= hpa_min  ⟹  Deny(ScaleDown(S))."""
    if proposal["action"] != "ScaleDown":
        return NOT_APPLICABLE
    service = proposal["target"]
    state = _state(proposal, ctx)
    current = state["spec_replicas"]
    # Sans HPA, on ne descend jamais sous 1 réplica
    hpa_min = state["hpa_min"] if state["hpa_min"] is not None else 1
    evidence = {"current_replicas": current, "hpa_min": hpa_min,
                "hpa_present": state["hpa_min"] is not None}
    ctx.evidence["invariant5_hpa_min"] = evidence
    if current > hpa_min:
        return None
    return {"message": (f"ScaleDown refusé — {service} est déjà au minimum de "
                        f"réplicas autorisé ({current}/{hpa_min})."),
            "evidence": evidence}


@invariant("invariant6_cause_racine")
def invariant_root_cause(proposal: dict, ctx: Context):
    """
    Service dégradé dont une dépendance l'est aussi  ⟹  Deny(Restart/Rollback).
    Agir sur un symptôme ne résout rien : on refuse et on désigne la cause.
    """
    action, service = proposal["action"], proposal["target"]
    if action not in ("Restart", "Rollback"):
        return NOT_APPLICABLE
    if ctx.diagnostic is None:
        raise DataUnavailable("diagnostic non configuré (PROMETHEUS_URL)")
    try:
        diagnosis = ctx.diagnostic.diagnose(ctx.now)
        if service not in diagnosis.degraded_nodes:
            ctx.evidence["invariant6_cause_racine"] = diagnosis.summary()
            return None   # le moteur ne voit pas ce service dégradé : rien à reprocher
        roots = diagnosis.roots_for(service)
    except DiagnosticUnavailable as e:
        raise DataUnavailable(str(e)) from e
    evidence = diagnosis.summary(roots)
    ctx.evidence["invariant6_cause_racine"] = evidence
    if service in roots:
        return None       # ce service est lui-même la cause
    causes = ", ".join(sorted(roots))
    if "postgres" in roots:
        ctx.alerts.append({
            "niveau": "CRITICAL", "cause_racine": "postgres",
            "action_suggeree": "InvestigateDB",
            "services_degrades": sorted(n for n in diagnosis.degraded_nodes if n != "postgres"),
            "message": "Plusieurs services sont lents vers la base : cause racine probable = postgres."})
    return {"message": (f"{action} refusé — {service} est un symptôme : cause racine "
                        f"probable = {causes}. Agir sur {service} ne résoudra pas le problème."),
            "evidence": evidence}


# ============================================================
# Validation, verdict
# ============================================================
def _parse_ts(value) -> datetime | None:
    try:
        ts = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def validate_proposal(proposal: dict, app_namespace: str | None = None) -> list[str]:
    """Liste des problèmes de forme (vide si la proposition est valide)."""
    problems = [f"champ manquant : {f}" for f in REQUIRED_FIELDS if f not in proposal]
    if "action" in proposal and proposal["action"] not in ALLOWED_ACTIONS:
        problems.append(f"action inconnue : {proposal['action']!r}")
    if "target" in proposal and proposal["target"] not in ALLOWED_TARGETS:
        problems.append(f"cible inconnue : {proposal['target']!r}")
    if "timestamp" in proposal and _parse_ts(proposal["timestamp"]) is None:
        problems.append(f"horodatage illisible : {proposal['timestamp']!r}")
    if (app_namespace and "namespace" in proposal
            and proposal["namespace"] != app_namespace):
        problems.append(f"namespace non géré : {proposal['namespace']!r}")
    return problems


def _incident_info(proposal: dict) -> dict:
    details = proposal.get("details") if isinstance(proposal.get("details"), dict) else {}
    return {k: details.get(k) for k in ("incident_id", "attempt", "max_attempts")}


def build_verdict(proposal: dict, checked: list[str], violations: list[dict],
                  evidence: dict, execution: dict, mode: str,
                  now: datetime, alerts: list | None = None) -> dict:
    deny = bool(violations)
    return {
        "timestamp": now.isoformat(),
        "proposal": proposal,
        "verdict": "DENY" if deny else "ALLOW",
        "invariant_checked": checked,
        "invariant_violated": [v["invariant"] for v in violations],
        "justification": (" | ".join(v["message"] for v in violations)
                          if deny else "Aucun invariant violé."),
        "violations": violations,
        "evidence": evidence,
        "alerts": alerts or [],
        "incident": _incident_info(proposal),
        "mode": mode,
        "execution": execution,
    }


# ============================================================
# Exécuteur (étape 8)
# ============================================================
RESTARTED_AT = "kubectl.kubernetes.io/restartedAt"
FIELD_MANAGER = "moteur"   # visible dans `kubectl get deploy -o yaml --show-managed-fields`


class ExecutionSkipped(Exception):
    """Action autorisée mais sans objet (déjà au maximum, déjà sur cette image...)."""


def _with_tag(image: str, tag: str) -> str:
    """docker.io/darbi/x:1.7 -> docker.io/darbi/x:1.6 (gère un port de registre et un digest)."""
    base = image.split("@", 1)[0]
    head, sep, last = base.rpartition("/")
    return f"{head}{sep}{last.split(':', 1)[0]}:{tag}"


def _norm_image(image: str) -> str:
    """Compare des images en ignorant le préfixe docker.io et le digest."""
    base = image.split("@", 1)[0]
    for prefix in ("docker.io/", "index.docker.io/"):
        if base.startswith(prefix):
            return base[len(prefix):]
    return base


class Executor:
    """
    Applique une action autorisée par l'API Kubernetes (patch du Deployment).
    - Un verrou garantit qu'une seule action s'exécute à la fois.
    - L'état est relu juste avant d'agir : un ScaleUp ajoute 1 réplica sans
      dépasser le maximum du HPA, un ScaleDown en retire 1 sans passer sous le
      minimum, un Rollback ne vise que l'image précédente observée.
    - Un échec n'est pas une exécution : il n'entre pas dans l'historique
      d'anti-flapping, et le verdict l'indique (result = "failed").
    - ATTENTION : avec un HPA actif, Kubernetes peut annuler un changement de
      réplicas à sa prochaine évaluation (le HPA possède `spec.replicas`).
    - Les patchs portent le field manager "moteur" : un `helm upgrade` en
      server-side apply peut ensuite demander --force-conflicts.
    """

    def __init__(self, dry_run: bool, apps_api=None, cluster: ClusterState | None = None,
                 cfg: Config | None = None, clock: Callable | None = None):
        self.dry_run = dry_run
        self.apps = apps_api
        self.cluster = cluster
        self.namespace = cfg.app_namespace if cfg else "university-app"
        self.timeout = cfg.api_timeout_s if cfg else 5
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._lock = threading.Lock()

    def run(self, proposal: dict) -> dict:
        with self._lock:
            try:
                step = self._plan(proposal)
            except ExecutionSkipped as e:
                return {"executed": False, "result": "skipped", "error": None, "reason": str(e)}
            except Exception as e:   # cluster injoignable, donnée absente, erreur imprévue...
                detail = f"{type(e).__name__}: {e}"
                if not isinstance(e, (ClusterUnavailable, DataUnavailable)):
                    _error(f"Préparation de l'action en erreur imprévue : {detail}")
                if self.dry_run:
                    return {"executed": False, "result": "skipped", "error": None,
                            "reason": "dry-run", "would_do": None, "plan_error": detail}
                return {"executed": False, "result": "failed",
                        "error": f"préparation impossible : {detail}", "reason": None}
            if self.dry_run:
                return {"executed": False, "result": "skipped", "error": None,
                        "reason": "dry-run", "would_do": step["description"]}
            return self._apply(step, proposal)

    def _plan(self, proposal: dict) -> dict:
        action, service = proposal["action"], proposal["target"]
        details = proposal.get("details") if isinstance(proposal.get("details"), dict) else {}
        if action == "InvestigateDB":
            return {"kind": "alert", "service": service,
                    "description": "alerte CRITICAL (aucune action sur le cluster)"}
        if action == "Restart":
            when = self._clock().isoformat()
            return {"kind": "patch", "service": service,
                    "body": {"spec": {"template": {"metadata": {"annotations": {RESTARTED_AT: when}}}}},
                    "description": f"rollout restart de deployment/{service}"}
        if self.cluster is None:
            raise DataUnavailable("état du cluster indisponible pour l'exécuteur")

        if action in ("ScaleUp", "ScaleDown"):
            snap = self.cluster.snapshot(service)
            current = snap["spec_replicas"]
            if action == "ScaleUp":
                new, limit = current + 1, snap["hpa_max"]
                if limit is not None and new > limit:
                    raise ExecutionSkipped(f"ScaleUp sans objet : déjà au maximum ({current}/{limit})")
            else:
                new = current - 1
                floor = snap["hpa_min"] if snap["hpa_min"] is not None else 1
                if new < floor:
                    raise ExecutionSkipped(f"ScaleDown sans objet : déjà au minimum ({current}/{floor})")
            return {"kind": "patch", "service": service, "body": {"spec": {"replicas": new}},
                    "description": f"réplicas de {service} : {current} → {new}"}

        if action == "Rollback":
            version = details.get("target_version")
            if not version:
                raise ExecutionSkipped("Rollback impossible : version cible absente de la proposition")
            container, current = self.cluster.container_image(service)
            target = _with_tag(current, str(version))
            previous = details.get("previous_images")
            if isinstance(previous, list) and previous and \
                    _norm_image(target) not in {_norm_image(p) for p in previous}:
                raise ExecutionSkipped(
                    f"Rollback refusé : l'image {target} n'est pas une image précédente observée ({previous})")
            if _norm_image(target) == _norm_image(current):
                raise ExecutionSkipped(f"Rollback sans objet : déjà sur l'image {current}")
            return {"kind": "patch", "service": service,
                    "body": {"spec": {"template": {"spec": {"containers": [
                        {"name": container, "image": target}]}}}},
                    "description": f"image de {service} : {current} → {target}"}
        raise ExecutionSkipped(f"action non gérée : {action}")

    def _apply(self, step: dict, proposal: dict) -> dict:
        try:
            if step["kind"] == "alert":
                _critical(f"ALERTE InvestigateDB — {proposal.get('reason', 'cause racine à investiguer')}")
            else:
                if self.apps is None:
                    raise RuntimeError("API Kubernetes non configurée pour l'exécuteur")
                self.apps.patch_namespaced_deployment(
                    step["service"], self.namespace, step["body"],
                    field_manager=FIELD_MANAGER, _request_timeout=self.timeout)
        except Exception as e:
            return {"executed": False, "result": "failed",
                    "error": f"{type(e).__name__}: {e}", "reason": None}
        return {"executed": True, "result": "success", "error": None, "reason": None,
                "done": step["description"]}


# ============================================================
# Moteur
# ============================================================
class Engine:
    def __init__(self, cfg: Config, reader, executor: Executor | None = None,
                 started_at: datetime | None = None,
                 cluster: ClusterState | None = None,
                 schema: SchemaReader | None = None,
                 diagnostic: Diagnostician | None = None):
        self.cfg = cfg
        self.reader = reader
        self.cluster = cluster
        self.schema = schema
        self.diagnostic = diagnostic
        self._alert_logged: dict[str, datetime] = {}
        self.executor = executor or Executor(cfg.dry_run, None, cluster, cfg)
        self.history = ActionHistory()
        self.incidents = IncidentTracker()
        self.started_at = started_at or datetime.now(timezone.utc)
        self._seen: dict[tuple, datetime] = {}

    @property
    def mode(self) -> str:
        return "dry-run" if self.cfg.dry_run else "live"

    @staticmethod
    def _dedup_key(proposal: dict) -> tuple:
        info = _incident_info(proposal)
        if info["incident_id"] and info["attempt"] is not None:
            return ("incident", info["incident_id"], info["attempt"])
        return ("contenu", json.dumps(proposal, sort_keys=True))

    def handle(self, proposal: dict, now: datetime) -> dict | None:
        """Juge une proposition. None si elle est ignorée (doublon, périmée...)."""
        key = self._dedup_key(proposal)
        if key in self._seen:
            return None
        self._seen[key] = now

        ts = _parse_ts(proposal.get("timestamp"))
        if ts is not None:
            if ts < self.started_at:
                _info("Proposition antérieure au démarrage du moteur : ignorée "
                      f"({proposal.get('action')} {proposal.get('target')})")
                return None
            if (now - ts).total_seconds() > self.cfg.max_proposal_age_s:
                _warn("Proposition périmée : ignorée "
                      f"({proposal.get('action')} {proposal.get('target')}, {ts.isoformat()})")
                return None

        ctx = Context(now=now, config=self.cfg, history=self.history,
                      incidents=self.incidents, cluster=self.cluster,
                      schema=self.schema, diagnostic=self.diagnostic)
        problems = validate_proposal(proposal, self.cfg.app_namespace)
        if problems:
            checked = ["proposition_valide"]
            violations = [{"invariant": "proposition_valide",
                           "message": "Proposition invalide : " + "; ".join(problems),
                           "evidence": {}}]
        else:
            checked, violations = evaluate(proposal, ctx)

        if violations:
            execution = {"executed": False, "result": "skipped",
                         "error": None, "reason": "DENY"}
        else:
            execution = self.executor.run(proposal)
            if execution["executed"]:
                self.history.record(proposal["target"], proposal["action"], now)
            elif self.cfg.dry_run and execution.get("reason") == "dry-run":
                # Ce qui aurait été exécuté : l'anti-flapping le compte quand même
                self.history.record(proposal["target"], proposal["action"], now,
                                    simulated=True)

        verdict = build_verdict(proposal, checked, violations, ctx.evidence,
                                execution, self.mode, now, ctx.alerts)
        info = verdict["incident"]
        self.incidents.record(info["incident_id"], info["attempt"],
                              verdict["verdict"], now)
        print(VERDICT_PREFIX + json.dumps(verdict), flush=True)
        _info(f"Verdict {verdict['verdict']} → {proposal.get('action')}"
              f"({proposal.get('target')}) : {verdict['justification']}")
        for alert in ctx.alerts:
            cause = alert["cause_racine"]
            last = self._alert_logged.get(cause)
            if last is None or (now - last).total_seconds() >= self.cfg.alert_cooldown_s:
                self._alert_logged[cause] = now
                _critical(f"ALERTE — {alert['message']} Action suggérée : "
                          f"{alert['action_suggeree']}. Services dégradés : "
                          f"{', '.join(alert['services_degrades'])}.")
        return verdict

    def run_cycle(self, now: datetime | None = None) -> list[dict]:
        """Un cycle : lit les propositions, juge les nouvelles, purge la mémoire."""
        now = now or datetime.now(timezone.utc)
        try:
            lines = self.reader.read_lines()
        except ClusterUnavailable as e:
            _warn(f"API Kubernetes indisponible, cycle ignoré : {str(e)[:200]}")
            return []
        verdicts = []
        for line in lines:
            proposal = parse_proposal_line(line)
            if proposal is None:
                continue
            verdict = self.handle(proposal, now)
            if verdict:
                verdicts.append(verdict)
        horizon = now - timedelta(hours=1)
        self._seen = {k: t for k, t in self._seen.items() if t >= horizon}
        return verdicts


# ============================================================
# Démarrage
# ============================================================
def validate_startup(cfg: Config) -> None:
    """Refuse le mode réel tant qu'aucun invariant n'est enregistré."""
    if not cfg.dry_run and not INVARIANTS:
        raise SystemExit(
            "DRY_RUN=false refusé : aucun invariant enregistré, "
            "le moteur autoriserait toutes les actions."
        )


def build_apis():
    """Clients Kubernetes : (core, apps, autoscaling)."""
    from kubernetes import client, config  # importé ici : inutile pour les tests
    try:
        config.load_incluster_config()      # dans le cluster (déploiement final)
    except config.ConfigException:
        config.load_kube_config()           # depuis ton terminal (essais, ~/.kube/config)
    return client.CoreV1Api(), client.AppsV1Api(), client.AutoscalingV2Api()


def check_schema_cli(service: str, version: str) -> None:
    """
    python3 moteur.py --check-schema enrollment-service 1.6
    Vérifie la connexion à la base et la compatibilité, sans lancer le moteur.
    """
    cfg = Config.from_env()
    if not (cfg.db_user and cfg.db_password):
        raise SystemExit("DB_USER et DB_PASSWORD doivent être définis.")
    try:
        result = check_schema_compat(SchemaReader(cfg), service, version)
    except DataUnavailable as e:
        raise SystemExit(f"Impossible de conclure : {e}")
    verdict = "compatible → rollback autorisé" if result["compatible"] \
        else "INCOMPATIBLE → rollback refusé"
    print(f"{service} {version} : schéma actuel {result['current']}, "
          f"plage supportée {result['min']} à {result['max']} : {verdict}")


_heartbeat_warned = False


def touch_heartbeat(path: str) -> None:
    """Marque la boucle comme vivante (lu par la sonde liveness)."""
    global _heartbeat_warned
    try:
        with open(path, "a"):
            os.utime(path, None)
    except OSError as e:
        if not _heartbeat_warned:   # une seule alerte, pas une par cycle
            _heartbeat_warned = True
            _warn(f"Fichier de vie inaccessible ({path}) : {e}")


def run_forever(engine: "Engine", cfg: Config, stop: threading.Event) -> None:
    """
    Boucle principale. S'arrête proprement quand `stop` est levé : le cycle en
    cours (et l'action éventuelle) va jusqu'au bout, puis la boucle sort.
    Une erreur dans un cycle ne l'arrête jamais.
    """
    touch_heartbeat(cfg.heartbeat_file)
    while not stop.is_set():
        try:
            engine.run_cycle()
        except Exception as e:
            _error(f"Erreur inattendue dans la boucle : {e}")
        touch_heartbeat(cfg.heartbeat_file)
        stop.wait(cfg.poll_interval_s)


def main() -> None:
    cfg = Config.from_env()
    setup_logging(cfg.log_level)
    validate_startup(cfg)
    core, apps, autoscaling = build_apis()
    cluster = ClusterState(apps, autoscaling, cfg.app_namespace, cfg.api_timeout_s)
    schema = SchemaReader(cfg) if (cfg.db_user and cfg.db_password) else None
    if schema is None:
        _warn("DB_USER / DB_PASSWORD absents : tout Rollback sera refusé "
              "(invariant 2 ne peut pas lire le schéma)")
    diagnostic = Diagnostician(Prometheus(cfg.prometheus_url, cfg.api_timeout_s), cluster, cfg)
    executor = Executor(cfg.dry_run, apps, cluster, cfg)
    engine = Engine(cfg, DecideurLogReader(core, cfg), executor=executor, cluster=cluster,
                    schema=schema, diagnostic=diagnostic)
    _info(f"Démarrage du moteur en mode {engine.mode} — polling toutes les "
          f"{cfg.poll_interval_s}s, {len(INVARIANTS)} invariant(s) enregistré(s) : "
          f"{', '.join(n for n, _ in INVARIANTS)}")
    # Kubernetes arrête un pod avec SIGTERM ; sans gestionnaire, un processus
    # PID 1 l'ignore et se fait tuer au bout du délai de grâce.
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda signum, frame: stop.set())
    run_forever(engine, cfg, stop)
    _info("Arrêt du moteur")


if __name__ == "__main__":
    import sys
    if len(sys.argv) == 4 and sys.argv[1] == "--check-schema":
        check_schema_cli(sys.argv[2], sys.argv[3])
    else:
        main()
