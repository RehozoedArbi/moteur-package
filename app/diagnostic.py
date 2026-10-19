"""
diagnostic.py — Détection de dégradation et cause racine (étapes 6 et 7)
========================================================================
Répond à deux questions pour le moteur :
1. Quels services sont dégradés en ce moment ? (étape 6)
2. Si un service est dégradé, est-il la cause ou un symptôme ? (étape 7)

Détection de dégradation. Un service est dégradé si :
- aucun de ses réplicas n'est prêt (API Kubernetes) ;
- OU son taux de 5xx dépasse error_rate_degraded ;
- OU sa P95 dépasse un seuil. Ce seuil vient de deux sources :
    * la BASELINE observée dans l'historique de Prometheus : seuls les
      échantillons SAINS comptent (réplicas au minimum, P95 sous le plafond
      fixe, peu d'erreurs, trafic présent), donc une panne, même longue, ne
      peut jamais devenir « normale ». Seuil = max(3 × P95 haute saine, plancher).
    * des SEUILS FIXES de repli (FIXED_P95_S, mesurés sur ton cluster) tant que
      l'historique sain est insuffisant (cluster récent, pas de trafic k6...).
  Le diagnostic dit toujours laquelle des deux sources il a utilisée.

Cause racine. Les dépendances viennent de la topologie DÉCLARÉE, complétée par
les arêtes OBSERVÉES dans le graphe de dépendances (traces_service_graph_*)
dont la part d'appels par requête est assez forte : une arête rare (comme
student → enrollment, 0,1 req/s) n'est pas une dépendance. La base de données
est un nœud « postgres » : il est dégradé si les latences des arêtes vers la
base sont dégradées pour AU MOINS db_min_clients services à la fois (un seul
service lent vers la base s'explique par ce service, pas par Postgres).
La cause racine d'un service dégradé est le nœud dégradé le plus en aval
parmi ses dépendances. Un service qui a une dépendance dégradée est un
symptôme. Un cycle entre nœuds dégradés rend la cause ambiguë : on refuse.

Le moteur n'utilise pas l'ordre d'apparition des anomalies : la direction des
dépendances suffit ici, et à 30 s de résolution avec des fenêtres de 2 minutes,
l'ordre est trop grossier pour trancher.
"""

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable

# ============================================================
# Topologie et métriques
# ============================================================
SERVICES = ("student-service", "enrollment-service", "teacher-admin-service")
DB_NODE = "postgres"
DB_GRAPH_NAME = "university_db"   # nom du serveur « base » dans le graphe de dépendances

APP_METRICS = {
    "flask": {"bucket": "flask_http_request_duration_seconds_bucket",
              "count": "flask_http_request_total",
              "jobs": "student-service|enrollment-service"},
    "spring": {"bucket": "http_server_requests_seconds_bucket",
               "count": "http_server_requests_seconds_count",
               "jobs": "teacher-admin-service"},
}

# Seuils fixes de repli (secondes) : P95 « dégradé » = max(3 × haut de la plage
# saine mesurée, plancher). teacher-admin a une marge plus large (max sain : 38 ms).
FIXED_P95_S = {"student-service": 0.05,
               "enrollment-service": 0.155,
               "teacher-admin-service": 0.08}
FIXED_DB_EDGE_P95_S = 0.1

# Dépendances fortes connues (mesurées : enrollment appelle student 0,5 fois
# et teacher-admin 2,9 fois par requête ; student → enrollment et student →
# teacher-admin sont des arêtes marginales, volontairement absentes).
DECLARED_DEPENDENCIES = {
    "enrollment-service": ("student-service", "teacher-admin-service", DB_NODE),
    "student-service": (DB_NODE,),
    "teacher-admin-service": (DB_NODE,),
}

Q_P95 = ('histogram_quantile(0.95, sum by (job, le) '
         '(rate({bucket}{{job=~"{jobs}"}}[{w}])))')
Q_RATE = 'sum by (job) (rate({count}{{job=~"{jobs}"}}[{w}]))'
Q_RATE5 = 'sum by (job) (rate({count}{{job=~"{jobs}", status=~"5.."}}[{w}]))'
Q_DB_P95 = ('histogram_quantile(0.95, sum by (client, le) (rate('
            'traces_service_graph_request_client_seconds_bucket{{server="{db}"}}[{w}])))')
Q_EDGES = ('sum by (client, server) '
           '(rate(traces_service_graph_request_total{{failed="false"}}[{w}]))')
Q_READY = 'kube_deployment_status_replicas_ready{{namespace="{ns}", deployment=~"{deps}"}}'


class DiagnosticUnavailable(Exception):
    """Le diagnostic ne peut pas conclure (Prometheus muet, données absentes...)."""


# ============================================================
# Client Prometheus minimal
# ============================================================
class Prometheus:
    """Requêtes instantanées et sur intervalle. `http_get` est injectable pour les tests."""

    def __init__(self, url: str, timeout_s: int = 5, http_get: Callable | None = None):
        self.url = url.rstrip("/")
        self.timeout = timeout_s
        self._get = http_get or self._default_get

    def _default_get(self, path: str, params: dict) -> dict:
        import requests  # importé ici : inutile pour les tests
        r = requests.get(f"{self.url}{path}", params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _call(self, path: str, params: dict) -> list:
        try:
            body = self._get(path, params)
        except Exception as e:
            raise DiagnosticUnavailable(f"Prometheus injoignable : {e}") from e
        if body.get("status") != "success":
            raise DiagnosticUnavailable(
                f"Prometheus a répondu en erreur : {body.get('error', body.get('status'))}")
        return body.get("data", {}).get("result", [])

    def instant(self, promql: str) -> list:
        return self._call("/api/v1/query", {"query": promql})

    def range(self, promql: str, start: int, end: int, step: int) -> list:
        return self._call("/api/v1/query_range",
                          {"query": promql, "start": start, "end": end, "step": step})


def _num(value) -> float | None:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _key(metric: dict, labels: tuple):
    return metric.get(labels[0]) if len(labels) == 1 else tuple(metric.get(l) for l in labels)


def _inst_map(result: list, *labels: str) -> dict:
    out = {}
    for r in result:
        v = _num(r["value"][1])
        if v is not None:
            out[_key(r["metric"], labels)] = v
    return out


def _range_map(result: list, *labels: str) -> dict:
    out = {}
    for r in result:
        series = {}
        for ts, raw in r["values"]:
            v = _num(raw)
            if v is not None:
                series[int(round(float(ts)))] = v
        out[_key(r["metric"], labels)] = series
    return out


def _quantile(values: list, q: float) -> float:
    v = sorted(values)
    pos = (len(v) - 1) * q
    lo, hi = int(math.floor(pos)), int(math.ceil(pos))
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


def _templates(template: str, window: str, **extra):
    for fam in APP_METRICS.values():
        yield template.format(bucket=fam["bucket"], count=fam["count"],
                              jobs=fam["jobs"], w=window, **extra)


def _inst_by_job(prom, template: str, window: str) -> dict:
    out = {}
    for q in _templates(template, window):
        out.update(_inst_map(prom.instant(q), "job"))
    return out


def _range_by_job(prom, template: str, window: str, start: int, end: int, step: int) -> dict:
    out = {}
    for q in _templates(template, window):
        out.update(_range_map(prom.range(q, start, end, step), "job"))
    return out


# ============================================================
# Baseline (historique sain)
# ============================================================
@dataclass
class Baseline:
    ok: bool
    samples: int
    needed: int
    p95_thresholds: dict = field(default_factory=dict)     # service -> s
    db_thresholds: dict = field(default_factory=dict)      # client -> s
    observed_deps: dict = field(default_factory=dict)      # client -> {dépendance: part d'appels}


def compute_baseline(prom, cfg, now: datetime, min_ready: dict) -> Baseline:
    """
    Baseline sur [now - gap - fenêtre, now - gap], échantillons sains uniquement.
    min_ready : réplicas prêts minimum par service (minimum du HPA).
    """
    end_dt = now - timedelta(seconds=cfg.baseline_gap_s)
    end = int(end_dt.timestamp())
    start = int((end_dt - timedelta(seconds=cfg.baseline_window_s)).timestamp())
    step, w = cfg.baseline_step_s, cfg.rate_window

    p95 = _range_by_job(prom, Q_P95, w, start, end, step)
    rate = _range_by_job(prom, Q_RATE, w, start, end, step)
    rate5 = _range_by_job(prom, Q_RATE5, w, start, end, step)
    ready = _range_map(prom.range(
        Q_READY.format(ns=cfg.app_namespace, deps="|".join(SERVICES)), start, end, step),
        "deployment")
    db_p95 = _range_map(prom.range(Q_DB_P95.format(db=DB_GRAPH_NAME, w=w), start, end, step),
                        "client")
    edges = _range_map(prom.range(Q_EDGES.format(w=w), start, end, step), "client", "server")

    def healthy(ts: int) -> bool:
        for svc in SERVICES:
            r = ready.get(svc, {}).get(ts)
            total = rate.get(svc, {}).get(ts)
            p = p95.get(svc, {}).get(ts)
            if r is None or total is None or p is None:
                return False
            errors = rate5.get(svc, {}).get(ts) or 0.0
            if (r < min_ready.get(svc, 1) or total < cfg.healthy_min_rate
                    or p > FIXED_P95_S[svc] or errors / total >= cfg.healthy_error_rate):
                return False
        return True

    grid = sorted(set().union(*(set(p95.get(s, {})) for s in SERVICES)))
    good = [ts for ts in grid if healthy(ts)]
    if len(good) < cfg.baseline_min_samples:
        return Baseline(ok=False, samples=len(good), needed=cfg.baseline_min_samples)

    p95_thr = {s: max(3 * _quantile([p95[s][t] for t in good], 0.95), cfg.p95_floor_s)
               for s in SERVICES}
    db_thr = {}
    for client, series in db_p95.items():
        vals = [series[t] for t in good if t in series]
        if client in SERVICES and len(vals) >= cfg.baseline_min_samples // 2:
            db_thr[client] = max(3 * _quantile(vals, 0.95), cfg.p95_floor_s)

    observed: dict = {}
    for (client, server), series in edges.items():
        if client not in SERVICES or server not in SERVICES + (DB_GRAPH_NAME,):
            continue
        shares = [series[t] / rate[client][t] for t in good
                  if t in series and rate.get(client, {}).get(t, 0) > 0]
        if shares and _quantile(shares, 0.5) >= cfg.dep_min_share:
            node = DB_NODE if server == DB_GRAPH_NAME else server
            observed.setdefault(client, {})[node] = round(_quantile(shares, 0.5), 3)
    return Baseline(ok=True, samples=len(good), needed=cfg.baseline_min_samples,
                    p95_thresholds=p95_thr, db_thresholds=db_thr, observed_deps=observed)


# ============================================================
# Diagnostic
# ============================================================
@dataclass
class Diagnosis:
    source: str
    services: dict          # service -> {degrade, raisons, p95_ms, seuil_ms, ready, spec_replicas, taux_5xx}
    db: dict                # {degrade, clients: {client: {p95_ms, seuil_ms, degrade}}}
    deps: dict              # noeud -> [dépendances]
    degraded_nodes: set

    def roots_for(self, node: str) -> set:
        """Nœuds dégradés les plus en aval à partir de `node` (lui-même s'il n'a aucune dépendance dégradée)."""
        bad = self.degraded_nodes

        def walk(current: str, path: frozenset) -> set:
            bad_deps = [d for d in self.deps.get(current, ()) if d in bad]
            if not bad_deps:
                return {current}
            roots: set = set()
            for dep in bad_deps:
                if dep == current or dep in path:
                    raise DiagnosticUnavailable(
                        f"dépendance circulaire entre {current} et {dep} : cause ambiguë")
                roots |= walk(dep, path | {current})
            return roots

        return walk(node, frozenset())

    def summary(self, roots: set | None = None) -> dict:
        out = {"source_seuils": self.source, "services": self.services,
               "base_de_donnees": self.db,
               "dependances": {k: list(v) for k, v in self.deps.items()},
               "noeuds_degrades": sorted(self.degraded_nodes)}
        if roots is not None:
            out["causes_racines"] = sorted(roots)
        return out


class Diagnostician:
    """Construit un Diagnosis à partir de Prometheus (instantané + baseline) et de l'API Kubernetes."""

    def __init__(self, prom, cluster, cfg):
        self.prom, self.cluster, self.cfg = prom, cluster, cfg
        self._baseline: Baseline | None = None
        self._baseline_at: datetime | None = None
        self._cache: tuple | None = None

    def diagnose(self, now: datetime) -> Diagnosis:
        # Même diagnostic pour toutes les propositions d'un même cycle
        if self._cache and abs((now - self._cache[0]).total_seconds()) < self.cfg.diagnosis_cache_s:
            return self._cache[1]
        states = {s: self.cluster.snapshot(s) for s in SERVICES}
        baseline = self._get_baseline(now, states)
        diagnosis = self._build(states, baseline)
        self._cache = (now, diagnosis)
        return diagnosis

    def _get_baseline(self, now: datetime, states: dict) -> Baseline:
        if (self._baseline is not None and self._baseline_at is not None
                and (now - self._baseline_at).total_seconds() < self.cfg.baseline_refresh_s):
            return self._baseline
        min_ready = {s: max(1, states[s]["hpa_min"] or 1) for s in SERVICES}
        self._baseline = compute_baseline(self.prom, self.cfg, now, min_ready)
        self._baseline_at = now
        return self._baseline

    def _build(self, states: dict, baseline: Baseline) -> Diagnosis:
        cfg, w = self.cfg, self.cfg.rate_window
        p95 = _inst_by_job(self.prom, Q_P95, w)
        rate = _inst_by_job(self.prom, Q_RATE, w)
        rate5 = _inst_by_job(self.prom, Q_RATE5, w)
        db_p95 = _inst_map(self.prom.instant(Q_DB_P95.format(db=DB_GRAPH_NAME, w=w)), "client")

        if baseline.ok:
            source = (f"baseline observée ({baseline.samples} points sains sur "
                      f"{cfg.baseline_window_s // 60} min)")
            thresholds = baseline.p95_thresholds
        else:
            source = (f"seuils fixes de démarrage (historique sain insuffisant : "
                      f"{baseline.samples}/{baseline.needed} points)")
            thresholds = FIXED_P95_S

        services, degraded = {}, set()
        for svc in SERVICES:
            st = states[svc]
            total = rate.get(svc)
            err = ((rate5.get(svc) or 0.0) / total
                   if total is not None and total >= cfg.min_request_rate else None)
            thr, latency = thresholds[svc], p95.get(svc)
            reasons = []
            if st["ready"] == 0:
                reasons.append("aucun réplica prêt")
            if err is not None and err > cfg.error_rate_degraded:
                reasons.append(f"taux de 5xx {err:.0%}")
            if latency is not None and latency > thr:
                reasons.append(f"P95 {latency * 1000:.0f} ms > seuil {thr * 1000:.0f} ms")
            if reasons:
                degraded.add(svc)
            services[svc] = {"degrade": bool(reasons), "raisons": reasons,
                             "p95_ms": None if latency is None else round(latency * 1000, 1),
                             "seuil_ms": round(thr * 1000, 1), "ready": st["ready"],
                             "spec_replicas": st["spec_replicas"],
                             "taux_5xx": None if err is None else round(err, 3)}

        clients = {}
        for client, latency in db_p95.items():
            if client not in SERVICES:
                continue
            thr = baseline.db_thresholds.get(client, FIXED_DB_EDGE_P95_S) if baseline.ok \
                else FIXED_DB_EDGE_P95_S
            clients[client] = {"p95_ms": round(latency * 1000, 1), "seuil_ms": round(thr * 1000, 1),
                               "degrade": latency > thr}
        if not clients:
            raise DiagnosticUnavailable(
                "latences des arêtes vers la base absentes : cause racine non vérifiable")
        db_degraded = sum(c["degrade"] for c in clients.values()) >= cfg.db_min_clients
        if db_degraded:
            degraded.add(DB_NODE)

        deps = {k: list(v) for k, v in DECLARED_DEPENDENCIES.items()}
        if baseline.ok:
            for client, found in baseline.observed_deps.items():
                for dep in found:
                    if dep not in deps.setdefault(client, []):
                        deps[client].append(dep)
        return Diagnosis(source=source, services=services,
                         db={"degrade": db_degraded, "clients": clients},
                         deps=deps, degraded_nodes=degraded)
