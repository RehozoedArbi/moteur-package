"""Tests du squelette du moteur. Lancer : python3 test_moteur.py"""
import io
import json
import contextlib
from datetime import datetime, timezone, timedelta

import moteur as m

# Les tests du squelette tournent sans invariant ; les vrais sont testés plus bas.
REAL = list(m.INVARIANTS)
m.INVARIANTS[:] = []

OK = True


def check(label, got, want):
    global OK
    good = got == want
    OK &= good
    print(("OK  " if good else "FAIL"), label, "->", got if not good else "")


NOW = datetime(2026, 10, 14, 12, 0, 0, tzinfo=timezone.utc)
START = NOW - timedelta(minutes=10)


def proposal(action="Restart", target="student-service", age_s=5, incident=None, attempt=None, **extra):
    details = dict(extra)
    if incident:
        details.update(incident_id=incident, attempt=attempt, max_attempts=2)
    return {"timestamp": (NOW - timedelta(seconds=age_s)).isoformat(), "source": "decideur-naif",
            "action": action, "target": target, "namespace": "university-app",
            "reason": "test", "details": details}


def line(p):
    return "ACTION_PROPOSAL:" + json.dumps(p)


class FakeReader:
    def __init__(self, lines=None, fail=False):
        self.lines, self.fail = lines or [], fail

    def read_lines(self):
        if self.fail:
            raise m.ClusterUnavailable("api down")
        return list(self.lines)


def engine(lines=None, dry_run=True, fail=False):
    cfg = m.Config(dry_run=dry_run)
    return m.Engine(cfg, FakeReader(lines, fail), started_at=START)


def run(eng, now=NOW):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        verdicts = eng.run_cycle(now)
    return verdicts, buf.getvalue()


def with_registry(entries):
    """Remplace temporairement le registre d'invariants."""
    saved = list(m.INVARIANTS)
    m.INVARIANTS[:] = entries
    return saved


def restore(saved):
    m.INVARIANTS[:] = saved


# --- lecture des lignes ---
check("ligne normale ignorée", m.parse_proposal_line('{"level": "INFO", "message": "Cycle OK"}'), None)
check("JSON illisible ignoré", m.parse_proposal_line('ACTION_PROPOSAL:{pas du json'), None)
check("proposition lue", m.parse_proposal_line(line(proposal()))["action"], "Restart")

# --- sans invariant, en dry-run : ALLOW mais rien d'exécuté ---
v, out = run(engine([line(proposal())]))
check("1 verdict", len(v), 1)
check("ALLOW sans invariant", v[0]["verdict"], "ALLOW")
check("dry-run : rien exécuté", (v[0]["execution"]["executed"], v[0]["execution"]["reason"]), (False, "dry-run"))
check("verdict imprimé", out.startswith("VERDICT:"), True)
check("clés du verdict", sorted(v[0]), sorted(["timestamp", "proposal", "verdict", "invariant_checked",
      "invariant_violated", "justification", "violations", "evidence", "alerts", "incident", "mode", "execution"]))

# --- déduplication ---
e = engine([line(proposal(incident="i1", attempt=1))] * 2)
check("doublon dans un cycle", len(run(e)[0]), 1)
check("relecture au cycle suivant", len(run(e)[0]), 0)
e = engine([line(proposal(incident="i1", attempt=1)), line(proposal(incident="i1", attempt=2))])
check("tentatives 1 et 2 = 2 verdicts", len(run(e)[0]), 2)
check("incident regroupé", len(e.incidents.history("i1")), 2)

# --- propositions ignorées ---
check("périmée (200 s) ignorée", len(run(engine([line(proposal(age_s=200))]))[0]), 0)
check("antérieure au démarrage ignorée", len(run(engine([line(proposal(age_s=700))]))[0]), 0)

# --- proposition invalide = DENY ---
v, _ = run(engine([line(proposal(action="FormaterLeCluster"))]))
check("action inconnue refusée", (v[0]["verdict"], v[0]["invariant_violated"]), ("DENY", ["proposition_valide"]))
v, _ = run(engine([line(proposal(target="frontend"))]))
check("cible inconnue refusée", v[0]["verdict"], "DENY")

# --- invariants : pas d'arrêt à la première violation, erreurs = refus ---
saved = with_registry([
    ("inv_ok", lambda p, c: None),
    ("inv_quorum", lambda p, c: {"message": "quorum insuffisant", "evidence": {"ready": 1}}),
    ("inv_schema", lambda p, c: {"message": "schéma incompatible"}),
])
v, _ = run(engine([line(proposal())]))
restore(saved)
check("2 violations dans le même verdict", v[0]["invariant_violated"], ["inv_quorum", "inv_schema"])
check("3 invariants vérifiés", len(v[0]["invariant_checked"]), 3)
check("justification cumulée", v[0]["justification"], "quorum insuffisant | schéma incompatible")
check("exécution sautée sur DENY", v[0]["execution"]["reason"], "DENY")

def boom(p, c): raise RuntimeError("bug")
def no_data(p, c): raise m.DataUnavailable("Prometheus muet")
saved = with_registry([("inv_boom", boom), ("inv_nodata", no_data)])
v, _ = run(engine([line(proposal())]))
restore(saved)
check("erreur d'invariant = DENY", v[0]["verdict"], "DENY")
check("erreur et données manquantes listées", v[0]["invariant_violated"], ["inv_boom", "inv_nodata"])

# --- historique : n'enregistre que ce qui est exécuté ---
class FakeExecutor:
    dry_run = False
    def run(self, p): return {"executed": True, "result": "success", "error": None}
e = m.Engine(m.Config(dry_run=False), FakeReader([line(proposal())]), FakeExecutor(), START)
saved = with_registry([("inv_ok", lambda p, c: None)])
v, _ = run(e)
restore(saved)
check("ALLOW exécuté enregistré", e.history.last("student-service", "Restart"), NOW)
saved = with_registry([("inv_non", lambda p, c: {"message": "non"})])
e2 = m.Engine(m.Config(dry_run=False), FakeReader([line(proposal())]), FakeExecutor(), START)
run(e2)
restore(saved)
check("DENY non enregistré", e2.history.last("student-service", "Restart"), None)

# --- panne de l'API Kubernetes : pas de crash ---
check("API indisponible : cycle ignoré", run(engine(fail=True))[0], [])

# --- démarrage ---
try:
    m.validate_startup(m.Config(dry_run=False))
    refused = False
except SystemExit:
    refused = True
check("mode réel sans invariant refusé", refused, True)
m.validate_startup(m.Config(dry_run=True))
check("dry-run sans invariant accepté", True, True)

# --- lecteur de logs avec un faux client Kubernetes ---
class Meta:  name = "decideur-abc"
class Pod:   metadata = Meta()
class Pods:  items = [Pod()]
class FakeCore:
    def __init__(self, fail=False): self.fail, self.calls = fail, []
    def list_namespaced_pod(self, ns, label_selector=None, _request_timeout=None):
        if self.fail: raise RuntimeError("401")
        self.calls.append((ns, label_selector)); return Pods()
    def read_namespaced_pod_log(self, name, namespace, since_seconds, _request_timeout=None):
        return "bruit\n" + line(proposal()) + "\nautre bruit"
cfg = m.Config()
core = FakeCore()
lines = m.DecideurLogReader(core, cfg).read_lines()
check("lecture des logs du décideur", (len(lines), core.calls), (3, [("university-decideur", "app.kubernetes.io/name=decideur")]))
try:
    m.DecideurLogReader(FakeCore(fail=True), cfg).read_lines(); raised = False
except m.ClusterUnavailable:
    raised = True
check("erreur d'API -> ClusterUnavailable", raised, True)


# =====================================================================
# Étapes 2 et 3 : état du cluster et invariants 1, 4, 5
# =====================================================================
from types import SimpleNamespace as NS


def state(ready=2, spec=2, hmin=2, hmax=4):
    return {"ready": ready, "spec_replicas": spec, "hpa_min": hmin, "hpa_max": hmax}


class FakeCluster:
    def __init__(self, states=None, fail=False):
        self.states, self.fail = states or {}, fail

    def snapshot(self, service):
        if self.fail:
            raise m.ClusterUnavailable("api down")
        if service not in m.WORKLOAD_SERVICES:
            raise m.DataUnavailable(f"{service} n'est pas un Deployment")
        return self.states[service]


class HealthyDiagnosis:
    degraded_nodes = set()
    def summary(self, roots=None): return {"stub": "tout est sain"}

class HealthyDiagnostic:
    def diagnose(self, now): return HealthyDiagnosis()

HEALTHY = HealthyDiagnostic()


def judge(p, cluster, schema=None, diagnostic=HEALTHY):
    """Verdict d'une proposition avec les VRAIS invariants."""
    saved = with_registry(REAL)
    try:
        e = m.Engine(m.Config(dry_run=True), FakeReader([line(p)]), started_at=START,
                     cluster=cluster, schema=schema, diagnostic=diagnostic)
        v, _ = run(e)
    finally:
        restore(saved)
    return v[0]


check("invariants enregistrés", [n for n, _ in REAL],
      ["invariant1_quorum", "invariant2_schema", "invariant3_anti_flapping", "invariant4_hpa_max", "invariant5_hpa_min",
       "invariant6_cause_racine"])

# --- invariant 1 : quorum (scénarios 1, 6, 7) ---
v = judge(proposal("Restart", "enrollment-service"), FakeCluster({"enrollment-service": state(ready=0)}))
check("S1 : Restart avec ready=0 refusé", (v["verdict"], v["invariant_violated"]), ("DENY", ["invariant1_quorum"]))
check("S1 : message", "aucun réplica actif" in v["justification"], True)
check("S1 : valeurs mesurées dans le verdict", v["violations"][0]["evidence"]["ready"], 0)
v = judge(proposal("Restart", "enrollment-service"), FakeCluster({"enrollment-service": state(ready=1)}))
check("Restart avec ready=1 refusé", (v["verdict"], "1 réplica prêt" in v["justification"]), ("DENY", True))
v = judge(proposal("Restart", "student-service"), FakeCluster({"student-service": state(ready=2)}))
check("Restart avec ready=2 autorisé", (v["verdict"], v["invariant_checked"]), ("ALLOW", ["invariant1_quorum", "invariant3_anti_flapping", "invariant6_cause_racine"]))
check("preuves conservées sur ALLOW", v["evidence"]["invariant1_quorum"]["ready"], 2)

# --- invariant 4 : HPA au maximum (scénario 4) ---
v = judge(proposal("ScaleUp", "teacher-admin-service"), FakeCluster({"teacher-admin-service": state(spec=4, hmax=4)}))
check("S4 : ScaleUp à 4/4 refusé", (v["verdict"], v["invariant_violated"]), ("DENY", ["invariant4_hpa_max"]))
check("S4 : message", "(4/4)" in v["justification"], True)
v = judge(proposal("ScaleUp", "teacher-admin-service"), FakeCluster({"teacher-admin-service": state(spec=3, hmax=4)}))
check("ScaleUp à 3/4 autorisé", v["verdict"], "ALLOW")
v = judge(proposal("ScaleUp", "teacher-admin-service"), FakeCluster({"teacher-admin-service": state(spec=2, hmin=None, hmax=None)}))
check("ScaleUp sans HPA autorisé", v["verdict"], "ALLOW")
check("invariants non concernés absents (1 et 5 hors sujet)", v["invariant_checked"], ["invariant3_anti_flapping", "invariant4_hpa_max"])

# --- invariant 5 : HPA au minimum (scénario 5) ---
v = judge(proposal("ScaleDown", "enrollment-service"), FakeCluster({"enrollment-service": state(spec=2, hmin=2)}))
check("S5 : ScaleDown à 2/2 refusé", (v["verdict"], v["invariant_violated"]), ("DENY", ["invariant5_hpa_min"]))
check("S5 : message", "(2/2)" in v["justification"], True)
v = judge(proposal("ScaleDown", "enrollment-service"), FakeCluster({"enrollment-service": state(spec=3, hmin=2)}))
check("ScaleDown à 3 (min 2) autorisé", v["verdict"], "ALLOW")
v = judge(proposal("ScaleDown", "enrollment-service"), FakeCluster({"enrollment-service": state(spec=1, hmin=None, hmax=None)}))
check("sans HPA : jamais sous 1 réplica", v["verdict"], "DENY")
v = judge(proposal("ScaleDown", "enrollment-service"), FakeCluster({"enrollment-service": state(spec=2, hmin=None, hmax=None)}))
check("sans HPA : 2 -> 1 autorisé", v["verdict"], "ALLOW")

# --- données manquantes = refus ---
v = judge(proposal("Restart", "student-service"), FakeCluster(fail=True))
check("API indisponible : DENY", (v["verdict"], "Données insuffisantes" in v["justification"]), ("DENY", True))
v = judge(proposal("ScaleDown", "student-service"), None)
check("lecture du cluster non configurée : DENY", v["verdict"], "DENY")
v = judge(proposal("Restart", "postgres"), FakeCluster())
check("Restart(postgres) non géré : DENY", v["verdict"], "DENY")
v = judge(proposal("InvestigateDB", "postgres"), FakeCluster())
check("InvestigateDB : aucun invariant ne s'applique", (v["verdict"], v["invariant_checked"]), ("ALLOW", []))

# --- namespace inconnu = refus ---
p = proposal("Restart", "student-service"); p["namespace"] = "kube-system"
v = judge(p, FakeCluster({"student-service": state()}))
check("namespace étranger refusé", (v["verdict"], v["invariant_violated"]), ("DENY", ["proposition_valide"]))

# --- ClusterState réel avec de faux clients Kubernetes ---
class ApiError(Exception):
    def __init__(self, status): self.status = status

def dep(spec=2, ready=2):
    return NS(spec=NS(replicas=spec), status=NS(ready_replicas=ready))

def hpa_obj(mn=2, mx=4):
    return NS(spec=NS(min_replicas=mn, max_replicas=mx))

class FakeApps:
    def __init__(self, d=None, err=None): self.d, self.err = d, err
    def read_namespaced_deployment(self, name, ns, _request_timeout=None):
        if self.err: raise self.err
        return self.d

class FakeAuto:
    def __init__(self, h=None, err=None): self.h, self.err = h, err
    def read_namespaced_horizontal_pod_autoscaler(self, name, ns, _request_timeout=None):
        if self.err: raise self.err
        return self.h

cs = m.ClusterState(FakeApps(dep(3, 1)), FakeAuto(hpa_obj(2, 4)), "university-app", 5)
check("snapshot normal", cs.snapshot("enrollment-service"),
      {"spec_replicas": 3, "ready": 1, "hpa_min": 2, "hpa_max": 4})
cs = m.ClusterState(FakeApps(dep(2, None)), FakeAuto(err=ApiError(404)), "university-app", 5)
check("ready_replicas absent = 0, HPA 404 = pas de HPA", cs.snapshot("student-service"),
      {"spec_replicas": 2, "ready": 0, "hpa_min": None, "hpa_max": None})
cs = m.ClusterState(FakeApps(dep()), FakeAuto(hpa_obj(None, 4)), "university-app", 5)
check("min_replicas absent = 1 (défaut Kubernetes)", cs.snapshot("student-service")["hpa_min"], 1)
for label, apps, auto in (("Deployment", FakeApps(err=ApiError(500)), FakeAuto(hpa_obj())),
                          ("HPA", FakeApps(dep()), FakeAuto(err=ApiError(500)))):
    try:
        m.ClusterState(apps, auto, "university-app", 5).snapshot("student-service"); raised = False
    except m.ClusterUnavailable:
        raised = True
    check(f"erreur API sur le {label} -> ClusterUnavailable", raised, True)
try:
    m.ClusterState(FakeApps(dep()), FakeAuto(hpa_obj()), "university-app", 5).snapshot("postgres"); raised = False
except m.DataUnavailable:
    raised = True
check("cible hors Deployments -> DataUnavailable", raised, True)


# =====================================================================
# Étape 4 : invariant 3 (anti-flapping)
# =====================================================================
def two_cycles(first, second, gap_s, cluster, dry_run=True):
    """Deux cycles espacés de gap_s : la 2e proposition arrive gap_s après la 1re."""
    saved = with_registry(REAL)
    try:
        e = m.Engine(m.Config(dry_run=dry_run), FakeReader([line(first)]), started_at=START,
                     cluster=cluster, diagnostic=HEALTHY)
        v1, _ = run(e, NOW)
        e.reader.lines = [line(second)]
        v2, _ = run(e, NOW + timedelta(seconds=gap_s))
    finally:
        restore(saved)
    return v1[0], v2[0]


def later(p, secs):
    """Même proposition, horodatée secs plus tard (tentative suivante)."""
    q = json.loads(json.dumps(p))
    q["timestamp"] = (datetime.fromisoformat(p["timestamp"]) + timedelta(seconds=secs)).isoformat()
    return q


good = FakeCluster({"student-service": state(ready=2), "enrollment-service": state(ready=2),
                    "teacher-admin-service": state(ready=2, spec=2)})

# --- scénario 3 : 1er restart accepté, 2e refusé 60 s plus tard ---
a1 = proposal("Restart", "student-service", incident="i3", attempt=1)
a2 = later(proposal("Restart", "student-service", incident="i3", attempt=2), 60)
v1, v2 = two_cycles(a1, a2, 60, good)
check("S3 : 1re tentative autorisée", v1["verdict"], "ALLOW")
check("S3 : 2e tentative refusée (anti-flapping)", (v2["verdict"], v2["invariant_violated"]), ("DENY", ["invariant3_anti_flapping"]))
check("S3 : message avec les durées", ("il y a 60s" in v2["justification"], "120s" in v2["justification"]), (True, True))
check("S3 : exécution simulée signalée", v2["violations"][0]["evidence"]["simulee"], True)
check("S3 : même incident, 2 tentatives", v2["incident"]["incident_id"], "i3")

# --- fenêtre dépassée : autorisé de nouveau ---
v1, v2 = two_cycles(a1, later(proposal("Restart", "student-service", incident="i3b", attempt=1), 130), 130, good)
check("après 130 s : autorisé", v2["verdict"], "ALLOW")

# --- pas identique = pas de flapping ---
v1, v2 = two_cycles(proposal("Restart", "student-service"),
                    later(proposal("ScaleUp", "student-service"), 30), 30, good)
check("autre action sur le même service : autorisée", v2["verdict"], "ALLOW")
v1, v2 = two_cycles(proposal("Restart", "student-service"),
                    later(proposal("Restart", "enrollment-service"), 30), 30, good)
check("même action sur un autre service : autorisée", v2["verdict"], "ALLOW")

# --- un DENY n'est pas une action : il ne déclenche pas le flapping ---
bad_quorum = FakeCluster({"student-service": state(ready=0)})
v1, v2 = two_cycles(proposal("Restart", "student-service"),
                    later(proposal("Restart", "student-service"), 30), 30, bad_quorum)
check("un refus n'est pas enregistré", (v1["verdict"], v2["invariant_violated"]), ("DENY", ["invariant1_quorum"]))

# --- plusieurs violations dans le même verdict (esprit du scénario 8) ---
class Flip(FakeCluster):
    """Quorum correct au 1er appel, puis tombé : le 2e Restart viole deux invariants."""
    def __init__(self): super().__init__(); self.n = 0
    def snapshot(self, service):
        self.n += 1
        return state(ready=2) if self.n == 1 else state(ready=0)
v1, v2 = two_cycles(proposal("Restart", "student-service"),
                    later(proposal("Restart", "student-service"), 30), 30, Flip())
check("2 violations dans le même verdict", v2["invariant_violated"], ["invariant1_quorum", "invariant3_anti_flapping"])

# --- InvestigateDB : l'anti-flapping ne s'applique pas ---
v1, v2 = two_cycles(proposal("InvestigateDB", "postgres"), later(proposal("InvestigateDB", "postgres"), 10), 10, good)
check("InvestigateDB répété : jamais bloqué", (v1["verdict"], v2["verdict"], v2["invariant_checked"]), ("ALLOW", "ALLOW", []))

# --- exécution réelle : le refus ne dit plus « simulée » ---
class RealExec:
    dry_run = False
    def run(self, p): return {"executed": True, "result": "success", "error": None}
saved = with_registry(REAL)
e = m.Engine(m.Config(dry_run=False), FakeReader([line(a1)]), RealExec(), START, good,
             diagnostic=HEALTHY)
run(e, NOW)
e.reader.lines = [line(a2)]
v, _ = run(e, NOW + timedelta(seconds=60))
restore(saved)
check("mode réel : refus sans mention de simulation", ("simulée" in v[0]["justification"], v[0]["verdict"]), (False, "DENY"))

# --- ActionHistory ---
h = m.ActionHistory()
h.record("student-service", "Restart", NOW, simulated=True)
check("last_entry", h.last_entry("student-service", "Restart"), (NOW, True))
check("last (compatibilité)", h.last("student-service", "Restart"), NOW)
check("aucune action", h.last_entry("student-service", "ScaleUp"), None)


# =====================================================================
# Étape 5 : invariant 2 (compatibilité de schéma)
# =====================================================================
class FakeCursor:
    def __init__(self, db): self.db, self.rows = db, []
    def execute(self, q, params=None):
        if self.db.get("fail_query"):
            raise RuntimeError("permission denied for table alembic_version")
        if "alembic_version" in q:
            self.rows = [(r,) for r in self.db["revisions"]]
        elif "service_compatibility" in q:
            self.rows = self.db["compat"].get(tuple(params), [])
    def fetchall(self): return self.rows

class FakeConn:
    def __init__(self, db): self.db, self.closed = db, False
    def cursor(self): return FakeCursor(self.db)
    def close(self): self.closed = True

COMPAT = {("enrollment-service", "1.6"): [("0001", "0001")],
          ("enrollment-service", "1.7"): [("0001", "0002")]}

def reader_for(revision="0002", compat=COMPAT, **extra):
    db = {"revisions": [revision] if revision else [], "compat": compat, **extra}
    conns = []
    def connect():
        c = FakeConn(db); conns.append(c); return c
    return m.SchemaReader(m.Config(), connect=connect), conns

def rollback(version="1.6", target="enrollment-service", **extra):
    kw = {"current_image": f"docker.io/darbi/x:1.7", "previous_images": [f"docker.io/darbi/x:{version}"]}
    if version is not None:
        kw["target_version"] = version
    kw.update(extra)
    return proposal("Rollback", target, **kw)

ok_cluster = FakeCluster({"enrollment-service": state(ready=2)})

# --- scénario 2 : rollback vers 1.6 alors que le schéma est en 0002 ---
rd, conns = reader_for("0002")
v = judge(rollback("1.6"), ok_cluster, rd)
check("S2 : rollback 1.6 avec schéma 0002 refusé", (v["verdict"], v["invariant_violated"]), ("DENY", ["invariant2_schema"]))
check("S2 : message", ("Rollback vers 1.6 impossible" in v["justification"], "(0002)" in v["justification"], "0001 à 0001" in v["justification"]), (True, True, True))
check("S2 : preuves", v["violations"][0]["evidence"], {"version_cible": "1.6", "schema_actuel": "0002", "plage_supportee": ["0001", "0001"], "compatible": False})
check("connexion fermée", conns[0].closed, True)

# --- contrôles : le moteur ne refuse pas tout ---
rd, _ = reader_for("0002")
check("rollback vers 1.7 (supporte 0002) autorisé", judge(rollback("1.7"), ok_cluster, rd)["verdict"], "ALLOW")
rd, _ = reader_for("0001")
check("rollback vers 1.6 avec schéma 0001 autorisé", judge(rollback("1.6"), ok_cluster, rd)["verdict"], "ALLOW")

# --- données manquantes = refus ---
rd, _ = reader_for("0002")
v = judge(rollback(None), ok_cluster, rd)
check("version cible absente : DENY", (v["verdict"], "absente de la proposition" in v["justification"]), ("DENY", True))
v = judge(rollback("1.5"), ok_cluster, rd)
check("version inconnue de la table : DENY", (v["verdict"], "absente de service_compatibility" in v["justification"]), ("DENY", True))
v = judge(rollback("1.6"), ok_cluster, None)
check("base non configurée : DENY", (v["verdict"], "non configurée" in v["justification"]), ("DENY", True))
v = judge(rollback("1.6", target="student-service"), FakeCluster({"student-service": state()}), rd)
check("service sans schéma versionné : DENY", (v["verdict"], "aucun schéma versionné" in v["justification"]), ("DENY", True))
def boom_connect(): raise ConnectionError("connection refused")
v = judge(rollback("1.6"), ok_cluster, m.SchemaReader(m.Config(), connect=boom_connect))
check("base injoignable : DENY", (v["verdict"], "injoignable" in v["justification"]), ("DENY", True))
rd, conns = reader_for("0002", fail_query=True)
v = judge(rollback("1.6"), ok_cluster, rd)
check("erreur de requête : DENY", (v["verdict"], "lecture du schéma impossible" in v["justification"]), ("DENY", True))
check("connexion fermée même en erreur", conns[0].closed, True)
db_two = {"revisions": ["0001", "0002"], "compat": COMPAT}
v = judge(rollback("1.6"), ok_cluster, m.SchemaReader(m.Config(), connect=lambda: FakeConn(db_two)))
check("2 révisions dans alembic_version : DENY", (v["verdict"], "2 révision(s)" in v["justification"]), ("DENY", True))
v = judge(rollback("1.6"), ok_cluster, reader_for(None)[0])
check("alembic_version vide : DENY", (v["verdict"], "0 révision(s)" in v["justification"]), ("DENY", True))

# --- scénario 8 : quorum fragilisé + schéma incompatible = 2 violations ---
rd, _ = reader_for("0002")
v = judge(rollback("1.6"), FakeCluster({"enrollment-service": state(ready=1)}), rd)
check("S8 : 2 violations dans le même verdict", v["invariant_violated"], ["invariant1_quorum", "invariant2_schema"])
check("S8 : le quorum vise bien le Rollback", "Rollback refusé" in v["violations"][0]["message"], True)
check("S8 : verdict DENY", v["verdict"], "DENY")

# --- l'invariant 2 ne concerne que les rollbacks ---
v = judge(proposal("Restart", "enrollment-service"), ok_cluster, rd)
check("Restart : invariant 2 non applicable", "invariant2_schema" in v["invariant_checked"], False)

# --- comparaison des révisions ---
check("0001 < 0002 < 0010", sorted(["0010", "0002", "0001"], key=m._rev_key), ["0001", "0002", "0010"])
check("numérique avant texte", sorted(["abc", "0003"], key=m._rev_key), ["0003", "abc"])

# --- le mot de passe n'apparaît jamais dans les logs ---
check("mot de passe absent de repr(Config)", "s3cret" in repr(m.Config(db_password="s3cret")), False)

# --- mode --check-schema ---
import os, sys
os.environ["DB_USER"], os.environ["DB_PASSWORD"] = "engine_ro", "x"
real_reader_cls = m.SchemaReader
rd, _ = reader_for("0002")
m.SchemaReader = lambda cfg: rd
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    m.check_schema_cli("enrollment-service", "1.6")
check("--check-schema affiche le verdict", ("INCOMPATIBLE" in buf.getvalue(), "0002" in buf.getvalue()), (True, True))
buf = io.StringIO()
with contextlib.redirect_stdout(buf):
    m.check_schema_cli("enrollment-service", "1.7")
check("--check-schema : version compatible", "compatible → rollback autorisé" in buf.getvalue(), True)
m.SchemaReader = real_reader_cls
del os.environ["DB_USER"], os.environ["DB_PASSWORD"]
try:
    m.check_schema_cli("enrollment-service", "1.6"); raised = False
except SystemExit:
    raised = True
check("--check-schema sans identifiants : arrêt", raised, True)

print("\nTOUS LES TESTS PASSENT" if OK else "\nDES TESTS ÉCHOUENT")
