"""Tests de l'exécuteur (étape 8). Lancer : python3 test_executor.py"""
import contextlib
import io
import json
import logging
import threading
import time
from datetime import datetime, timezone, timedelta

import moteur as m

OK = True


def check(label, got, want):
    global OK
    good = got == want
    OK &= good
    print(("OK  " if good else "FAIL"), label, "->", got if not good else "")


NOW = datetime(2026, 10, 16, 12, 0, 0, tzinfo=timezone.utc)
START = NOW - timedelta(minutes=10)
S, E, T = "student-service", "enrollment-service", "teacher-admin-service"
IMG17 = "docker.io/darbi/university-enrollment-service:1.7"
IMG16 = "docker.io/darbi/university-enrollment-service:1.6"


class ApiError(Exception):
    def __init__(self, status): self.status = status


class FakeApps:
    """Faux AppsV1Api : enregistre les patchs, peut échouer ou ralentir."""
    def __init__(self, fail=False, delay=0.0):
        self.patches, self.fail, self.delay = [], fail, delay
        self.active = self.max_active = 0
        self._lock = threading.Lock()

    def patch_namespaced_deployment(self, name, namespace, body, field_manager=None, _request_timeout=None):
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        time.sleep(self.delay)
        try:
            if self.fail:
                raise ApiError(500)
            self.patches.append({"name": name, "namespace": namespace, "body": body,
                                 "field_manager": field_manager})
        finally:
            with self._lock:
                self.active -= 1


class FakeCluster:
    def __init__(self, spec=2, hmin=2, hmax=4, ready=2, image=IMG17, container=E):
        self.spec, self.hmin, self.hmax, self.ready = spec, hmin, hmax, ready
        self.image, self.container = image, container

    def snapshot(self, service):
        return {"spec_replicas": self.spec, "ready": self.ready,
                "hpa_min": self.hmin, "hpa_max": self.hmax}

    def container_image(self, service):
        return self.container, self.image


def proposal(action, target=E, **details):
    return {"timestamp": (NOW - timedelta(seconds=5)).isoformat(), "source": "decideur-naif",
            "action": action, "target": target, "namespace": "university-app",
            "reason": "test", "details": details}


FIXED_CLOCK = lambda: NOW
CFG_LIVE, CFG_DRY = m.Config(dry_run=False), m.Config(dry_run=True)


def live(cluster=None, apps=None):
    apps = apps if apps is not None else FakeApps()
    return m.Executor(False, apps, cluster or FakeCluster(), CFG_LIVE, FIXED_CLOCK), apps


# ---------------------------------------------------------------------
# Utilitaires d'image
# ---------------------------------------------------------------------
check("tag remplacé", m._with_tag(IMG17, "1.6"), IMG16)
check("port de registre conservé", m._with_tag("localhost:5000/x:1.7", "1.6"), "localhost:5000/x:1.6")
check("digest retiré", m._with_tag("docker.io/darbi/x@sha256:abc", "1.6"), "docker.io/darbi/x:1.6")
check("image sans tag", m._with_tag("darbi/x", "1.6"), "darbi/x:1.6")
check("comparaison sans docker.io", m._norm_image("docker.io/darbi/x:1.6"), m._norm_image("darbi/x:1.6"))

# ---------------------------------------------------------------------
# Restart
# ---------------------------------------------------------------------
ex, apps = live()
r = ex.run(proposal("Restart", S))
check("Restart : succès", (r["executed"], r["result"], r["error"]), (True, "success", None))
check("Restart : annotation restartedAt posée", apps.patches[0]["body"],
      {"spec": {"template": {"metadata": {"annotations": {m.RESTARTED_AT: NOW.isoformat()}}}}})
check("Restart : bon déploiement, bon namespace, field manager", (apps.patches[0]["name"], apps.patches[0]["namespace"], apps.patches[0]["field_manager"]),
      (S, "university-app", "moteur"))
check("Restart : description", r["done"], "rollout restart de deployment/student-service")

# ---------------------------------------------------------------------
# Dry-run : rien ne change, mais on dit ce qui serait fait
# ---------------------------------------------------------------------
apps = FakeApps()
ex = m.Executor(True, apps, FakeCluster(spec=3), CFG_DRY, FIXED_CLOCK)
r = ex.run(proposal("ScaleUp", T))
check("dry-run : rien exécuté", (r["executed"], r["reason"], apps.patches), (False, "dry-run", []))
check("dry-run : décrit ce qui serait fait", r["would_do"], "réplicas de teacher-admin-service : 3 → 4")
r = m.Executor(True, None, None, CFG_DRY).run(proposal("ScaleUp", T))
check("dry-run sans accès au cluster : pas de plantage", (r["reason"], r["would_do"], "plan_error" in r), ("dry-run", None, True))

# ---------------------------------------------------------------------
# ScaleUp / ScaleDown : bornes du HPA relues juste avant d'agir
# ---------------------------------------------------------------------
ex, apps = live(FakeCluster(spec=2, hmax=4))
r = ex.run(proposal("ScaleUp", T))
check("ScaleUp 2 → 3", (r["result"], apps.patches[0]["body"]), ("success", {"spec": {"replicas": 3}}))
ex, apps = live(FakeCluster(spec=4, hmax=4))
r = ex.run(proposal("ScaleUp", T))
check("ScaleUp au maximum : sans objet, aucun patch", (r["executed"], r["result"], apps.patches), (False, "skipped", []))
check("ScaleUp au maximum : raison", "déjà au maximum (4/4)" in r["reason"], True)
ex, apps = live(FakeCluster(spec=2, hmin=None, hmax=None))
check("ScaleUp sans HPA : +1", ex.run(proposal("ScaleUp", T))["result"], "success")

ex, apps = live(FakeCluster(spec=3, hmin=2))
r = ex.run(proposal("ScaleDown", E))
check("ScaleDown 3 → 2", (r["result"], apps.patches[0]["body"]), ("success", {"spec": {"replicas": 2}}))
ex, apps = live(FakeCluster(spec=2, hmin=2))
r = ex.run(proposal("ScaleDown", E))
check("ScaleDown au minimum : sans objet, aucun patch", (r["result"], apps.patches), ("skipped", []))
ex, apps = live(FakeCluster(spec=1, hmin=None, hmax=None))
check("ScaleDown sans HPA : jamais sous 1", ex.run(proposal("ScaleDown", E))["result"], "skipped")

# ---------------------------------------------------------------------
# Rollback : seulement vers l'image précédente observée
# ---------------------------------------------------------------------
details = {"target_version": "1.6", "previous_images": [IMG16], "current_image": IMG17}
ex, apps = live()
r = ex.run(proposal("Rollback", E, **details))
check("Rollback : succès", (r["executed"], r["result"]), (True, "success"))
check("Rollback : patch du conteneur par son nom", apps.patches[0]["body"],
      {"spec": {"template": {"spec": {"containers": [{"name": E, "image": IMG16}]}}}})
check("Rollback : description", r["done"], f"image de {E} : {IMG17} → {IMG16}")

ex, apps = live()
r = ex.run(proposal("Rollback", E, target_version="1.5", previous_images=[IMG16]))
check("Rollback vers une image jamais observée : refusé", (r["result"], apps.patches, "image précédente observée" in r["reason"]), ("skipped", [], True))
ex, apps = live(FakeCluster(image=IMG16))
r = ex.run(proposal("Rollback", E, **details))
check("Rollback vers l'image déjà en place : sans objet", (r["result"], apps.patches), ("skipped", []))
ex, apps = live()
r = ex.run(proposal("Rollback", E, previous_images=[IMG16]))
check("Rollback sans version cible : sans objet", (r["result"], apps.patches, "version cible absente" in r["reason"]), ("skipped", [], True))
ex, apps = live(FakeCluster(image="darbi/university-enrollment-service:1.7"))
r = ex.run(proposal("Rollback", E, **details))
check("Rollback : image du Deployment sans docker.io acceptée", r["result"], "success")

# ---------------------------------------------------------------------
# InvestigateDB : aucune action sur le cluster, une alerte
# ---------------------------------------------------------------------
logbuf = io.StringIO()
handler = logging.StreamHandler(logbuf); m.logger.addHandler(handler)
ex, apps = live()
r = ex.run(proposal("InvestigateDB", "postgres"))
m.logger.removeHandler(handler)
check("InvestigateDB : succès sans patch", (r["executed"], r["result"], apps.patches), (True, "success", []))
check("InvestigateDB : alerte CRITICAL journalisée", "ALERTE InvestigateDB" in logbuf.getvalue(), True)

# ---------------------------------------------------------------------
# Échecs : jamais de plantage, jamais d'exécution enregistrée à tort
# ---------------------------------------------------------------------
ex, apps = live(apps=FakeApps(fail=True))
r = ex.run(proposal("Restart", S))
check("API en erreur : échec signalé", (r["executed"], r["result"], "ApiError" in r["error"]), (False, "failed", True))
r = m.Executor(False, None, FakeCluster(), CFG_LIVE).run(proposal("Restart", S))
check("sans accès à l'API : échec signalé", (r["result"], "non configurée" in r["error"]), ("failed", True))
class BrokenCluster:
    def snapshot(self, s): raise m.ClusterUnavailable("api down")
r = m.Executor(False, FakeApps(), BrokenCluster(), CFG_LIVE).run(proposal("ScaleUp", T))
check("cluster injoignable pendant la préparation : échec, aucun patch", (r["result"], r["executed"]), ("failed", False))
class BuggyCluster:
    def snapshot(self, s): raise KeyError("champ inattendu")
r = m.Executor(False, FakeApps(), BuggyCluster(), CFG_LIVE).run(proposal("ScaleUp", T))
check("erreur imprévue pendant la préparation : échec propre", (r["result"], "KeyError" in r["error"]), ("failed", True))

# ---------------------------------------------------------------------
# Une seule action à la fois (verrou)
# ---------------------------------------------------------------------
slow = FakeApps(delay=0.05)
ex = m.Executor(False, slow, FakeCluster(), CFG_LIVE, FIXED_CLOCK)
threads = [threading.Thread(target=ex.run, args=(proposal("Restart", S),)) for _ in range(5)]
[t.start() for t in threads]; [t.join() for t in threads]
check("5 exécutions simultanées demandées : jamais 2 en même temps", (slow.max_active, len(slow.patches)), (1, 5))


# ---------------------------------------------------------------------
# De bout en bout en mode réel : moteur complet + exécuteur
# ---------------------------------------------------------------------
class HealthyDiagnosis:
    degraded_nodes = set()
    def summary(self, roots=None): return {}

class HealthyDiagnostic:
    def diagnose(self, now): return HealthyDiagnosis()

class FakeReader:
    def __init__(self, lines): self.lines = lines
    def read_lines(self): return list(self.lines)


def attempt(n, secs):
    p = proposal("Restart", S, incident_id="i3", attempt=n, max_attempts=2)
    p["timestamp"] = (NOW + timedelta(seconds=secs - 5)).isoformat()
    return p


def two_live_cycles(apps):
    cluster = FakeCluster(ready=2)
    ex = m.Executor(False, apps, cluster, CFG_LIVE, FIXED_CLOCK)
    eng = m.Engine(CFG_LIVE, FakeReader(["ACTION_PROPOSAL:" + json.dumps(attempt(1, 0))]),
                   executor=ex, started_at=START, cluster=cluster, diagnostic=HealthyDiagnostic())
    with contextlib.redirect_stdout(io.StringIO()):
        v1 = eng.run_cycle(NOW)
        eng.reader.lines = ["ACTION_PROPOSAL:" + json.dumps(attempt(2, 60))]
        v2 = eng.run_cycle(NOW + timedelta(seconds=60))
    return eng, v1[0], v2[0]

apps = FakeApps()
eng, v1, v2 = two_live_cycles(apps)
check("S3 réel : 1re tentative exécutée", (v1["verdict"], v1["execution"]["executed"], len(apps.patches)), ("ALLOW", True, 1))
check("S3 réel : 2e tentative refusée, aucun 2e patch", (v2["verdict"], v2["invariant_violated"], len(apps.patches)), ("DENY", ["invariant3_anti_flapping"], 1))
check("S3 réel : le refus ne parle pas de simulation", "simulée" in v2["justification"], False)

failing = FakeApps(fail=True)
eng, v1, v2 = two_live_cycles(failing)
check("échec d'exécution : non enregistré dans l'historique", eng.history.last(S, "Restart"), None)
check("échec d'exécution : la 2e tentative n'est pas refusée pour flapping", v2["invariant_violated"], [])

print("\nTOUS LES TESTS PASSENT" if OK else "\nDES TESTS ÉCHOUENT")
