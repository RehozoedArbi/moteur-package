"""Tests de la boucle principale : preuve de vie et arrêt propre. Lancer : python3 test_runtime.py"""
import os
import tempfile
import threading
import time

import moteur as m

OK = True


def check(label, got, want):
    global OK
    good = got == want
    OK &= good
    print(("OK  " if good else "FAIL"), label, "->", got if not good else "")


tmp = tempfile.mkdtemp()
hb = os.path.join(tmp, "heartbeat")

# --- preuve de vie ---
m.touch_heartbeat(hb)
check("le fichier de vie est créé", os.path.exists(hb), True)
os.utime(hb, (1000, 1000))
m.touch_heartbeat(hb)
check("sa date est rafraîchie", os.path.getmtime(hb) > 1000, True)
m.touch_heartbeat(os.path.join(tmp, "absent", "heartbeat"))   # dossier inexistant : ne doit pas planter
check("chemin inaccessible : pas de plantage", True, True)
check("chemin par défaut", m.Config().heartbeat_file, "/tmp/moteur-heartbeat")
os.environ["HEARTBEAT_FILE"] = "/tmp/x"
check("chemin lu dans l'environnement", m.Config.from_env().heartbeat_file, "/tmp/x")
del os.environ["HEARTBEAT_FILE"]


class FakeEngine:
    def __init__(self, fail_first=0):
        self.cycles, self.fail_first = 0, fail_first

    def run_cycle(self):
        self.cycles += 1
        if self.cycles <= self.fail_first:
            raise RuntimeError("panne dans le cycle")
        return []


cfg = m.Config(poll_interval_s=0.01, heartbeat_file=hb)

# --- la boucle tourne, survit aux erreurs, rafraîchit la preuve de vie ---
os.utime(hb, (1000, 1000))
eng, stop = FakeEngine(fail_first=2), threading.Event()
t = threading.Thread(target=m.run_forever, args=(eng, cfg, stop)); t.start()
time.sleep(0.3)
check("des cycles s'enchaînent malgré 2 erreurs", eng.cycles > 3, True)
check("la preuve de vie est fraîche", time.time() - os.path.getmtime(hb) < 2, True)
stop.set(); t.join(timeout=2)
check("arrêt propre sur demande", t.is_alive(), False)

# --- l'arrêt réveille immédiatement une longue attente ---
slow_cfg = m.Config(poll_interval_s=30, heartbeat_file=hb)
eng, stop = FakeEngine(), threading.Event()
t = threading.Thread(target=m.run_forever, args=(eng, slow_cfg, stop)); t.start()
time.sleep(0.1); start = time.time()
stop.set(); t.join(timeout=5)
check("arrêt immédiat malgré un cycle de 30 s", (t.is_alive(), time.time() - start < 1), (False, True))
check("un seul cycle a eu lieu", eng.cycles, 1)

# --- l'arrêt ne coupe pas un cycle en cours ---
class SlowEngine:
    def __init__(self, stop): self.stop, self.finished = stop, False
    def run_cycle(self):
        self.stop.set()            # l'arrêt est demandé PENDANT le cycle
        time.sleep(0.1)
        self.finished = True
        return []
stop = threading.Event(); eng = SlowEngine(stop)
m.run_forever(eng, cfg, stop)
check("le cycle en cours va jusqu'au bout", eng.finished, True)

# --- la preuve de vie est posée avant le premier cycle ---
os.remove(hb)
seen = {}
class ProbeEngine:
    def run_cycle(self): seen["existait"] = os.path.exists(hb); return []
stop = threading.Event(); stop.clear()
th = threading.Thread(target=m.run_forever, args=(ProbeEngine(), cfg, stop)); th.start()
time.sleep(0.1); stop.set(); th.join(timeout=2)
check("fichier présent dès le premier cycle (sonde liveness)", seen.get("existait"), True)

print("\nTOUS LES TESTS PASSENT" if OK else "\nDES TESTS ÉCHOUENT")
