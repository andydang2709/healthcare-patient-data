"""Run build_db then all export scripts. Execute from project root: python scripts/run_all_exports.py"""
import subprocess
import sys
from pathlib import Path

# Ensure we run from project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if Path.cwd() != PROJECT_ROOT:
    print(f"Changing to project root: {PROJECT_ROOT}")
    import os
    os.chdir(PROJECT_ROOT)

scripts = [
    "scripts/build_db.py",
    "scripts/export_patients.py",
    "scripts/export_allergyintolerance.py",
    "scripts/export_condition.py",
    "scripts/export_device.py",
    "scripts/export_encounter.py",
    "scripts/export_immunization.py",
    "scripts/export_medicationrequest.py",
    "scripts/export_observation.py",
    "scripts/export_procedure.py",
]

for script in scripts:
    print(f"\n--- Running {script} ---", flush=True)
    result = subprocess.run([sys.executable, script], cwd=PROJECT_ROOT)
    if result.returncode != 0:
        sys.exit(result.returncode)

print("\n--- All exports complete ---")
