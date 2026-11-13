"""
Upload everything under bronze/ to a Databricks Unity Catalog Volume.

Env vars (set as GitHub secrets):
  DATABRICKS_HOST   e.g. https://dbc-xxxx.cloud.databricks.com  (no trailing slash)
  DATABRICKS_TOKEN  personal access token

Optional:
  VOLUME_ROOT       default /Volumes/workspace/bronze/landing
  LOCAL_DIR         default bronze

Result: bronze/prices/prices_20261113.csv
     -> /Volumes/workspace/bronze/landing/prices/prices_20261113.csv

Same file name = overwritten, so re-running a day is safe.
Exit code 1 if any file fails, so the GitHub job turns red.
"""
import os
import sys
from pathlib import Path

import requests

HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
VOLUME_ROOT = os.environ.get("VOLUME_ROOT", "/Volumes/workspace/bronze/landing").rstrip("/")
LOCAL_DIR = Path(os.environ.get("LOCAL_DIR", "bronze"))

if not HOST or not TOKEN:
    sys.exit("Missing DATABRICKS_HOST or DATABRICKS_TOKEN")
if not HOST.startswith("https://"):
    sys.exit("DATABRICKS_HOST must start with https://")

HEADERS = {"Authorization": f"Bearer {TOKEN}"}


def make_dir(path):
    r = requests.put(f"{HOST}/api/2.0/fs/directories{path}", headers=HEADERS, timeout=60)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"mkdir {path} -> {r.status_code} {r.text[:300]}")


def upload(local, remote):
    with open(local, "rb") as f:
        r = requests.put(
            f"{HOST}/api/2.0/fs/files{remote}",
            headers=HEADERS,
            params={"overwrite": "true"},
            data=f,
            timeout=300,
        )
    if r.status_code not in (200, 204):
        raise RuntimeError(f"upload {remote} -> {r.status_code} {r.text[:300]}")


def main():
    if not LOCAL_DIR.exists():
        sys.exit(f"{LOCAL_DIR} not found, nothing to upload")

    files = sorted(p for p in LOCAL_DIR.rglob("*") if p.is_file())
    if not files:
        sys.exit("No files found under bronze/")

    made, failed = set(), []
    for p in files:
        rel = p.relative_to(LOCAL_DIR).as_posix()
        remote = f"{VOLUME_ROOT}/{rel}"
        folder = remote.rsplit("/", 1)[0]
        try:
            if folder not in made:
                make_dir(folder)
                made.add(folder)
            upload(p, remote)
            print(f"OK    {rel} -> {remote}")
        except Exception as e:
            print(f"FAIL  {rel}: {e}")
            failed.append(rel)

    print(f"\nUploaded {len(files) - len(failed)}/{len(files)} files")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
