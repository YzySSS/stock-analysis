"""Fail-closed implementation identity, independent of a release-tag ancestor."""
from __future__ import annotations

import hashlib
import json
import subprocess
from importlib.metadata import distributions
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATHS = ('app/shared', 'app/strategies', 'app/stock_selection',
                'requirements.txt', 'requirements.lock.txt')


def implementation_identity() -> dict[str, str]:
    def git(*args: str) -> str:
        return subprocess.check_output(['git', *args], cwd=ROOT, text=True).strip()

    if git('status', '--porcelain', '--untracked-files=all', '--', *SOURCE_PATHS):
        raise RuntimeError('research implementation must have a clean source tree')
    source = git('ls-tree', '-r', 'HEAD', '--', *SOURCE_PATHS)
    packages = sorted((d.metadata['Name'].lower(), d.version) for d in distributions())
    digest = lambda value: hashlib.sha256(value.encode()).hexdigest()
    return {
        'source_sha256': digest(source),
        'dependencies_sha256': digest(json.dumps(packages, separators=(',', ':'))),
    }
