import sys
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))

SAMPLE = PROJECT / "data" / "sample" / "olist"


@pytest.fixture(scope="session")
def small_frames():
    from olistpipe.generator import generate
    return generate(n_orders=400, seed=11)


@pytest.fixture()
def seeded_engine(small_frames):
    from olistpipe.db import sqlite_engine
    from olistpipe.loader import seed
    eng = sqlite_engine()
    seed(eng, small_frames)
    return eng
