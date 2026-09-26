from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from olistpipe.cosmos_store import FakeCosmosStore
from olistpipe.customer360 import build_customer_docs, build_review_docs, doc_hash, sync_docs
from olistpipe.db import sqlite_engine
from olistpipe.storage import LocalStore


@pytest.fixture()
def gold():
    fo = pd.DataFrame({
        "order_id": ["o1", "o2", "o3", "o4"],
        "customer_unique_id": ["u1", "u1", "u2", "u3"],
        "purchase_ts": pd.to_datetime(["2018-01-05", "2018-07-01", "2018-03-03", "2018-04-04"]),
        "order_status": ["delivered", "delivered", "delivered", "canceled"],
        "order_value": [100.0, 60.0, 30.0, 45.0],
        "is_late": [0, 1, 0, 0],
    })
    dim = pd.DataFrame({"customer_unique_id": ["u1", "u2", "u3"],
                        "customer_city": ["sao paulo", "recife", "natal"],
                        "customer_state": ["SP", "PE", "RN"],
                        "customer_zip_code_prefix": ["01001", "50000", "59000"]})
    rfm = pd.DataFrame({"customer_unique_id": ["u1", "u2"], "r_score": [5, 2], "f_score": [5, 1],
                        "m_score": [5, 1], "rfm_code": ["555", "211"],
                        "segment": ["Champions", "Hibernating"]})
    rv = pd.DataFrame({"review_id": ["r1", "r2", "r3"], "order_id": ["o1", "o2", "o3"],
                       "customer_unique_id": ["u1", "u1", "u2"], "review_score": [5, 2, 4],
                       "review_comment_title": [None, None, "Bom"],
                       "review_comment_message": ["Otimo", "Atrasou", None],
                       "review_creation_ts": pd.to_datetime(["2018-01-15", "2018-07-20", "2018-03-20"])})
    return fo, dim, rfm, rv


def test_customer_doc_shape(gold):
    docs = {d["id"]: d for d in build_customer_docs(*gold, as_of=date(2018, 9, 1))}
    assert set(docs) == {"u1", "u2"}  # u3 only has a canceled order
    u1 = docs["u1"]
    assert u1["customer_unique_id"] == "u1" and u1["order_count"] == 2
    assert u1["total_spend"] == 160.0 and u1["late_deliveries"] == 1
    assert u1["last_order"]["order_id"] == "o2"
    assert u1["rfm"]["segment"] == "Champions"
    assert [r["review_id"] for r in u1["recent_reviews"]] == ["r2", "r1"]  # newest first


def test_point_read_and_query_on_fake(gold):
    store = FakeCosmosStore()
    for d in build_customer_docs(*gold, as_of=date(2018, 9, 1)):
        store.upsert("customer360", d)
    doc, ru = store.read("customer360", "u2", "u2")
    assert doc["profile"]["state"] == "PE" and ru is None  # fake never invents RU numbers
    hits, _ = store.query("customer360", "SELECT * FROM c WHERE c.rfm.segment = @s",
                          {"@s": "Champions"})
    assert [h["id"] for h in hits] == ["u1"]
    with pytest.raises(KeyError):
        store.read("customer360", "u2", "wrong-partition")


def test_review_ids_only_unique_within_partition():
    rv = pd.DataFrame({"review_id": ["same", "same"], "order_id": ["o1", "o2"],
                       "customer_unique_id": ["u1", "u2"], "review_score": [5, 1],
                       "review_comment_title": [None, None], "review_comment_message": [None, None],
                       "review_creation_ts": pd.to_datetime(["2018-01-01", "2018-01-02"])})
    store = FakeCosmosStore()
    for d in build_review_docs(rv):
        store.upsert("reviews", d)
    assert store.count("reviews") == 2


def test_sync_is_idempotent(gold):
    eng = sqlite_engine()
    store = FakeCosmosStore()
    docs = build_customer_docs(*gold, as_of=date(2018, 9, 1))
    first = sync_docs(store, eng, "customer360", docs, log=lambda *a: None)
    again = sync_docs(store, eng, "customer360", build_customer_docs(*gold, as_of=date(2018, 9, 2)),
                      log=lambda *a: None)
    assert first["upserted"] == 2 and again["upserted"] == 0  # as_of excluded from the hash
    docs[0]["order_count"] = 99
    assert sync_docs(store, eng, "customer360", docs, log=lambda *a: None)["upserted"] == 1


def test_doc_hash_ignores_cosmos_system_fields():
    d = {"id": "x", "a": 1}
    assert doc_hash(d) == doc_hash({**d, "_etag": "abc", "_ts": 1, "as_of": "2018-01-01"})


def test_local_store_roundtrip(tmp_path):
    s = LocalStore(tmp_path)
    s.put_bytes("/a/b/c.bin", b"hi")
    assert s.get_bytes("a/b/c.bin") == b"hi" and s.list("/a/b") == ["c.bin"]
    s.delete("/a")
    assert not s.exists("/a/b/c.bin")


def test_config_host_vs_docker(monkeypatch):
    from olistpipe.config import get_settings
    for k in ("MYSQL_HOST", "MYSQL_PORT", "RUN_MODE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("IN_DOCKER", "0")
    s = get_settings()
    assert (s.mysql_host, s.mysql_port) == ("127.0.0.1", 33306)
    assert "127.0.0.1:33000" in s.blob_connection_string
    monkeypatch.setenv("IN_DOCKER", "1")
    s = get_settings()
    assert (s.mysql_host, s.mysql_port) == ("mysql", 3306)
    assert s.jdbc_url.startswith("jdbc:mysql://mysql:3306/analytics?rewriteBatchedStatements=true")


def test_dag_file_compiles():
    src = (Path(__file__).resolve().parents[1] / "dags" / "olist_daily.py").read_text()
    compile(src, "olist_daily.py", "exec")
    assert "from airflow.sdk import" in src and "schedule=" in src
