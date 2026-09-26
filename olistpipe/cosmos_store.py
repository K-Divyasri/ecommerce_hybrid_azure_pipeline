"""
Cosmos DB for NoSQL: the real client and an in-memory fake with the same four methods.

    ensure_container(name, pk_path)
    upsert(container, doc)                      -> request charge (RU)
    read(container, doc_id, pk_value)           -> (doc, RU)       point read
    query(container, sql, params, pk=None)      -> (docs, RU)      query (cross-partition if pk is None)

Request units (RU) are Cosmos's currency: every operation costs some, and the free tier
gives 1000 RU/s. The real client reads the charge from the `x-ms-request-charge`
response header. The fake cannot know real charges, so it returns None for RU rather
than inventing numbers.
"""
from __future__ import annotations

import copy
import re

# Container name -> partition key path. Both keys match the main read and have high
# cardinality. customer360 is read one person at a time (a point read); customer_id is
# minted per order and customer_state has 27 skewed values (hot partition). Reviews are
# opened per order, and a review_id only has to be unique inside its order_id partition.
CONTAINERS = {
    "customer360": "/customer_unique_id",
    "reviews": "/order_id",
}


class CosmosStore:
    def __init__(self, endpoint: str, key: str, database: str):
        from azure.cosmos import CosmosClient
        # The emulator advertises its own internal address; endpoint discovery would
        # follow it and fail from outside the container, so it is switched off. Real
        # Azure works either way.
        self.client = CosmosClient(endpoint, credential=key, enable_endpoint_discovery=False,
                                   connection_timeout=30)
        self.db = self.client.create_database_if_not_exists(database)
        self._containers = {}

    def ensure_container(self, name: str, pk_path: str | None = None):
        from azure.cosmos import PartitionKey
        if name not in self._containers:
            self._containers[name] = self.db.create_container_if_not_exists(
                id=name, partition_key=PartitionKey(path=pk_path or CONTAINERS[name]))
        return self._containers[name]

    @staticmethod
    def _charge(container) -> float | None:
        headers = container.client_connection.last_response_headers or {}
        value = headers.get("x-ms-request-charge")
        return float(value) if value not in (None, "") else None

    def upsert(self, container: str, doc: dict) -> float | None:
        c = self.ensure_container(container)
        c.upsert_item(doc)
        return self._charge(c)

    def read(self, container: str, doc_id: str, pk_value: str):
        c = self.ensure_container(container)
        doc = c.read_item(item=doc_id, partition_key=pk_value)
        return doc, self._charge(c)

    def query(self, container: str, sql: str, params: dict | None = None, pk: str | None = None):
        c = self.ensure_container(container)
        parameters = [{"name": k, "value": v} for k, v in (params or {}).items()]
        kwargs = {"partition_key": pk} if pk is not None else {"enable_cross_partition_query": True}
        pages = c.query_items(query=sql, parameters=parameters, **kwargs).by_page()
        docs, total, seen_charge = [], 0.0, False
        for page in pages:
            docs.extend(list(page))
            ch = self._charge(c)
            if ch is not None:
                total += ch
                seen_charge = True
        return docs, (round(total, 2) if seen_charge else None)


class FakeCosmosStore:
    """Dictionary-backed stand-in for tests and offline runs. Supports the only
    query shape this project uses: SELECT * FROM c WHERE c.a = @x AND c.b.c = @y"""

    _EQ = re.compile(r"c\.([\w.]+)\s*=\s*(@\w+)")

    def __init__(self):
        self.data: dict[str, dict[tuple[str, str], dict]] = {}
        self.pk_paths: dict[str, str] = {}

    def ensure_container(self, name: str, pk_path: str | None = None):
        self.pk_paths.setdefault(name, pk_path or CONTAINERS[name])
        return self.data.setdefault(name, {})

    def _pk(self, container: str, doc: dict) -> str:
        path = self.pk_paths[container].strip("/").split("/")
        v = doc
        for p in path:
            v = v[p]
        return v

    def upsert(self, container: str, doc: dict):
        store = self.ensure_container(container)
        if "id" not in doc:
            raise ValueError("Cosmos documents need an 'id'")
        store[(self._pk(container, doc), doc["id"])] = copy.deepcopy(doc)
        return None

    def read(self, container: str, doc_id: str, pk_value: str):
        store = self.ensure_container(container)
        if (pk_value, doc_id) not in store:
            raise KeyError(f"{container}: no doc id={doc_id} in partition {pk_value}")
        return copy.deepcopy(store[(pk_value, doc_id)]), None

    def query(self, container: str, sql: str, params: dict | None = None, pk: str | None = None):
        store = self.ensure_container(container)
        where = sql.split("WHERE", 1)[1] if "WHERE" in sql.upper() else ""
        conds = self._EQ.findall(where)
        if where and not conds:
            raise NotImplementedError(f"fake only supports equality filters: {sql}")
        out = []
        for (doc_pk, _), doc in store.items():
            if pk is not None and doc_pk != pk:
                continue
            ok = True
            for path, param in conds:
                v = doc
                for p in path.split("."):
                    v = v.get(p) if isinstance(v, dict) else None
                if v != (params or {}).get(param):
                    ok = False
                    break
            if ok:
                out.append(copy.deepcopy(doc))
        return out, None

    def count(self, container: str) -> int:
        return len(self.ensure_container(container))
