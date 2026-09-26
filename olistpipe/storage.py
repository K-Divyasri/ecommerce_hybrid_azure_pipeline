"""
Where files go: HDFS (on-prem raw zone) and Azure Blob (cloud landing / backup).

All three classes share one tiny interface, so the extract code does not care which one
it is talking to and the tests can use LocalStore with no services running:

    put_bytes(path, data)   list(path)   get_bytes(path)   exists(path)   delete(path)

HdfsStore talks WebHDFS, the REST API every Hadoop namenode exposes on port 9870.
A write is a two-step dance: ask the namenode where to write (it answers with a 307
redirect to a datanode), then send the bytes to that datanode. The datanode address
in the redirect is a docker-network hostname, so HdfsStore must run INSIDE the compose
network (Airflow containers, the spark container). From the Windows host you can only
browse the namenode UI at http://localhost:33870.
"""
from __future__ import annotations

import posixpath
from pathlib import Path

import requests


class LocalStore:
    """A folder on disk pretending to be HDFS or a blob container."""

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def _p(self, path: str) -> Path:
        return self.root / path.lstrip("/")

    def put_bytes(self, path: str, data: bytes) -> None:
        p = self._p(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def get_bytes(self, path: str) -> bytes:
        return self._p(path).read_bytes()

    def list(self, path: str) -> list[str]:
        p = self._p(path)
        return sorted(x.name for x in p.iterdir()) if p.exists() else []

    def exists(self, path: str) -> bool:
        return self._p(path).exists()

    def delete(self, path: str) -> None:
        import shutil
        p = self._p(path)
        if p.is_dir():
            shutil.rmtree(p)
        elif p.exists():
            p.unlink()

    def __repr__(self) -> str:
        return f"LocalStore({self.root})"


class HdfsStore:
    def __init__(self, webhdfs_url: str, user: str = "hadoop", timeout: int = 60):
        self.base = webhdfs_url.rstrip("/")
        self.user = user
        self.timeout = timeout

    def _url(self, path: str, op: str, **params) -> str:
        q = "&".join([f"op={op}", f"user.name={self.user}"] + [f"{k}={v}" for k, v in params.items()])
        return f"{self.base}{posixpath.join('/', path)}?{q}"

    def put_bytes(self, path: str, data: bytes) -> None:
        step1 = requests.put(self._url(path, "CREATE", overwrite="true"),
                             allow_redirects=False, timeout=self.timeout)
        if step1.status_code != 307:
            raise RuntimeError(f"WebHDFS CREATE {path}: expected 307, got {step1.status_code} "
                               f"{step1.text[:200]}")
        step2 = requests.put(step1.headers["Location"], data=data, timeout=self.timeout,
                             headers={"Content-Type": "application/octet-stream"})
        if step2.status_code != 201:
            raise RuntimeError(f"WebHDFS write {path}: {step2.status_code} {step2.text[:200]}")

    def get_bytes(self, path: str) -> bytes:
        r = requests.get(self._url(path, "OPEN"), timeout=self.timeout)
        r.raise_for_status()
        return r.content

    def list(self, path: str) -> list[str]:
        r = requests.get(self._url(path, "LISTSTATUS"), timeout=self.timeout)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return sorted(s["pathSuffix"] for s in r.json()["FileStatuses"]["FileStatus"])

    def exists(self, path: str) -> bool:
        r = requests.get(self._url(path, "GETFILESTATUS"), timeout=self.timeout)
        return r.status_code == 200

    def mkdirs(self, path: str) -> None:
        requests.put(self._url(path, "MKDIRS"), timeout=self.timeout).raise_for_status()

    def delete(self, path: str) -> None:
        requests.delete(self._url(path, "DELETE", recursive="true"), timeout=self.timeout)

    def __repr__(self) -> str:
        return f"HdfsStore({self.base})"


class AzureBlobStore:
    """Same interface on top of azure-storage-blob. Identical code for Azurite and for a
    real storage account: only the connection string differs."""

    def __init__(self, connection_string: str, container: str):
        from azure.storage.blob import BlobServiceClient
        self.service = BlobServiceClient.from_connection_string(connection_string)
        self.container = self.service.get_container_client(container)
        if not self.container.exists():
            self.container.create_container()

    def put_bytes(self, path: str, data: bytes) -> None:
        self.container.upload_blob(path.lstrip("/"), data, overwrite=True)

    def get_bytes(self, path: str) -> bytes:
        return self.container.download_blob(path.lstrip("/")).readall()

    def list(self, path: str) -> list[str]:
        prefix = path.strip("/") + "/"
        names = set()
        for b in self.container.list_blobs(name_starts_with=prefix):
            names.add(b.name[len(prefix):].split("/")[0])
        return sorted(names)

    def exists(self, path: str) -> bool:
        return self.container.get_blob_client(path.lstrip("/")).exists()

    def delete(self, path: str) -> None:
        prefix = path.strip("/")
        for b in self.container.list_blobs(name_starts_with=prefix):
            self.container.delete_blob(b.name)

    def __repr__(self) -> str:
        return f"AzureBlobStore({self.service.account_name}/{self.container.container_name})"
