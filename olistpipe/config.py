"""
One place that decides WHERE everything lives.

RUN_MODE=local  -> Docker emulators (MySQL container, Azurite, Cosmos emulator)
RUN_MODE=azure  -> real Azure MySQL Flexible Server, Storage account, Cosmos DB free tier

HDFS and Spark always run in Docker: that is the "on-prem" half of this hybrid design.
The code never branches on the mode beyond this file; only connection details change.

Values come from environment variables (loaded from .env in the repo root if python-dotenv is
installed, or passed in by docker compose). IN_DOCKER=1 is set by docker compose so
the same .env works from the host (localhost:33306) and from inside the network
(mysql:3306).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[1]

# Azurite and the Cosmos emulator publish these well-known development keys in their
# own docs. They are not secrets and only work against the local emulators.
AZURITE_ACCOUNT = "devstoreaccount1"
AZURITE_KEY = ("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
               "K1SZFPTOtr/KBHBeksoGMGw==")
COSMOS_EMULATOR_KEY = ("C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsE"
                       "caGQy67XIw/Jw==")


def _load_dotenv() -> None:
    env = PROJECT_DIR / ".env"
    if not env.exists():
        return
    for line in env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


@dataclass
class Settings:
    run_mode: str
    in_docker: bool
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    mysql_db: str
    mysql_analytics_db: str
    mysql_ssl_ca: str | None
    blob_connection_string: str
    blob_container: str
    cosmos_endpoint: str
    cosmos_key: str
    cosmos_database: str
    webhdfs_url: str
    hdfs_user: str
    hdfs_uri: str
    raw_root: str
    dq_max_bad_ratio: float

    @property
    def sqlalchemy_url(self) -> str:
        return self._url(self.mysql_db)

    @property
    def analytics_url(self) -> str:
        return self._url(self.mysql_analytics_db)

    def _url(self, db: str) -> str:
        url = (f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}"
               f"@{self.mysql_host}:{self.mysql_port}/{db}?charset=utf8mb4")
        if self.mysql_ssl_ca:
            url += f"&ssl_ca={self.mysql_ssl_ca}"
        return url

    @property
    def jdbc_url(self) -> str:
        """For Spark. rewriteBatchedStatements turns N single-row INSERTs into multi-row
        INSERTs: one of the cheapest big wins when writing to MySQL over JDBC."""
        ssl = "true" if self.mysql_ssl_ca else "false"
        return (f"jdbc:mysql://{self.mysql_host}:{self.mysql_port}/{self.mysql_analytics_db}"
                f"?rewriteBatchedStatements=true&useSSL={ssl}&allowPublicKeyRetrieval=true"
                f"&serverTimezone=UTC")


def get_settings() -> Settings:
    _load_dotenv()
    e = os.environ.get
    mode = e("RUN_MODE", "local").lower()
    in_docker = e("IN_DOCKER", "0") == "1"
    if mode not in ("local", "azure"):
        raise ValueError(f"RUN_MODE must be local or azure, got {mode!r}")

    if mode == "local":
        mysql_host = "mysql" if in_docker else "127.0.0.1"
        mysql_port = 3306 if in_docker else 33306
        blob_host = "http://azurite:10000" if in_docker else "http://127.0.0.1:33000"
        blob_cs = (f"DefaultEndpointsProtocol=http;AccountName={AZURITE_ACCOUNT};"
                   f"AccountKey={AZURITE_KEY};BlobEndpoint={blob_host}/{AZURITE_ACCOUNT};")
        cosmos_endpoint = "http://cosmos:8081" if in_docker else "http://127.0.0.1:33081"
        cosmos_key = COSMOS_EMULATOR_KEY
        ssl_ca = None
        user, pwd = "olist", "olist"
    else:
        mysql_host, mysql_port = e("AZURE_MYSQL_HOST", ""), int(e("AZURE_MYSQL_PORT", "3306"))
        user, pwd = e("AZURE_MYSQL_USER", ""), e("AZURE_MYSQL_PASSWORD", "")
        default_ca = str(PROJECT_DIR / "azure" / "DigiCertGlobalRootG2.crt.pem")
        ssl_ca = e("AZURE_MYSQL_SSL_CA", default_ca)
        if not os.path.exists(ssl_ca):
            # .env holds the in-container path (/opt/project/...); on the host use the local copy
            ssl_ca = default_ca
        blob_cs = e("AZURE_STORAGE_CONNECTION_STRING", "")
        cosmos_endpoint, cosmos_key = e("AZURE_COSMOS_ENDPOINT", ""), e("AZURE_COSMOS_KEY", "")

    namenode = "namenode:9870" if in_docker else "127.0.0.1:33870"
    return Settings(
        run_mode=mode,
        in_docker=in_docker,
        mysql_host=e("MYSQL_HOST", mysql_host),
        mysql_port=int(e("MYSQL_PORT", str(mysql_port))),
        mysql_user=e("MYSQL_USER", user),
        mysql_password=e("MYSQL_PASSWORD", pwd),
        mysql_db=e("MYSQL_DB", "olist"),
        mysql_analytics_db=e("MYSQL_ANALYTICS_DB", "analytics"),
        mysql_ssl_ca=ssl_ca,
        blob_connection_string=e("BLOB_CONNECTION_STRING", blob_cs),
        blob_container=e("BLOB_CONTAINER", "landing"),
        cosmos_endpoint=e("COSMOS_ENDPOINT", cosmos_endpoint),
        cosmos_key=e("COSMOS_KEY", cosmos_key),
        cosmos_database=e("COSMOS_DATABASE", "olist"),
        webhdfs_url=e("WEBHDFS_URL", f"http://{namenode}/webhdfs/v1"),
        hdfs_user=e("HDFS_USER", "hadoop"),
        hdfs_uri=e("HDFS_URI", "hdfs://namenode:8020"),
        raw_root=e("RAW_ROOT", "/olist/raw"),
        dq_max_bad_ratio=float(e("DQ_MAX_BAD_RATIO", "0.02")),
    )
