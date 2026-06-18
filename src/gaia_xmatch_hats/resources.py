import dagster as dg
from dask.distributed import Client, LocalCluster

class CatalogPathsResource(dg.ConfigurableResource):
    gaia_parquet_dir: str
    gaia_hats_dir: str
    gaia_hats_artifact_name: str
    ztf_hats_dir: str
    ztf_hats_artifact_name: str
    xmatched_hats_dir: str


class DaskClientResource(dg.ConfigurableResource):
    n_workers: int = 4
    threads_per_worker: int = 8
    memory_limit: str = "auto"

    def create_or_get_client(self) -> Client:
        cluster = LocalCluster(n_workers = self.n_workers, memory_limit = self.memory_limit)
        client = cluster.get_client()

        return client