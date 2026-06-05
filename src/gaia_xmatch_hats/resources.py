import dagster as dg
from dask.distributed import Client

class CatalogPathsResource(dg.ConfigurableResource):
    gaia_parquet_dir: str
    output_hats_dir: str
    ztf_hats_path: str


class DaskClientResource(dg.ConfigurableResource):
    n_workers: int = 32
    memory_limit: str = "auto"

    def get_client(self) -> Client:
        cluster = LocalCluster(n_workers = n_workers, memory_limit = memory_limit)
        client = cluster.get_client()

        return client