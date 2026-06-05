from dask.distributed import Client
import lsdb

def crossmatch_with_gaia(gaia_hats_dir: str, other_hats_dir: str, other_catalog_name: str, xmatch_dir: str, dask_client: Client):
    
    with dask_client as client:
        other_catalog = lsdb.open_catalog(other_hats_dir)
        gaia_catalog = lsdb.open_catalog(gaia_hats_dir)

        xmatched = other_catalog.crossmatch(
            gaia_catalog,
            radius_arcsec=1.0,
            n_neighbors=1,
            suffixes=(f"_{other_catalog_name}", "_gaia"),
        )

        xmatched.write_catalog(xmatch_dir)
