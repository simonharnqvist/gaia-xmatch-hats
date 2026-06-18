from dask.distributed import Client
import lsdb

def crossmatch_with_gaia(gaia_hats: str, other_hats: str,
                         other_catalog_name: str, xmatch_dir: str, dask_client: Client):
    
    with dask_client as client:
        gaia_catalog = lsdb.open_catalog(gaia_hats)
        other_catalog = lsdb.open_catalog(other_hats)


        if not isinstance(gaia_catalog, lsdb.Catalog):
            raise TypeError("LH input is not of type lsdb.Catalog")

        if not isinstance(other_catalog, lsdb.Catalog):
            raise TypeError("RH input is not of type lsdb.Catalog")

        xmatched = gaia_catalog.crossmatch(
            other_catalog,
            radius_arcsec=1.0,
            n_neighbors=1,
            suffixes=("_gaia", f"_{other_catalog_name}"),
        )

        if not isinstance(xmatched, lsdb.Catalog):
            raise TypeError("Output is not of type lsdb.Catalog")

        lsdb.io.to_hats(catalog = xmatched, base_catalog_path=xmatch_dir)
