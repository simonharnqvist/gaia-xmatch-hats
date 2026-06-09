from gaia_xmatch_hats.assets import gaia_hats, gaia_ztf_xmatched
from gaia_xmatch_hats.resources import DaskClientResource, CatalogPathsResource

defs = Definitions(
    assets=[gaia_hats, gaia_ztf_xmatched],
    resources={
        "dask_client": DaskClientResource.configure_at_launch(),
        "paths": CatalogPathsResource.configure_at_launch()
    },
)