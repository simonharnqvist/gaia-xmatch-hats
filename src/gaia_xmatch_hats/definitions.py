import dagster as dg

from gaia_xmatch_hats import assets, checks, resources


all_assets = dg.load_assets_from_modules([assets])
all_checks = dg.load_asset_checks_from_modules([checks])


defs = dg.Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    resources={
        "paths": resources.CatalogPathsResource.configure_at_launch(),
        "dask_client": resources.DaskClientResource.configure_at_launch(),
    },
    loggers={"console": dg.json_console_logger}
)