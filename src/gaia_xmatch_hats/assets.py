from dagster import asset
from hats_import.catalog.file_readers import ParquetPyarrowReader
from hats_import.pipeline import ImportArguments, pipeline_with_client
from gaia_xmatch_hats.crossmatch import crossmatch_with_gaia
from pathlib import Path
import subprocess


@asset(required_resource_keys={"dask_client", "paths"}, deps="ztf_hats")
def gaia_hats(context):
    paths = context.resources.paths

    gaia_parquet_dir = Path(paths.gaia_parquet_dir).resolve()
    gaia_hats_dir = Path(paths.gaia_hats_dir).resolve()
    ztf_hats_dir = Path(paths.ztf_hats_dir).resolve()


    gaia_args = ImportArguments(
        sort_columns="source_id",
        ra_column="ra",
        dec_column="dec",
        input_file_list=list(gaia_parquet_dir.glob("*.parquet")),
        file_reader=ParquetPyarrowReader(chunksize=100_000),
        output_artifact_name=paths.gaia_artifact_name,
        output_path=gaia_hats_dir,
    )

    client = context.resources.dask_client.create_or_get_client()

    pipeline_with_client(gaia_args, client)


@asset(required_resource_keys={"paths"})
def ztf_hats(context):

    ztf_hats_dir = Path(context.resources.paths.ztf_hats_dir).resolve()
    ztf_hats_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        "aws", "s3", "cp",
        "--recursive",
        "--no-sign-request",
        "s3://ipac-irsa-ztf/ztf/enhanced/dr24/lc/hats/",
        str(ztf_hats_dir),
    ]

    subprocess.run(cmd, check=True)


@asset(required_resource_keys={"dask_client", "paths"}, deps=["gaia_hats"])
def gaia_ztf_xmatched(context):

    paths = context.resources.paths

    gaia_hats_path = Path(paths.gaia_hats_dir).resolve().joinpath(paths.gaia_hats_artifact_name)
    ztf_hats_path = Path(paths.ztf_hats_dir).resolve().joinpath(paths.ztf_hats_artifact_name)
    xmatched_hats_dir = Path(paths.gaia_ztf_xmatched_dir).resolve()

    client = context.resources.dask_client.create_or_get_client()

    crossmatch_with_gaia(gaia_hats = gaia_hats_path,
                        other_hats = ztf_hats_path,
                        other_catalog_name = "ztf",
                        xmatch_dir = xmatched_hats_dir,
                        dask_client = client)


@asset(required_resource_keys={"dask_client", "paths"}, deps=["gaia_hats"])
def ps1_hats(context):
    paths = context.resources.paths

    client = context.resources.dask_client.create_or_get_client()

    PS1_PATH = UPath(path.ps1_hats_url, anon=True)
    PS1_OBJECT = PS1_PATH / "otmo"
    PS1_DETECTION = PS1_PATH / "detection"

    ps1_object = open_catalog(
        PS1_OBJECT,
        columns=[
            "objID",
            "raMean",
            "decMean",
            "nStackDetections"
        ])

    ps1_detection = open_catalog(
        PS1_DETECTION,
        columns=[
            "objID",
            "detectID",
            "obsTime",
            "filterID",
            "psfFlux",
            "psfFluxErr",
            "airMass", 
            "sky", 
            "skyErr", 
            "apFlux", 
            "apFluxErr", 
            "zp"
        ]
    )

    ps1_object.write_catalog(base_catalog_path=paths.ps1_hats_dir, catalog_name="PS1_object")
    ps1_detection.write_catalog(base_catalog_path=paths.ps1_hats_dir, catalog_name="PS1_detection")


@asset(required_resource_keys={"dask_client", "paths"}, deps=["gaia_hats"])
def gaia_ps1_xmatched(context):

    paths = context.resources.paths

    gaia_hats_path = Path(paths.gaia_hats_dir).resolve().joinpath(paths.gaia_hats_artifact_name)
    ps1_hats_path = Path(paths.ps1_hats_dir).resolve().joinpath("PS1_detection")
    xmatched_hats_dir = Path(paths.gaia_ps1_xmatched_dir).resolve()

    client = context.resources.dask_client.create_or_get_client()

    crossmatch_with_gaia(gaia_hats = gaia_hats_path,
                        other_hats = ps1_hats_path,
                        other_catalog_name = "ps1",
                        xmatch_dir = xmatched_hats_dir,
                        dask_client = client)
