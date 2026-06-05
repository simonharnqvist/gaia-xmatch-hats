from gaia_xmatch_hats.crossmatch import crossmatch_with_gaia

@asset(required_resource_keys={"dask_client", "paths"})
def gaia_hats(context):
    paths = context.resources.paths

    gaia_args = ImportArguments(
        sort_columns="source_id",
        ra_column="ra",
        dec_column="dec",
        input_file_list=list(paths.gaia_parquet_dir.glob("*.parquet")),
        file_reader=ParquetPyarrowReader(chunksize=100_000),
        use_schema_file=paths.gaia_schema,
        output_artifact_name="gaia_hats",
        output_path=paths.output_hats_dir,
    )

    pipeline_with_client(gaia_args, context.resources.dask_client)

@asset(required_resource_keys={"dask_client", "paths"}, deps=["gaia_hats"])
def gaia_ztf_xmatched(context):

    paths = context.resources.paths

    crossmatch_with_gaia(gaia_hats_dir = paths.gaia_hats_dir,
                        other_hats_dir = paths.ztf_hats_dir,
                        other_catalog_name = "ztf",
                        xmatch_dir = paths.output_hats_dir,
                        client = context.resources.dask_client)