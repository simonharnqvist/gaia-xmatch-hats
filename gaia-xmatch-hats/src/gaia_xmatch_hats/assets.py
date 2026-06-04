@asset
def gaia_hats(context):

    client = context.resources.dask_client
    paths = context.resources.paths

    gaia_args = ImportArguments(
        sort_columns="source_id",
        ra_column="ra",
        dec_column="dec",
        input_file_list=list(paths.gaia_parquet_dir.glob("*.parquet")),
        file_reader=ParquetPyarrowReader(chunksize=100_000),
        use_schema_file=paths.gaia_schema,
        output_artifact_name="gaia_hats",
        output_path=paths.hats_dir,
    )

    pipeline_with_client(gaia_args, client)

@asset 
def ztf():


@asset
def panstarrs()

    # Load PS1 catalogs metadata
    ps1_object = open_catalog(
        PS1_OBJECT,
        columns=[
            "objID",  # PS1 ID
            "raMean",
            "decMean",  # coordinates to use for cross-matching
            "nStackDetections",  # some other data to use
        ],
    )
    display(ps1_object)

    ps1_detection = open_catalog(
        PS1_DETECTION,
        columns=[
            "objID",  # PS1 object ID
            "detectID",  # PS1 detection ID
            # not really going to use it, but we can alternatively directly cross-match with detection table
            "ra",
            "dec",
            # light-curve stuff
            "obsTime",
            "filterID",
            "psfFlux",
            "psfFluxErr",
        ],
    )
    display(ps1_detection)


@asset
def gaia_xtf_xmatched()

    xtf_catalog = lsdb.open_catalog(XTF_HATS_DIR)
    gaia_catalog = lsdb.open_catalog(GAIA_HATS_DIR)

    xmatched = xtf_catalog.crossmatch(
        gaia_catalog,
        radius_arcsec=1.0,
        n_neighbors=1,
        suffixes=("_xtf", "_gaia"),
    )

@asset 
def gaia_panstarrs_xmatched()
    ps1_catalog = lsdb.open_catalog(PS1_HATS_DIR)
    gaia_catalog = lsdb.open_catalog(GAIA_HATS_DIR)

    xmatched = ps1_catalog.crossmatch(
        gaia_catalog,
        radius_arcsec=1.0,
        n_neighbors=1,
        suffixes=("_ps1", "_gaia"),
    )