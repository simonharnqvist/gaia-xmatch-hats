from pathlib import Path
import lsdb
import pandas as pd
import pyarrow.parquet as pq
from dagster import asset_check, AssetCheckResult

from gaia_xmatch_hats.check_helpers import (
    get_root,
    load_partition_info,
    sample_partitions,
    partition_path,
    check_metadata_files,
    check_partition_info,
    check_partitions_exist,
    check_rowcounts
)


################
# Gaia HATS
################

@asset_check(asset="gaia_hats", required_resource_keys={"paths"})
def gaia_metadata_files_exist(context):
    root = get_root(context, "gaia")
    missing = check_metadata_files(root)

    return AssetCheckResult(
        passed=not missing,
        metadata={"missing_files": missing},
    )


@asset_check(asset="gaia_hats", required_resource_keys={"paths"})
def gaia_lsdb_open_check(context):
    root = get_root(context, "gaia")

    try:
        ddf = lsdb.open_catalog(path=root, columns=["ra", "dec"])
        passed = {"ra", "dec"}.issubset(set(ddf.columns))
    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
        )

    return AssetCheckResult(
        passed=passed,
        metadata={"columns": list(ddf.columns)},
    )


################
# ZTF
################

@asset_check(asset="ztf_hats", required_resource_keys={"paths"})
def ztf_metadata_files_exist(context):
    root = get_root(context, "ztf")
    missing = check_metadata_files(root)

    return AssetCheckResult(
        passed=not missing,
        metadata={"missing_files": missing},
    )


@asset_check(asset="ztf_hats", required_resource_keys={"paths"})
def ztf_partition_info_valid(context):
    root = get_root(context, "ztf")
    df = load_partition_info(root)

    issues = check_partition_info(df)

    return AssetCheckResult(
        passed=not issues,
        metadata={"issues": issues},
    )


@asset_check(asset="ztf_hats", required_resource_keys={"paths"})
def ztf_sampled_partitions_exist(context):
    root = get_root(context, "ztf")
    df = load_partition_info(root)
    sample_df = sample_partitions(df)

    missing = check_partitions_exist(root, sample_df)

    return AssetCheckResult(
        passed=not missing,
        metadata={"missing": missing[:20]},
    )


@asset_check(asset="ztf_hats", required_resource_keys={"paths"})
def ztf_sampled_rowcounts(context):
    root = get_root(context, "ztf")
    df = load_partition_info(root)
    sample_df = sample_partitions(df)

    mismatches = check_rowcounts(root, sample_df)

    return AssetCheckResult(
        passed=not mismatches,
        metadata={"mismatches": mismatches[:20]},
    )


################
# PS1
################

@asset_check(asset="ps1_hats", required_resource_keys={"paths"})
def ps1_metadata_files_exist(context):
    root = get_root(context, "ps1")
    missing = check_metadata_files(root)

    return AssetCheckResult(
        passed=not missing,
        metadata={"missing_files": missing},
    )


@asset_check(asset="ps1_hats", required_resource_keys={"paths"})
def ps1_partition_info_valid(context):
    root = get_root(context, "ps1")
    df = load_partition_info(root)

    issues = check_partition_info(df)

    return AssetCheckResult(
        passed=not issues,
        metadata={"issues": issues},
    )


@asset_check(asset="ps1_hats", required_resource_keys={"paths"})
def ps1_sampled_partitions_exist(context):
    root = get_root(context, "ps1")
    df = load_partition_info(root)
    sample_df = sample_partitions(df)

    missing = check_partitions_exist(root, sample_df)

    return AssetCheckResult(
        passed=not missing,
        metadata={"missing": missing[:20]},
    )


@asset_check(asset="ps1_hats", required_resource_keys={"paths"})
def ps1_sampled_rowcounts(context):
    root = get_root(context, "ps1")
    df = load_partition_info(root)
    sample_df = sample_partitions(df)

    mismatches = check_rowcounts(root, sample_df)

    return AssetCheckResult(
        passed=not mismatches,
        metadata={"mismatches": mismatches[:20]},
    )


#######################
# Gaia X ZTF crossmatch
#######################

@asset_check(asset="gaia_ztf_xmatched", required_resource_keys={"paths"})
def gaia_ztf_xmatch_loadable(context):
    root = get_xmatch_root(context, "gaia_ztf")

    ok, info = check_lsdb_open(root)

    return AssetCheckResult(
        passed=ok,
        metadata={"columns_or_error": str(info)},
    )


@asset_check(asset="gaia_ztf_xmatched", required_resource_keys={"paths"})
def gaia_ztf_xmatch_schema(context):
    root = get_xmatch_root(context, "gaia_ztf")

    ok, cols = check_lsdb_open(root)

    required = {"ra", "dec"}
    missing = check_required_columns(cols, required) if ok else ["failed_open"]

    return AssetCheckResult(
        passed=not missing,
        metadata={
            "columns": list(cols) if ok else None,
            "missing": missing,
        },
    )


@asset_check(asset="gaia_ztf_xmatched", required_resource_keys={"paths"})
def gaia_ztf_xmatch_rowcount(context):
    root = get_xmatch_root(context, "gaia_ztf")

    try:
        cat = lsdb.open_catalog(path=root)
        n_rows = len(cat)

        return AssetCheckResult(
            passed=n_rows > 0,
            metadata={"n_rows": int(n_rows)},
        )

    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
        )


#######################
# Gaia X PS1 crossmatch
#######################

@asset_check(asset="gaia_ps1_xmatched", required_resource_keys={"paths"})
def gaia_ps1_xmatch_loadable(context):
    root = get_xmatch_root(context, "gaia_ps1")

    ok, info = check_lsdb_open(root)

    return AssetCheckResult(
        passed=ok,
        metadata={"columns_or_error": str(info)},
    )


@asset_check(asset="gaia_ps1_xmatched", required_resource_keys={"paths"})
def gaia_ps1_xmatch_schema(context):
    root = get_xmatch_root(context, "gaia_ps1")

    ok, cols = check_lsdb_open(root)

    required = {"ra", "dec"}
    missing = check_required_columns(cols, required) if ok else ["failed_open"]

    return AssetCheckResult(
        passed=not missing,
        metadata={
            "columns": list(cols) if ok else None,
            "missing": missing,
        },
    )


@asset_check(asset="gaia_ps1_xmatched", required_resource_keys={"paths"})
def gaia_ps1_xmatch_rowcount(context):
    root = get_xmatch_root(context, "gaia_ps1")

    try:
        cat = lsdb.open_catalog(path=root)
        n_rows = len(cat)

        return AssetCheckResult(
            passed=n_rows > 0,
            metadata={"n_rows": int(n_rows)},
        )

    except Exception as e:
        return AssetCheckResult(
            passed=False,
            metadata={"error": str(e)},
        )