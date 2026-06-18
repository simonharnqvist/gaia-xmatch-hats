from pathlib import Path
import lsdb
import pandas as pd
from dagster import asset_check, AssetCheckResult

SAMPLE_SIZE = 20


def hats_root(context) -> Path:
    return Path(
        context.resources.paths.ztf_hats_dir
    ).resolve()


def load_partition_info(root: Path) -> pd.DataFrame:
    return pd.read_csv(
        root / "partition_info.csv",
        sep=None,
        engine="python",
    )


def sample_partitions(root: Path) -> pd.DataFrame:
    df = load_partition_info(root)

    return df.sample(
        min(SAMPLE_SIZE, len(df)),
        random_state=42,
    )


def partition_path(root: Path, row) -> Path:
    return (
        root
        / "dataset"
        / f"Norder={int(row.Norder)}"
        / f"Dir={int(row.Dir)}"
        / f"Npix={int(row.Npix)}.parquet"
    )

@asset_check(
    asset="ztf_hats",
    required_resource_keys={"paths"},
)
def metadata_files_exist(context):

    root = hats_root(context)

    required = [
        "hats.properties",
        "partition_info.csv",
        "schema.txt",
        "linecounts.txt",
        "md5sums.txt",
    ]

    missing = [
        f
        for f in required
        if not (root / f).exists()
    ]

    return AssetCheckResult(
        passed=not missing,
        metadata={
            "archive_root": str(root),
            "missing_files": missing,
        },
    )


@asset_check(
    asset="ztf_hats",
    required_resource_keys={"paths"},
)
def partition_info_valid(context):

    root = hats_root(context)

    df = load_partition_info(root)

    duplicate_count = (
        df.duplicated(["Norder", "Npix"]).sum()
    )

    invalid_rows = (
        df["num_rows"] <= 0
    ).sum()

    invalid_dirs = (
        df["Dir"]
        != (df["Npix"] // 10000) * 10000
    ).sum()

    failures = []

    if duplicate_count:
        failures.append(
            f"{duplicate_count} duplicate partitions"
        )

    if invalid_rows:
        failures.append(
            f"{invalid_rows} empty partitions"
        )

    if invalid_dirs:
        failures.append(
            f"{invalid_dirs} invalid Dir values"
        )

    return AssetCheckResult(
        passed=not failures,
        metadata={
            "partitions": len(df),
            "total_rows": int(df["num_rows"].sum()),
            "issues": failures,
        },
    )

@asset_check(
    asset="ztf_hats",
    required_resource_keys={"paths"},
)
def sampled_partitions_exist(context):

    root = hats_root(context)

    missing = []

    for _, row in sample_partitions(root).iterrows():

        path = partition_path(root, row)

        if not path.exists():
            missing.append(str(path))

    return AssetCheckResult(
        passed=not missing,
        metadata={
            "sample_size": SAMPLE_SIZE,
            "missing": missing[:20],
        },
    )

@asset_check(
    asset="ztf_hats",
    required_resource_keys={"paths"},
)
def sampled_partition_rowcounts(context):

    root = hats_root(context)

    mismatches = []

    for _, row in sample_partitions(root).iterrows():

        path = partition_path(root, row)

        if not path.exists():
            continue

        actual = (
            pq.ParquetFile(path)
            .metadata
            .num_rows
        )

        expected = int(row.num_rows)

        if actual != expected:

            mismatches.append(
                (
                    f"Norder={row.Norder}, "
                    f"Npix={row.Npix}: "
                    f"{actual} != {expected}"
                )
            )

    return AssetCheckResult(
        passed=not mismatches,
        metadata={
            "sample_size": SAMPLE_SIZE,
            "mismatches": mismatches[:20],
        },
    )

@asset_check(asset = "gaia_hats")
def check_lsdb_can_open_gaia_hats(context) -> AssetCheckResult:

    gaia_hats_path = Path(context.resources.paths.gaia_hats_dir).join("gaia_hats")

    ddf = lsdb.open_catalog(path=gaia_hats_path, columns=["ra", "dec"])
    passed = (list(ddf.columns) == ['ra', 'dec'])

    return AssetCheckResult(passed = passed, metadata={"ddf.columns": ddf.columns})


# @asset_check(asset = gaia_ztf_xmatched)
# def check_lsdb_can_open_gaia_ztf_xmatched(context) -> AssetCheckResult:

#     ddf = lsdb.open_catalog(path=context.resources.paths.xmatched_hats_dir, columns=["ra", "dec"])
#     passed = (list(ddf.columns) == ['ra', 'dec'])

#     return AssetCheckResult(passed = passed, metadata={"ddf.columns": ddf.columns})


