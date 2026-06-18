from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq


def get_root(context, dataset: str) -> Path:
    """
    Generic root resolver for ANY HATS dataset:
    ztf, ps1, gaia, xmatches, etc.
    """
    return Path(
        context.resources.paths.__dict__[f"{dataset}_hats_dir"]
    ).resolve()


def load_partition_info(root: Path) -> pd.DataFrame:
    return pd.read_csv(
        root / "partition_info.csv",
        sep=None,
        engine="python",
    )


def sample_partitions(df: pd.DataFrame, seed: int = 42, sample_size: int = 20) -> pd.DataFrame:
    return df.sample(
        min(sample_size, len(df)),
        random_state=seed,
    )


def partition_path(root: Path, row) -> Path:
    return (
        root
        / "dataset"
        / f"Norder={int(row.Norder)}"
        / f"Dir={int(row.Dir)}"
        / f"Npix={int(row.Npix)}.parquet"
    )


def check_metadata_files(root: Path):
    required = [
        "hats.properties",
        "partition_info.csv",
        "schema.txt",
        "linecounts.txt",
        "md5sums.txt",
    ]

    return [f for f in required if not (root / f).exists()]


def check_partition_info(df: pd.DataFrame):
    issues = []

    duplicate_count = df.duplicated(["Norder", "Npix"]).sum()
    if duplicate_count:
        issues.append(f"{duplicate_count} duplicate partitions")

    invalid_rows = (df["num_rows"] <= 0).sum()
    if invalid_rows:
        issues.append(f"{invalid_rows} empty partitions")

    invalid_dirs = (df["Dir"] != (df["Npix"] // 10000) * 10000).sum()
    if invalid_dirs:
        issues.append(f"{invalid_dirs} invalid Dir values")

    return issues


def check_partitions_exist(root: Path, sample_df: pd.DataFrame):
    missing = []

    for _, row in sample_df.iterrows():
        path = partition_path(root, row)
        if not path.exists():
            missing.append(str(path))

    return missing


def check_rowcounts(root: Path, sample_df: pd.DataFrame):
    mismatches = []

    for _, row in sample_df.iterrows():
        path = partition_path(root, row)

        if not path.exists():
            continue

        actual = pq.ParquetFile(path).metadata.num_rows
        expected = int(row.num_rows)

        if actual != expected:
            mismatches.append(
                f"Norder={row.Norder}, Npix={row.Npix}: {actual} != {expected}"
            )

    return mismatches