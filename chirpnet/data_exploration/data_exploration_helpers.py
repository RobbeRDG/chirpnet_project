import polars as pl
import os
from glob import glob
from os.path import join


def get_combined_full_species_metadata_across_collections(
    base_collections_dir_path: str,
) -> pl.DataFrame:
    """
    Get the full species metadata of all species across all collections of species
    recordings.

    Returns
    -------
    pl.DataFrame
        The full species metadata across all collections.

    """

    collection_names = os.listdir(base_collections_dir_path)

    # Get the metadata for each collection
    full_collections_metadata_list: list[pl.DataFrame] = []
    for collection_name in collection_names:
        full_collections_metadata_list.append(
            get_full_species_metadata_for_collection(
                join(base_collections_dir_path, collection_name)
            )
        )

    return pl.concat(full_collections_metadata_list)


def get_full_species_metadata_for_collection(collection_dir_path: str) -> pl.DataFrame:
    """
    Get the full species metadata for a collection of species recordings.

    Parameters
    ----------
    collection_dir_path : str
        The path to the directory containing the collection.

    Returns
    -------
    pl.DataFrame
        The full species metadata for the collection.

    """

    # Each collection contains multiple species directories
    species_dirs = os.listdir(collection_dir_path)

    # Remove the unneeded metadata directory
    species_dirs.remove("metadata")

    full_species_metadata_list: list[pl.DataFrame] = []
    for species_dir in species_dirs:
        # Skip if empty directory
        if not os.listdir(join(collection_dir_path, species_dir)):
            continue

        species_metadata_path = glob(
            join(collection_dir_path, species_dir, "*recording_metadata.csv")
        )[0]

        full_species_metadata_list.append(
            pl.read_csv(species_metadata_path, infer_schema=False)  # type: ignore
        )

    return pl.concat(full_species_metadata_list)


def extract_general_metadata_statistics(
    species_metadata: pl.DataFrame,
) -> dict[str, float]:
    """Extract some general statistics from the species metadata.

    The following statistics are extracted:
    - Total number of species recordings ("total_spececies_recordings").
    - Total number of "clean" species recordings ("total_clean_species_recordings").
    - 25th percentile of species recordings ("25th_percentile_species_recordings").
    - 25th percentile of "clean" species recordings ("25th_percentile_clean_species_recordings").
    - Average number of species recordings ("avg_species_recordings").
    - Average number of "clean" species recordings ("avg_clean_species_recordings").
    - Median number of species recordings ("median_species_recordings").
    - Median number of "clean" species recordings ("median_clean_species_recordings").
    - 75th percentile of species recordings ("75th_percentile_species_recordings").
    - 75th percentile of "clean" species recordings ("75th_percentile_clean_species_recordings").
    - Maximum number of species recordings ("max_species_recordings").
    - Maximum number of "clean" species recordings ("max_clean_species_recordings").
    - Minimum number of species recordings ("min_species_recordings").
    - Minimum number of "clean" species recordings ("min_clean_species_recordings").
    - Total species recording duration ("total_species_recording_length").
    - Total "clean" species recording duration ("total_clean_species_recording_length").
    - 25th percentile of species recording duration ("25th_percentile_species_recording_length").
    - 25th percentile of "clean" species recording duration ("25th_percentile_clean_species_recording_length").
    - Average species recording duration ("avg_species_recording_length").
    - Average "clean" species recording duration ("avg_clean_species_recording_length").
    - Median species recording duration ("median_species_recording_length").
    - Median "clean" species recording duration ("median_clean_species_recording_length").
    - 75th percentile of species recording duration ("75th_percentile_species_recording_length").
    - 75th percentile of "clean" species recording duration ("75th_percentile_clean_species_recording_length").
    - Maximum species recording duration ("max_species_recording_length").
    - Maximum "clean" species recording duration ("max_clean_species_recording_length").
    - Minimum species recording duration ("min_species_recording_length").
    - Minimum "clean" species recording duration ("min_clean_species_recording_length").

    Note: Here, we define a "clean" recording as one where there are no other species
    present as background species in the recording.

    Parameters
    ----------
    species_metadata
        The species metadata DataFrame.

    Returns
    -------
    dict[str, float]
        The extracted per-species statistics.
    """

    # Extract the per-species statistics from the species metadata
    per_species_metadata_statistics = extract_per_species_metadata_statistics(
        species_metadata
    )

    # Use the build-in polars describe method to calculate the general metadata
    # statistics
    general_metadata_statistics = per_species_metadata_statistics.describe()

    # Calculate the general metadata statistics
    general_metadata_statistics = {
        "total_species_recordings": general_metadata_statistics.filter(
            pl.col("statistic") == "max"
        )
        .select(pl.col("num_recordings"))
        .item(),
        "total_clean_species_recordings": general_metadata_statistics.filter(
            pl.col("statistic") == "max"
        )
        .select(pl.col("num_clean_recordings"))
        .item(),
        "25th_percentile_species_recordings": general_metadata_statistics.filter(
            pl.col("statistic") == "25%"
        )
        .select(pl.col("num_recordings"))
        .item(),
        "25th_percentile_clean_species_recordings": general_metadata_statistics.filter(
            pl.col("statistic") == "25%"
        )
        .select(pl.col("num_clean_recordings"))
        .item(),
        "avg_species_recordings": general_metadata_statistics.filter(
            pl.col("statistic") == "mean"
        )
        .select(pl.col("num_recordings"))
        .item(),
        "avg_clean_species_recordings": general_metadata_statistics.filter(
            pl.col("statistic") == "mean"
        )
        .select(pl.col("num_clean_recordings"))
        .item(),
        "median_species_recordings": general_metadata_statistics.filter(
            pl.col("statistic") == "50%"
        )
        .select(pl.col("num_recordings"))
        .item(),
        "median_clean_species_recordings": general_metadata_statistics.filter(
            pl.col("statistic") == "50%"
        )
        .select(pl.col("num_clean_recordings"))
        .item(),
        "75th_percentile_species_recordings": general_metadata_statistics.filter(
            pl.col("statistic") == "75%"
        )
        .select(pl.col("num_recordings"))
        .item(),
        "75th_percentile_clean_species_recordings": general_metadata_statistics.filter(
            pl.col("statistic") == "75%"
        )
        .select(pl.col("num_clean_recordings"))
        .item(),
        "max_species_recordings": general_metadata_statistics.filter(
            pl.col("statistic") == "max"
        )
        .select(pl.col("num_recordings"))
        .item(),
        "max_clean_species_recordings": general_metadata_statistics.filter(
            pl.col("statistic") == "max"
        )
        .select(pl.col("num_clean_recordings"))
        .item(),
        "min_species_recordings": general_metadata_statistics.filter(
            pl.col("statistic") == "min"
        )
        .select(pl.col("num_recordings"))
        .item(),
        "min_clean_species_recordings": general_metadata_statistics.filter(
            pl.col("statistic") == "min"
        )
        .select(pl.col("num_clean_recordings"))
        .item(),
        "total_species_recording_length": general_metadata_statistics.filter(
            pl.col("statistic") == "max"
        )
        .select(pl.col("total_recording_length"))
        .item(),
        "total_clean_species_recording_length": general_metadata_statistics.filter(
            pl.col("statistic") == "max"
        )
        .select(pl.col("total_clean_recording_length"))
        .item(),
        "25th_percentile_species_recording_length": general_metadata_statistics.filter(
            pl.col("statistic") == "25%"
        )
        .select(pl.col("total_recording_length"))
        .item(),
        "25th_percentile_clean_species_recording_length": general_metadata_statistics.filter(
            pl.col("statistic") == "25%"
        )
        .select(pl.col("total_clean_recording_length"))
        .item(),
        "avg_species_recording_length": general_metadata_statistics.filter(
            pl.col("statistic") == "mean"
        )
        .select(pl.col("total_recording_length"))
        .item(),
        "avg_clean_species_recording_length": general_metadata_statistics.filter(
            pl.col("statistic") == "mean"
        )
        .select(pl.col("total_clean_recording_length"))
        .item(),
        "median_species_recording_length": general_metadata_statistics.filter(
            pl.col("statistic") == "50%"
        )
        .select(pl.col("total_recording_length"))
        .item(),
        "median_clean_species_recording_length": general_metadata_statistics.filter(
            pl.col("statistic") == "50%"
        )
        .select(pl.col("total_clean_recording_length"))
        .item(),
        "75th_percentile_species_recording_length": general_metadata_statistics.filter(
            pl.col("statistic") == "75%"
        )
        .select(pl.col("total_recording_length"))
        .item(),
        "75th_percentile_clean_species_recording_length": general_metadata_statistics.filter(
            pl.col("statistic") == "75%"
        )
        .select(pl.col("total_clean_recording_length"))
        .item(),
        "max_species_recording_length": general_metadata_statistics.filter(
            pl.col("statistic") == "max"
        )
        .select(pl.col("total_recording_length"))
        .item(),
        "max_clean_species_recording_length": general_metadata_statistics.filter(
            pl.col("statistic") == "max"
        )
        .select(pl.col("total_clean_recording_length"))
        .item(),
        "min_species_recording_length": general_metadata_statistics.filter(
            pl.col("statistic") == "min"
        )
        .select(pl.col("total_recording_length"))
        .item(),
        "min_clean_species_recording_length": general_metadata_statistics.filter(
            pl.col("statistic") == "min"
        )
        .select(pl.col("total_clean_recording_length"))
        .item(),
    }

    return general_metadata_statistics


def extract_per_species_metadata_statistics(
    species_metadata: pl.DataFrame,
) -> pl.DataFrame:
    """Extract some per-species statistics from the species metadata.

    The following statistics are extracted:
    - Number of recordings per species ("num_recordings").
    - Total recording duration per species ("total_recording_length").
    - Number of "clean" recordings per species ("num_clean_recordings").
    - Total recording duration of "clean" recordings per species
    ("total_clean_recording_length").

    Note: Here, we define a "clean" recording as one where there are no other species
    present as background species in the recording.

    Parameters
    ----------
    species_metadata
        The species metadata DataFrame.

    Returns
    -------
    pl.DataFrame
        The extracted per-species statistics.
    """

    # Count the number of recordings per species
    num_recordings_per_species = species_metadata.group_by(pl.col("english_name")).agg(
        pl.len().alias("num_recordings")
    )

    # Calculate the total recording duration per species
    total_duration_per_species = species_metadata.group_by(pl.col("english_name")).agg(
        pl.duration(
            minutes=pl.col("recording_length").str.split(":").list.get(0),
            seconds=pl.col("recording_length").str.split(":").list.get(1),
        )
        .dt.total_seconds()
        .alias("total_recording_length")
        .sum()
    )

    # Count the number of "clean" recordings per species
    num_clean_recordings_per_species = (
        species_metadata.filter(pl.col("background_species") == "[]")
        .group_by(pl.col("english_name"))
        .agg(pl.len().alias("num_clean_recordings"))
    )

    # Calculate the total recording duration per species
    total_clean_duration_per_species = (
        species_metadata.filter(pl.col("background_species") == "[]")
        .group_by(pl.col("english_name"))
        .agg(
            pl.duration(
                minutes=pl.col("recording_length").str.split(":").list.get(0),
                seconds=pl.col("recording_length").str.split(":").list.get(1),
            )
            .dt.total_seconds()
            .alias("total_clean_recording_length")
            .sum()
        )
    )

    # Merge the metadata characteristics into a single DataFrame
    metadata_characteristics = (
        num_recordings_per_species.join(
            num_clean_recordings_per_species, on="english_name", how="left"
        )
        .join(total_duration_per_species, on="english_name", how="left")
        .join(total_clean_duration_per_species, on="english_name", how="left")
    )

    return metadata_characteristics.sort("english_name")
