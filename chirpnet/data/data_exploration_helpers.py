import pandas as pd
import os
from glob import glob
from os.path import join


def get_combined_full_species_metadata_across_collections(
    base_collections_dir_path: str,
) -> pd.DataFrame:
    """
    Get the full species metadata of all species across all collections of species
    recordings.

    Returns
    -------
    pd.DataFrame
        The full species metadata across all collections.

    """

    collection_names = os.listdir(base_collections_dir_path)

    # Get the metadata for each collection
    full_collections_metadata_list: list[pd.DataFrame] = []
    for collection_name in collection_names:
        full_collections_metadata_list.append(
            get_full_species_metadata_for_collection(
                join(base_collections_dir_path, collection_name)
            )
        )

    return pd.concat(full_collections_metadata_list, axis=0)


def get_full_species_metadata_for_collection(collection_dir_path: str) -> pd.DataFrame:
    """
    Get the full species metadata for a collection of species recordings.

    Parameters
    ----------
    collection_dir_path : str
        The path to the directory containing the collection.

    Returns
    -------
    pd.DataFrame
        The full species metadata for the collection.

    """

    # Each collection contains multiple species directories
    species_dirs = os.listdir(collection_dir_path)

    # Remove the unneeded metadata directory
    species_dirs.remove("metadata")

    full_species_metadata_list: list[pd.DataFrame] = []
    for species_dir in species_dirs:
        # Skip if empty directory
        if not os.listdir(join(collection_dir_path, species_dir)):
            continue

        species_metadata_path = glob(
            join(collection_dir_path, species_dir, "*recording_metadata.csv")
        )[0]

        full_species_metadata_list.append(
            pd.read_csv(species_metadata_path, index_col=None)  # type: ignore
        )

    return pd.concat(full_species_metadata_list, axis=0)
