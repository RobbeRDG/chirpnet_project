import json
from os.path import join
from chirpnet.config import Config
from chirpnet.data_gathering import ChirpNetDownloader

from prefect import flow  # type: ignore


@flow(log_prints=True)
def download_species_data():
    """Download species data for a specific species list from the Xeno-Canto API.

    This function reads the configuration from the `download_species_data_config.json`
    file from the `resources` directory and downloads the species data for the species
    list specified in the configuration along with the other configurations.

    """
    with open(
        join(
            Config.PROJECT_BASE_DIR_PATH, "resources/download_species_data_config.json"
        )
    ) as f:
        config = json.load(f)

    ChirpNetDownloader.download_species_data(  # type: ignore
        config["species_list_path"],
        config["recorded_year"],
        config["quality"],
        config["max_pages"],
        config["downloader_base_path"],
    )
