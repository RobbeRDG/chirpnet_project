from prefect import flow  # type: ignore
from chirpnet.cli import download_species_data


if __name__ == "__main__":
    download_species_data.serve(name="local_chirpnet_data_download") # type: ignore
