from cantopy import Query, FetchManager, DownloadManager
import pandas as pd
from os import path
import os
import shutil


class ChirpNetDownloader:

    @classmethod
    def download_species_data(
        cls,
        species_list_path: str,
        recorded_year: int,
        quality: str,
        max_pages: int,
        downloader_base_path: str,
    ) -> None:
        """Download the XenoCanto recording data for a list of species.

        Parameters
        ----------
        species_list_path
            The path to the CSV file containing the list of species to download. This
            CSV file should have a column named "Common Name" containing the common
            names of the species to download.
        recorded_year
            The year to filter the recordings by. Only recordings from this year will
            be downloaded.
        quality
            The quality of the recordings to download. This can be one of "A", "B", or
            "C" to download recordings of quality A, B, or C respectively.
        max_pages
            The maximum number of pages to fetch from the XenoCanto API for each
            species.
        downloader_base_path
            The base path where the downloaded data will be stored. The data will be
            stored in a subfolder of this base path, named after the species list, year,
            quality and max_pages.

        """

        # Check the quality parameter
        if quality not in ["A", "B", "C"]:
            raise ValueError(
                f"Quality parameter must be one of 'A', 'B', or 'C'. Got: {quality}."
            )

        download_manager, download_folder_path = cls._initialize_download_manager(
            species_list_path,
            recorded_year,
            quality,
            max_pages,
            downloader_base_path,
        )

        cls._initialize_metadata(species_list_path, download_folder_path)

        # Load the species list and extract only the common names of the species
        species_list = pd.read_csv(species_list_path, index_col=None)[  # type: ignore
            "Common Name"
        ].values.tolist()

        # Filter out the already downloaded species
        species_list = cls._filter_already_downloaded_species(
            species_list, download_folder_path  # type: ignore
        )

        print(f"Downloading data for {len(species_list)} species ...")

        # Download the data for each species in the list
        for species_name in species_list:

            cls._download_single_species_data(
                download_manager, species_name, recorded_year, quality, max_pages
            )

            cls._update_already_downloaded_metadata(species_name, download_folder_path)

        print("Download complete.")

    @staticmethod
    def _initialize_download_manager(
        species_list_path: str,
        recorded_year: int,
        quality: str,
        max_pages: int,
        downloader_base_path: str,
    ) -> tuple[DownloadManager, str]:
        """Initialize a download manager instance for a species list download request.

        This download manager instance will be used to download the recording data for
        the species in the species list.

        The download manager's base path will be set to a subfolder of the
        ChirpNetDownloader's base path with the subfolder's name set to the specific
        species list, year and quality for which to download recording data, in the
        format: <species_list_name>_y<year>_q<quality>_mp<max_pages>.

        Parameters
        ----------
        species_list_path
            The path to the CSV file containing the list of species to download.
        recorded_year
            The year to filter the recordings by.
        quality
            The quality of the recordings to download.
        max_pages
            The maximum number of pages to fetch from the XenoCanto API for each
            species.
        downloader_base_path
            The base path where the downloaded data will be stored.

        Returns
        -------
        DownloadManager
            The initialized download manager instance.
        str
            The folder path where the downloaded data will be stored for the
            specific species list query.

        """

        print("Initializing download manager ...")

        species_list_name = path.basename(species_list_path).split(".")[0]

        download_folder_name = (
            f"{species_list_name}_y{str(recorded_year)}_q{quality}_mp{max_pages}"
        )

        download_folder_path = path.join(downloader_base_path, download_folder_name)
        if not path.exists(download_folder_path):
            os.mkdir(download_folder_path)

        return DownloadManager(download_folder_path), download_folder_path

    @staticmethod
    def _initialize_metadata(
        species_list_path: str,
        download_folder_path: str,
    ):
        """Initialize a metadata folder for a species list download request.

        The metadata folder contains the following files:
        - species_list.csv: The list of species for which data is being downloaded.
        - already_downloaded.csv: A CSV file containing the names of the species for
            which data has already been downloaded.

        Parameters
        ----------
        species_list_path
            The path to the CSV file containing the list of species to download.
        download_folder_path
            The folder path where the downloaded data will be stored for the species
            list download request.

        """

        print("Initializing metadata folder ...")

        metadata_folder_path = path.join(download_folder_path, "metadata")

        if path.exists(metadata_folder_path):
            print("Metadata folder already exists. Skipping initialization.")
            return

        os.mkdir(metadata_folder_path)

        # Copy the species list to the metadata folder
        species_list_name = path.basename(species_list_path).split(".")[0]
        shutil.copy(
            species_list_path,
            path.join(metadata_folder_path, f"{species_list_name}.csv"),
        )

        # Create the already_downloaded.csv file
        pd.DataFrame(columns=["Species"]).to_csv(
            path.join(metadata_folder_path, "already_downloaded.csv"), index=False
        )

    @staticmethod
    def _filter_already_downloaded_species(
        species_list: list[str],
        download_folder_path: str,
    ) -> list[str]:
        """Filter out the species that have already been downloaded for a species list
        download request.

        Parameters
        ----------
        species_list
            The list of species to filter.
        download_folder_path
            The folder path where the downloaded data is stored for the species list
            download request.

        Returns
        -------
        list[str]
            The filtered list of species that have not been downloaded yet.

        """

        metadata_folder_path = path.join(download_folder_path, "metadata")

        # Load the already downloaded species metadata
        already_downloaded_path = path.join(
            metadata_folder_path, "already_downloaded.csv"
        )
        already_downloaded = pd.read_csv(already_downloaded_path, index_col=None)  # type: ignore
        already_downloaded_species = already_downloaded["Species"].values.tolist()  # type: ignore

        # Filter out the already downloaded species
        return list(
            filter(
                lambda species: species not in already_downloaded_species,
                species_list,
            )
        )

    @staticmethod
    def _download_single_species_data(
        download_manager: DownloadManager,
        species_name: str,
        recorded_year: int,
        quality: str,
        max_pages: int,
    ) -> None:
        """Download the recording data for a single species.

        Parameters
        ----------
        download_manager
            The download manager instance to use for downloading the data.
        species_name
            The name of the species to download.
        recorded_year
            The year to filter the recordings by.
        quality
            The quality of the recordings to download.
        max_pages
            The maximum number of pages to fetch from the XenoCanto API for the
            species.

        """

        print(f"Downloading recordings data for species: {species_name} ...")

        # Construct the query object
        query = Query(
            species_name=species_name,
            recorded_year=str(recorded_year),
            quality=quality,
        )

        print(f"Sending Query for species: {species_name} ...")

        # Fetch the data for the given species
        query_result = FetchManager.send_query(query, max_pages=max_pages)

        print(
            f"Query for species: {species_name} returned {len(query_result.get_all_recordings())} recordings."
        )
        print(f"Downloading recordings data for species: {species_name} ...")

        # Download the data contained in the query result
        download_manager.download_all_recordings_in_queryresult(query_result)

    @staticmethod
    def _update_already_downloaded_metadata(
        species_name: str,
        download_folder_path: str,
    ) -> None:
        """Update the already-downloaded metadata file for a species list download
        request.

        Parameters
        ----------
        species_name
            The name of the species for which data has been downloaded.
        download_folder_path
            The folder path where the downloaded data is stored for the species list
            download request.

        """

        metadata_folder_path = path.join(download_folder_path, "metadata")

        # Load the already downloaded metadata
        already_downloaded_path = path.join(
            metadata_folder_path, "already_downloaded.csv"
        )
        already_downloaded = pd.read_csv(already_downloaded_path, index_col=None)  # type: ignore

        # Add the species name to the already downloaded metadata
        already_downloaded = pd.concat(
            [already_downloaded, pd.DataFrame({"Species": [species_name]})],
            ignore_index=True,
        )

        already_downloaded.to_csv(already_downloaded_path, index=False)
