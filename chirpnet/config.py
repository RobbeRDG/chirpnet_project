from pydantic_settings import BaseSettings


class Config(BaseSettings):
    """
    A class to store the configuration of the Chirpnet project.

    Attributes
    ----------
    DOWNLOADER_MAX_WORKERS : int
        The maximum number of workers to use in the chirpnet downloader when downloading
        species data.

    """

    # Chirpnet downloader configurations
    DOWNLOADER_MAX_WORKERS: int = 4
