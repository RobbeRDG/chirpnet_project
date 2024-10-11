from pydantic_settings import BaseSettings


class Config(BaseSettings):
    """The configuration settings for the ChirpNet project.

    Attributes
    ----------
    RAW_SPECIES_LIST_PATH : str
        The path to the raw species list (European Red List of Birds 2021).

    """

    RAW_SPECIES_LIST_PATH: str = (
        "/workspaces/chirpnet/resources/ERLoB2021_categories.xlsx"
    )
