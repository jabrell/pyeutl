import os
import requests

MOST_RECENT_YEAR = 2024

URLS = {
    MOST_RECENT_YEAR: "https://euets-info-public.s3.eu-central-1.amazonaws.com/eutl_2024_202410.zip",
    20245: "https://euets-info-public.s3.eu-central-1.amazonaws.com/eutl_2024_202405.zip",
    2023: "https://euets-info-public.s3.eu-central-1.amazonaws.com/eutl_2023.zip",
    2022: "https://euets-info-public.s3.eu-central-1.amazonaws.com/eutl_2022.zip",
    2021: "https://euets-info-public.s3.eu-central-1.amazonaws.com/eutl_2021.zip",
}


def download_data(
    year: int = MOST_RECENT_YEAR,
    fn_out: str | None = None,
) -> str:
    """Download data from the EUTL website for the given year.

    Args:
        year (int, optional): Year to download data for. Defaults to MOST_RECENT_YEAR.
        fn_out (str, optional): Filename to save the data to. If None, file will be saved
            to the current working directory as eutl_{year}.zip.
            Defaults to None.

    Returns:
        str: Path to the downloaded file.
    """
    if fn_out is None:
        fn_out = os.path.join(os.getcwd(), f"eutl_{year}.zip")
    dir_out = os.path.abspath(os.path.join(os.path.abspath(__file__), "../data/"))
    r = requests.get(URLS[year], allow_redirects=True)
    open(fn_out, "wb").write(r.content)
    return fn_out
