# dags/include/download.py
import os
import time
import logging
import shutil
from pathlib import Path
from typing import Optional
import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class DownloadError(Exception):
    pass


def build_wikimedia_url(year: int, month: int, day: int, hour: int) -> str:

    y = int(year)
    m = int(month)
    d = int(day)
    h = int(hour)
    ystr = f"{y:04d}"
    mstr = f"{m:02d}"
    dstr = f"{d:02d}"
    hstr = f"{h:02d}"
    filename = f"pageviews-{ystr}{mstr}{dstr}-{hstr}0000.gz"
    url = f"https://dumps.wikimedia.org/other/pageviews/{ystr}/{ystr}-{mstr}/{filename}"
    return url


def download_pageviews(
    year: int,
    month: int,
    day: int,
    hour: int,
    output_dir: str = "/opt/airflow/output",
    timeout: int = 30,
    max_retries: int = 5,
    backoff_factor: float = 1.5,
    chunk_size: int = 1024 * 64,
    force_redownload: bool = True,
) -> str:
    url = build_wikimedia_url(year, month, day, hour)
    ystr = f"{int(year):04d}"
    mstr = f"{int(month):02d}"
    dstr = f"{int(day):02d}"
    hstr = f"{int(hour):02d}"
    filename = f"pageviews-{ystr}{mstr}{dstr}-{hstr}0000.gz"

    out_dir = Path(output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    dest_path = out_dir / filename
    temp_path = out_dir / f".{filename}.part"

    if dest_path.exists():
        if force_redownload:
            logger.info("File exists and force_redownload=True -> removing existing file: %s", dest_path)
            try:
                dest_path.unlink()
            except Exception:
                logger.exception("Failed to remove existing file: %s", dest_path)
                raise DownloadError(f"Failed to remove existing file: {dest_path}")
        else:
            logger.info("File already exists, skipping download: %s", dest_path)
            return str(dest_path)

    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            logger.info("Attempt %s: downloading %s -> %s", attempt, url, temp_path)
            with requests.get(url, stream=True, timeout=timeout) as r:
                if r.status_code == 404:
                    msg = f"File not found on server (404): {url}"
                    logger.error(msg)
                    raise DownloadError(msg)
                r.raise_for_status()

                # stream to temp file
                with open(temp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:  # filter out keep-alive chunks
                            f.write(chunk)
                # Basic validation: file size
                file_size = temp_path.stat().st_size
                if file_size == 0:
                    raise DownloadError("Downloaded file is empty")

                # atomic move to final destination
                logger.info("Download finished, moving %s -> %s", temp_path, dest_path)
                try:
                    # If dest exists somehow, remove
                    if dest_path.exists():
                        dest_path.unlink()
                    shutil.move(str(temp_path), str(dest_path))
                except Exception:
                    logger.exception("Failed to move temp file to destination")
                    raise

                logger.info("Successfully downloaded %s (%d bytes)", dest_path, file_size)
                return str(dest_path)

        except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as exc:
            logger.warning("Download attempt %s failed: %s", attempt, exc)
            # remove partial temp file if exists
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    logger.debug("Could not remove temp file %s", temp_path)
            if attempt < max_retries:
                sleep_time = backoff_factor ** attempt
                logger.info("Retrying after %.1f seconds...", sleep_time)
                time.sleep(sleep_time)
            else:
                logger.error("Exceeded max_retries (%s)", max_retries)
                raise DownloadError(f"Failed to download {url} after {max_retries} attempts") from exc

        except DownloadError:
            # re-raise DownloadError without backoff loop handling
            raise

        except Exception as exc:
            logger.exception("Unexpected error while downloading %s", url)
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    logger.debug("Could not remove temp file %s", temp_path)
            raise DownloadError(f"Unexpected error while downloading {url}: {exc}") from exc

    
    raise DownloadError(f"Failed to download {url}")


# convenience wrapper for Airflow 
def download_pageviews_for_datetime(dt, output_dir: Optional[str] = None, **kwargs) -> str:

    output_dir = output_dir or os.getenv("OUTPUT_PATH", "/opt/airflow/output")
    return download_pageviews(
        year=dt.year,
        month=dt.month,
        day=dt.day,
        hour=dt.hour,
        output_dir=output_dir,
        **kwargs,
    )
