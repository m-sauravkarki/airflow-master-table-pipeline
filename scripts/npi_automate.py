import os
import shutil
import zipfile
import requests
from datetime import datetime
from urllib.parse import urlparse
from dateutil.relativedelta import relativedelta

from duckdb_provider.hooks.duckdb_hook import DuckDBHook

try:
    from .fileutils import get_logger, log_step, log_file_info, log_dir_contents
    from .manifest import write_manifest
except ImportError:
    from fileutils import get_logger, log_step, log_file_info, log_dir_contents
    from manifest import write_manifest

logger = get_logger("nppes.npi_automate")

'''Global variables'''
# Path to the zip file
# zip_file_path = r'C:\Users\Saurav.Karki\Documents\Maitri\Python_Notebooks\downloaded_file.zip'
parquet_output_dir = 'parquet_output_dir/nppes/'

def dynamic_base_url():
    # Get current date
    today = datetime.now()

    # Subtract exactly one month to handle year rollovers automatically
    last_month_date = today - relativedelta(months=1)

    # Extract the name and year
    month_name = last_month_date.strftime("%B")
    year = last_month_date.year

    # Dynamic base url
    base_url = f"https://download.cms.gov/nppes/NPPES_Data_Dissemination_{month_name}_{year}_V2.zip"
    logger.info(f"Resolved CMS NPPES URL for {month_name} {year}: {base_url}")

    return base_url


def request_url():
    try:
        url = dynamic_base_url()
        with log_step(logger, f"download zip from {url}"):
            response = requests.get(url)
            # response.raise_for_status()

            if response.status_code == 404:
                logger.error("The requested URL was not found.")
                return "The requested URL was not found."

            elif response.status_code == 200:
                parsed_url = urlparse(url)
                filename = os.path.basename(parsed_url.path)

                # Removing any existing .zip files in the current directory
                for f in os.listdir('.'):
                    if f.endswith('.zip'):
                        os.remove(f)
                        logger.info(f"Removed existing ZIP file: {f}")

                with open(filename, "wb") as file:
                    dump_file = file.write(response.content)
                logger.info(f"Working directory: {os.getcwd()}")
                log_file_info(logger, filename, label="downloaded zip")
                return dump_file

            else:
                logger.error(f"Request failed with status code: {response.status_code}")
                return f"Request failed with status code: {response.status_code}"

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"An error occurred: {req_err}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")



def process_extracted_npi_data_v2():

    parquet_output_dir = "/opt/airflow/data/parquet"
    temp_dir = "/opt/airflow/data/temp"

    os.makedirs(parquet_output_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    logger.info(f"Output parquet dir: {parquet_output_dir}")
    logger.info(f"DuckDB temp dir: {temp_dir}")
    log_dir_contents(logger, parquet_output_dir, suffix='.parquet', label="existing parquet")

    zip_file_path = None

    for f in os.listdir('.'):
        if f.endswith('.zip'):
            zip_file_path = f
            break

    if not zip_file_path:
        logger.error("No zip file found in working directory - did the download step run?")
        return

    log_file_info(logger, zip_file_path, label="source zip")

    hook = DuckDBHook.get_hook("duckdb_default")
    con = hook.get_conn()

    # Apply engine settings declared in the Airflow Connection's Extra JSON.
    # Keeps tuning (memory_limit, threads, temp_directory) editable from the UI.
    extra = hook.get_connection("duckdb_default").extra_dejson
    for key, value in extra.items():
        if isinstance(value, str):
            con.execute(f"SET {key}='{value}'")
        else:
            con.execute(f"SET {key}={value}")
    logger.info(f"DuckDB configured from Airflow connection 'duckdb_default': {extra}")

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:

        for file_name in zip_ref.namelist():

            if (
                file_name.endswith('.csv')
                and 'npidata_pfile' in file_name
                and 'fileheader' not in file_name
            ):

                output_path = os.path.join(
                    parquet_output_dir,
                    file_name.replace('.csv', '.parquet')
                )

                # Idempotency: if this CMS dump was already processed, keep the
                # existing parquet (and its U/A flags) and skip the rebuild.
                if os.path.exists(output_path):
                    logger.info(
                        f"Output parquet already exists for this CMS dump - "
                        f"skipping rebuild to preserve existing npi_updated_flag values: {output_path}"
                    )
                    continue

                with log_step(logger, f"convert {file_name} -> parquet"):
                    extracted_path = zip_ref.extract(
                        file_name,
                        path=temp_dir
                    )
                    log_file_info(logger, extracted_path, label="extracted csv")

                    previous_parquet = None
                    all_parquets = [
                        os.path.join(parquet_output_dir, f)
                        for f in os.listdir(parquet_output_dir)
                        if f.endswith('.parquet')
                    ]
                    logger.info(f"Output path for this run: {output_path}")
                    logger.info(f"Parquet files present in dir: {all_parquets or '[]'}")
                    if all_parquets:
                        previous_parquet = max(all_parquets, key=os.path.getmtime)
                        logger.info(f"Diffing names against previous parquet: {previous_parquet}")
                    else:
                        logger.info("No previous parquet found - all rows will be flagged 'A'.")

                    if previous_parquet:
                        select_sql = f"""
                            SELECT
                                new.*,
                                CASE
                                    WHEN old."NPI" IS NULL THEN 'A'
                                    WHEN new."Provider First Name" IS DISTINCT FROM old."Provider First Name"
                                      OR new."Provider Last Name (Legal Name)" IS DISTINCT FROM old."Provider Last Name (Legal Name)"
                                      OR new."Provider Middle Name" IS DISTINCT FROM old."Provider Middle Name"
                                      OR new."Provider Name Prefix Text" IS DISTINCT FROM old."Provider Name Prefix Text"
                                    THEN 'U'
                                    ELSE 'A'
                                END AS npi_updated_flag
                            FROM read_csv(
                                '{extracted_path}',
                                delim=',',
                                header=true,
                                all_varchar=true
                            ) AS new
                            LEFT JOIN (
                                SELECT "NPI",
                                       "Provider First Name",
                                       "Provider Last Name (Legal Name)",
                                       "Provider Middle Name",
                                       "Provider Name Prefix Text"
                                       
                                FROM read_parquet('{previous_parquet}')
                            ) AS old
                                ON new."NPI" = old."NPI"
                        """
                    else:
                        select_sql = f"""
                            SELECT
                                *,
                                'A' AS npi_updated_flag
                            FROM read_csv(
                                '{extracted_path}',
                                delim=',',
                                header=true,
                                all_varchar=true
                            )
                        """

                    logger.info(f"Writing parquet to {output_path}")
                    con.execute(f"""
                        COPY (
                            {select_sql}
                        )
                        TO '{output_path}'
                        (FORMAT PARQUET, COMPRESSION SNAPPY)
                    """)
                    log_file_info(logger, output_path, label="written parquet")

                    if previous_parquet and os.path.exists(previous_parquet):
                        os.remove(previous_parquet)
                        logger.info(f"Removed previous parquet: {previous_parquet}")

                    os.remove(extracted_path)
                    logger.info(f"Removed extracted csv: {extracted_path}")

    if os.path.exists(zip_file_path):
        os.remove(zip_file_path)
        logger.info(f"Removed source zip: {zip_file_path}")

    # Final sweep of the temp dir to clear any leftovers from this or prior failed runs
    if os.path.isdir(temp_dir):
        for entry in os.listdir(temp_dir):
            entry_path = os.path.join(temp_dir, entry)
            try:
                if os.path.isfile(entry_path):
                    os.remove(entry_path)
                else:
                    shutil.rmtree(entry_path)
                logger.info(f"Cleaned temp entry: {entry_path}")
            except OSError as cleanup_err:
                logger.warning(f"Could not clean {entry_path}: {cleanup_err}")

    logger.info("Processing and conversion to Parquet completed.")


def write_nppes_manifest(airflow_run_id: str | None = None) -> str:
    """Generate /opt/airflow/data/parquet/_manifest.json for the NPPES dataset.

    Thin wrapper over manifest.write_manifest that supplies NPPES-specific
    source info (URL, filename, publisher).
    """
    source_url = dynamic_base_url()
    return write_manifest(
        logger=logger,
        dataset="nppes",
        parquet_dir="/opt/airflow/data/parquet",
        source={
            "url": source_url,
            "filename": os.path.basename(urlparse(source_url).path),
            "publisher": "CMS NPPES",
        },
        schema_version="v1",
        airflow_run_id=airflow_run_id,
    )


if __name__ == '__main__':
    pull_data = request_url()
    extract_zipped_data = process_extracted_npi_data_v2()
    write_nppes_manifest()

    
