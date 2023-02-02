import google.cloud.logging
import google.cloud.bigquery
import google.cloud.storage

import os

import pandas as pd

from canvas_data.api import CanvasDataAPI

if __name__ == "__main__":
    try:
        logging_client = google.cloud.logging.Client()
        logging_client.setup_logging()
        logger = logging_client.logger("canvas_logger")

        storage_client = google.cloud.storage.Client()
        bucket = storage_client.get_bucket(os.enviorn['BUCKET_NAME'])

        bigquery_client = google.cloud.bigquery.Client()

        logger.log_text("Beginning Canvas Bigquery Sync", severity="ALERT")

        cd = CanvasDataAPI(api_key=os.environ['API_KEY'], api_secret=os.environ['API_SECRET'])

        schema = cd.get_schema('latest', key_on_tablenames=True)
        try:
            for table_name in schema:
                local_data_filename = cd.get_data_for_table(table_name=table_name)
            
            logger.log_text("Canvas Data Dumped", severity="INFO")
        except Exception as e:
            logger.log_text(f"Canvas Data Dump Failed: {e}", severity="ERROR")

        logger.log_text("Converting files to csv", severity="INFO")
        
        for table_name in schema:
            try:
                df = pd.read_csv(f"data/{table_name}.txt", sep="\t")
                table_columns = schema[table_name]['columns']
                df.columns = [column["name"] for column in table_columns]
                df.to_csv(f"csvs/{table_name}.csv", index=False)
            except Exception as e:
                logger.log_text(f"File Conversion Failed: {e}", severity="ERROR")

        logger.log_text("Adding CSV files to Cloud Storage", severity="INFO")

        for table_name in schema:
            try:
                bucket.blob(f"{table_name}.csv").upload_from_filename(f"csvs/{table_name}.csv")
            except Exception as e:
                logger.log_text(f"Cloud Storage Upload Failed: {e}", severity="ERROR")

        logger.log_text("Loading CSV files to Bigquery", severity="INFO")

        for table_name in schema:
            try:
                table_id = f"dtsdatastore.CanvasDataFlatFiles.{table_name}"
                job_config = google.cloud.bigquery.LoadJobConfig(source_format=google.cloud.bigquery.SourceFormat.CSV, autodetect=True)
                uri = f"gs://canvas_sync_bucket/{table_name}.csv"
                
                load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)
                load_job.result()  # Waits for the job to complete.

                destination_table = bigquery_client.get_table(table_id)  # Make an API request.
                logger.log_text("Loaded {} rows.".format(destination_table.num_rows), severity="INFO")
            except Exception as e:
                logger.log_text(f"Bigquery Load Job Failed: {e}", severity="ERROR")

        logger.log_text("Finished Canvas Data Sync", severity="INFO")
    except Exception as e:
        logger.log_text(f"Canvas Data Sync Failed: {e}", severity="ERROR")
