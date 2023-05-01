import google.cloud.logging
import google.cloud.bigquery
import google.cloud.storage

import logging
import os
import shutil
import pandas as pd

from canvas_data.api import CanvasDataAPI

type_conversion_map = {   
    'bigint': 'FLOAT64',
    'boolean': 'BOOLEAN',
    'date': 'DATE',
    'datetime': 'DATE',
    'double precision': 'FLOAT64',
    'enum': 'STRING',
    'guid': 'STRING',
    'int': 'FLOAT64',
    'integer': 'FLOAT64',
    'text': 'STRING',
    'timestamp': 'TIMESTAMP',
    'varchar': 'STRING'
}

def app():
    try:
        logging_client = google.cloud.logging.Client()
        logging_client.setup_logging()
        logging.basicConfig(level=logging.INFO)

        #logger = logging_client.logger("canvas_logger")

        storage_client = google.cloud.storage.Client()
        bucket = storage_client.get_bucket(os.environ['BUCKET_NAME'])

        bigquery_client = google.cloud.bigquery.Client()

        #logger.log_text("Beginning Canvas Bigquery Sync", severity="ALERT")
        logging.info("Beginning Canvas Bigquery Sync")

        cd = CanvasDataAPI(api_key=os.environ['API_KEY'], 
                           api_secret=os.environ['API_SECRET'])

        schema = cd.get_schema('latest', key_on_tablenames=True)
        try:
            for table_name in sorted(schema):
                if table_name != "requests" and 'catalog' not in table_name:
                    try:
                        local_data_filename = cd.get_data_for_table(table_name=table_name)

                        with open(f"data/{table_name}.txt", "r") as f:
                            data = f.read()
                            data = data.replace("\\N", "")
                            data = data.replace("\"", "")
                        
                        with open(f"data/{table_name}.txt", "w") as f:
                            f.write(data)

                        df = pd.read_csv(f"data/{table_name}.txt", sep="\t", low_memory=False)
                        table_columns = schema[table_name]['columns']
                        df.columns = [column["name"] for column in table_columns]
                        df.to_csv(f"csvs/{table_name}.csv", index=False)
            
                        bucket.blob(f"{table_name}.csv").upload_from_filename(f"csvs/{table_name}.csv")

                        #logger.log_text(f"Cloud Storage sync complete for {table_name}", severity="INFO")
                        logging.info(f"Cloud Storage sync complete for {table_name}")
                    except Exception as e:
                        #logger.log_text(f"File sync failed for {table_name}: {e}", severity="ERROR")
                        logging.error(f"File sync failed for {table_name}: {e}")
                    finally:
                        shutil.rmtree("data", ignore_errors=True)
                        shutil.rmtree("downloads", ignore_errors=True)
                        if os.path.exists(f"csvs/{table_name}.csv"):
                            os.remove(f"csvs/{table_name}.csv")
            for table_name in sorted(schema):
                if table_name != "requests":
                    try:
                        table_columns = schema[table_name]['columns']
                        table_id = f"{os.environ['PROJECT_NAME']}.{os.environ['TABLE_NAME']}.{table_name}"
                        
                        job_config = google.cloud.bigquery.LoadJobConfig(source_format=google.cloud.bigquery.SourceFormat.CSV, autodetect=True)
                        
                        job_config.write_disposition = google.cloud.bigquery.WriteDisposition.WRITE_TRUNCATE
                        job_config.schema = [
                            google.cloud.bigquery.SchemaField(column["name"], 
                                                              type_conversion_map.get(column["type"],"STRING"),
                                                              description=column["description"],
                                                              ) for column in table_columns
                        ]

                        uri = f"gs://{os.environ['BUCKET_NAME']}/{table_name}.csv"
                        
                        load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)
                        load_job.result()  # Waits for the job to complete.

                        destination_table = bigquery_client.get_table(table_id)  # Make an API request.
                        #logger.log_text("Loaded {} rows: {}.".format(destination_table.num_rows, table_name), severity="INFO")
                        logging.info("Loaded {} rows: {}.".format(destination_table.num_rows, table_name))
                    except Exception as e:
                        #logger.log_text(f"Bigquery sync failed for {table_name}: {e}", severity="ERROR")
                        logging.error(f"Bigquery sync failed for {table_name}: {e}")
        except Exception as e:
            #logger.log_text(f"Canvas Data Dump Failed: {e}", severity="ERROR")
            logging.error(f"Canvas Data Dump Failed: {e}")

        #logger.log_text("Canvas Bigquery Sync Complete", severity="ALERT")
        logging.info("Canvas Bigquery Sync Complete")
    except Exception as e:
        #logger.log_text(f"Canvas Data Dump Failed: {e}", severity="ERROR")
        logging.error(f"Canvas Data Dump Failed: {e}")

if __name__ == "__main__":
    app()