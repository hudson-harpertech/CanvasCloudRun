import google.cloud.bigquery
import google.cloud.storage
import logging
import os
import shutil
import pandas as pd

from canvas_data.api import CanvasDataAPI

# map for handling data type conversion from Canvas Data to Bigquery
type_conversion_map = {   
    'bigint': 'FLOAT64',
    'boolean': 'BOOLEAN',
    'date': 'STRING',
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
        # initialize logging client for Cloud Run Job logs
        logging.basicConfig(level=logging.INFO)

        # initialize Cloud Storage and Bigquery clients
        storage_client = google.cloud.storage.Client()
        bucket = storage_client.get_bucket(os.environ['BUCKET_NAME'])
        bigquery_client = google.cloud.bigquery.Client()

        logging.info("Beginning Canvas Bigquery Sync")

        # fetch latest data dump and schema from Canvas Data Portal
        cd = CanvasDataAPI(api_key=os.environ['API_KEY'], 
                           api_secret=os.environ['API_SECRET'])
        schema = cd.get_schema('latest', key_on_tablenames=True)
        
        try:
            # iterate through tables in schema
            for table_name in sorted(schema):
                # skip requests table as it is handled separately
                if table_name != "requests" and 'catalog' not in table_name:
                    try:
                        local_data_filename = cd.get_data_for_table(table_name=table_name)

                        # clean special characters to avoid errors with parsing CSV files later
                        with open(f"data/{table_name}.txt", "r") as f:
                            data = f.read()
                            data = data.replace("\\N", "")
                            data = data.replace("\"", "")
                        
                        with open(f"data/{table_name}.txt", "w") as f:
                            f.write(data)

                        # read data into pandas dataframe and write to CSV
                        df = pd.read_csv(f"data/{table_name}.txt", sep="\t", low_memory=False)
                        table_columns = schema[table_name]['columns']
                        df.columns = [column["name"] for column in table_columns]
                        df.to_csv(f"csvs/{table_name}.csv", index=False)

                        # upload CSV to Cloud Storage
                        bucket.blob(f"{table_name}.csv").upload_from_filename(f"csvs/{table_name}.csv")
                        logging.info(f"Cloud Storage sync complete for {table_name}")
                    except Exception as e:
                        logging.error(f"File sync failed for {table_name}: {e}")
                    finally:
                        # clean up local files
                        shutil.rmtree("data", ignore_errors=True)
                        shutil.rmtree("downloads", ignore_errors=True)
                        if os.path.exists(f"csvs/{table_name}.csv"):
                            os.remove(f"csvs/{table_name}.csv")

            try:
                # handle requests table separately as it is not included in schema
                local_data_filename = cd.get_data_for_table("requests")

                # clean special characters to avoid errors with parsing CSV files later
                with open(f"data/requests.txt", "r") as f:
                    data = f.read()
                    data = data.replace("\\N", "")
                    data = data.replace("\"", "")

                with open(f"data/requests.txt", "w") as f:
                    f.write(data)

                # read data into pandas dataframe and write to CSV
                df = pd.read_csv(f"data/requests.txt", sep="\t", low_memory=False)
                table_columns = schema['requests']['columns']
                df.columns = [column["name"] for column in table_columns]
                df.to_csv(f"csvs/requests.csv", index=False)

                # upload CSV to Cloud Storage
                bucket.blob(f"requests.csv").upload_from_filename(f"csvs/requests.csv")
            except Exception as e:
                logging.error(f"File sync failed for requests: {e}")

            # loop through tables in schema and load into Bigquery
            for table_name in sorted(schema):
                if table_name != "requests":
                    try:
                        table_columns = schema[table_name]['columns']
                        table_id = f"{os.environ['PROJECT_NAME']}.{os.environ['TABLE_NAME']}.{table_name}"
                        
                        # configure Bigquery job to rewrite tables on upload and automatically detect schema from CSV
                        job_config = google.cloud.bigquery.LoadJobConfig(source_format=google.cloud.bigquery.SourceFormat.CSV, autodetect=True)
                        job_config.write_disposition = google.cloud.bigquery.WriteDisposition.WRITE_TRUNCATE
                        job_config.schema = [
                            google.cloud.bigquery.SchemaField(column["name"], 
                                                              type_conversion_map.get(column["type"],"STRING"),
                                                              description=column["description"],
                                                              ) for column in table_columns
                        ]

                        uri = f"gs://{os.environ['BUCKET_NAME']}/{table_name}.csv"
                        
                        # load CSV from Cloud Storage into Bigquery
                        load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)
                        load_job.result()  # Waits for the job to complete.
                        
                        # report and log success
                        destination_table = bigquery_client.get_table(table_id)  # Make an API request.
                        logging.info("Loaded {} rows: {}.".format(destination_table.num_rows, table_name))
                    except Exception as e:
                        logging.error(f"Bigquery sync failed for {table_name}: {e}")
            try:
                # handle requests table separately as it is not included in schema
                table_columns = schema['requests']['columns']
                table_id = f"{os.environ['PROJECT_NAME']}.{os.environ['TABLE_NAME']}.requests"
                
                # configure Bigquery job to append request table on upload and automatically detect schema from CSV 
                job_config = google.cloud.bigquery.LoadJobConfig(source_format=google.cloud.bigquery.SourceFormat.CSV, autodetect=True)
                job_config.write_disposition = google.cloud.bigquery.WriteDisposition.WRITE_APPEND
                job_config.schema = [
                    google.cloud.bigquery.SchemaField(column["name"], 
                                                        type_conversion_map.get(column["type"],"STRING"),
                                                        description=column["description"],
                                                        ) for column in table_columns
                ]

                uri = f"gs://{os.environ['BUCKET_NAME']}/requests.csv"
                
                # load CSV from Cloud Storage into Bigquery
                load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)
                load_job.result()  # Waits for the job to complete.

                # log success
                destination_table = bigquery_client.get_table(table_id)  # Make an API request.
                logging.info("Loaded {} rows: {}.".format(destination_table.num_rows, "requests"))
            except Exception as e:
                logging.error(f"Bigquery sync failed for requests: {e}")
        except Exception as e:
            logging.error(f"Canvas Data Dump Failed: {e}")

        logging.info("Canvas Bigquery Sync Complete")
    except Exception as e:
        logging.error(f"Canvas Data Dump Failed: {e}")

if __name__ == "__main__":
    app()
