# Canvas Bigquery Sync Script

## Description

This script is designed to sync data from Canvas Data Portal to Google BigQuery. The main functionality includes:

-   Fetching the latest data dump and schema from Canvas Data Portal.
-   Preprocessing the data (cleaning special characters and converting it into a CSV format).
-   Uploading the CSVs to Google Cloud Storage.
-   Finally, loading the data from Cloud Storage to BigQuery.

## Prerequisites

1. You need to have a valid `API_KEY` and `API_SECRET` from Canvas Data Portal.
2. Google Cloud credentials should be set up to access both Google Cloud Storage and Google BigQuery.
3. Necessary Python libraries: `google.cloud.bigquery`, `google.cloud.storage`, `logging`, `os`, `shutil`, and `pandas`.
4. `canvas_data.api` library (used to fetch data from Canvas).

## Configuration

Set the following environment variables:

-   `BUCKET_NAME`: Name of the Google Cloud Storage bucket where CSVs are uploaded.
-   `API_KEY`: Your Canvas Data Portal API Key.
-   `API_SECRET`: Your Canvas Data Portal API Secret.
-   `PROJECT_NAME`: Your Google Cloud project name.
-   `TABLE_NAME`: Name of the BigQuery dataset where tables will be loaded.

## How to Run

To run the script, simply use the command:

```bash
python <script_name>.py
```

Replace `<script_name>` with the name of this script.

## Building and Deploying Docker Container to Google Cloud Artifact

### Prerequisites

1. You should have the Google Cloud SDK (`gcloud` CLI tool) installed and configured on your local machine.
2. Ensure you have `Docker` installed on your local machine.
3. You should have a Google Cloud project and access to Google Cloud Artifact Registry.

### Instructions

1. **Set your project**:
   Replace `YOUR_PROJECT_ID` with your Google Cloud Project ID.

    ```bash
    gcloud config set project YOUR_PROJECT_ID
    ```

2. **Enable the Artifact Registry API**:

    ```bash
    gcloud services enable artifactregistry.googleapis.com
    ```

3. **Create a Docker repository in Artifact Registry**:
   Replace `YOUR_REPOSITORY_NAME` with the name you'd like to give your repository and `YOUR_REGION` with your desired Google Cloud region (e.g., `us-central1`).

    ```bash
    gcloud artifacts repositories create YOUR_REPOSITORY_NAME --repository-format=docker --location=YOUR_REGION
    ```

4. **Configure Docker for authentication with Artifact Registry**:

    ```bash
    gcloud auth configure-docker YOUR_REGION-docker.pkg.dev
    ```

5. **Build your Docker image**:
   Navigate to the directory containing your Dockerfile. Replace `YOUR_IMAGE_NAME` with the name you'd like to give your Docker image and `YOUR_TAG` with your image tag (e.g., `v1`).

    ```bash
    docker build -t YOUR_REGION-docker.pkg.dev/YOUR_PROJECT_ID/YOUR_REPOSITORY_NAME/YOUR_IMAGE_NAME:YOUR_TAG .
    ```

6. **Push your Docker image to Artifact Registry**:

    ```bash
    docker push YOUR_REGION-docker.pkg.dev/YOUR_PROJECT_ID/YOUR_REPOSITORY_NAME/YOUR_IMAGE_NAME:YOUR_TAG
    ```

7. **Verify your Docker image in Artifact Registry**:
   You can now navigate to the Artifact Registry section in the Google Cloud Console to see your uploaded Docker image.

### Deploying from Artifact Registry

Once your Docker image is stored in Artifact Registry, you can use it in any Google Cloud services that support container deployment (like GKE or Cloud Run). Simply reference the image by its location in Artifact Registry when deploying.

Replace placeholders (YOUR_PROJECT_ID, YOUR_REPOSITORY_NAME, etc.) with appropriate values before executing the commands.

## Important Notes

-   The script has a type conversion map (`type_conversion_map`) to map the data types from Canvas to BigQuery.
-   The script skips the `requests` table and tables containing the word 'catalog' as they are handled separately.
-   If there's any exception during the process, the script will log the error but will continue processing the next tables.
-   The BigQuery jobs are set to overwrite the tables by default except for the `requests` table, which appends the new data.

## Troubleshooting

-   Ensure that the environment variables are set correctly before running the script.
-   Ensure that the Google Cloud credentials are valid and have the necessary permissions.
-   For detailed logs and potential issues, check the console output as the script uses the logging module to report progress and errors.

## Future Enhancements

-   Consider adding more robust error handling and retries.
-   Allow dynamic table filtering to include/exclude specific tables.
-   Integrate monitoring and alerting for failures.

## Feedback and Contributions

Feel free to provide feedback or contribute to this project by [link to repository or contact method].

## License

GPL-3.0 license [See LICENSE for more details.]
