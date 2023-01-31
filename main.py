import google.cloud.logging
import google.cloud.storage
import os
from canvas_data.api import CanvasDataAPI

if __name__ == "__main__":
    API_KEY = "e5c6e94761aa92955248d6979d068ca63860bc79"
    API_SECRET = "0d7bb1b2e494278b6bcfb38f31a7e3da8eff80a1"
    cd = CanvasDataAPI(api_key=API_KEY, api_secret=API_SECRET)


    # logging_client = google.cloud.logging.Client()
    # logging_client.setup_logging()

    # logger = logging_client.logger("canvas_logger")

    # logger.log_text("Hello, world!", severity="INFO")