import os
import json
import logging
import subprocess
from dotenv import load_dotenv

load_dotenv()
api = os.getenv("API_KEY")

logging.basicConfig(filename='log_Api_Call.log',
                    format='%(asctime)s %(message)s',
                    filemode='a')


class DataFetch:
    def __init__(self, station, station_id, date, table, localtime=True):
        self.station = station
        self.station_id = station_id
        self.date = date
        self.table = table
        self.localtime = localtime
        self.token = api

    def __str__(self):
        return f"https://{self.station}.advm2.net/api/v1/station_editor/{self.station_id}/data?date={self.date}&table={self.table}&localtime={self.localtime}"

    def call_api(self):
        url = self.__str__()
        headers = ["-H", f"X-API-Token: {self.token}"]

        try:
            # result = subprocess.run(
            #     ["curl", "-s"] + headers + [url],
            #     capture_output=True,
            #     text=True,
            #     timeout=10
            # )
            result = requests.get(url)

            if result.returncode != 0:
                logging.error(f"cURL command failed: {result.stderr}")
                return None

            try:
                data = json.loads(result.stdout)
                logging.info("API call successful.")
                with open(f"./{self.station}.json", "w") as outfile:
                    json.dump(data, outfile)
                return data

            except json.JSONDecodeError:
                logging.error("Failed to parse JSON response.")
                return None

        except subprocess.TimeoutExpired:
            logging.error("API call timed out.")
            return None

        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return None
