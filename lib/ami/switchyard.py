"""
Switchyard REST API wrapper
"""
import logging
import time
import requests

logger = logging.getLogger()

class Switchyard:
    def __init__(self, url, token, retries=3, retry_time=3):
        self.url = url
        self.token = token
        self.retries = retries
        self.retry_time = retry_time

    def _make_request(self, method, url, data=None):        
        "Make a request and handle the data"
        retries = self.retries
        while True:
            try:
                if method == "GET":
                    r = requests.get(self.url + url, headers={'api-token': self.token})            
                elif method == "POST":
                    r = requests.post(self.url + url, headers={'api-token': self.token}, json=data)
                else:
                    raise ValueError(f"Unhandled method {method}")
                r.raise_for_status()
                return r.json()

            except requests.HTTPError as e:
                if retries < 0 or e.response.status_code == 401:
                    print(e.request.url, e.request.headers)
                    raise IOError(e)
                logger.debug(f"Failed retrieving {method} {url}: {e}")
                retries -= 1
                time.sleep(self.retry_time)

        

    def get_processing_status(self, group):
        "get the processing status of a group"
        res = self._make_request("GET", f"/media_objects/status/{group}")
        if res['error']:
            raise Exception(res['message'])
        return res

    def submit_group(self, group, data):
        "Sumbit a group file for processing"
        res = self._make_request("POST", "/media_objects/create", data)
        if res['error']:
            raise Exception(res['message'])


    