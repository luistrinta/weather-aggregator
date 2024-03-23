import json
from string import Formatter
import requests

class Extractor:
    "Represents the API connection and data extraction manager for the pipeline"

    def __init__(self,api_name="",api_url="",api_key="") -> None:
        """ Define the URL and key for the API. Other functions inside the class will deal with API calls."""    
        self.api_name= api_name
        self.api_url= api_url
        self.api_key = api_key

    def set_values(self,api_name=None,api_url=None,api_key=None):
        if api_name is not None:
            self.api_name= api_name
        if api_url is not None:
            self.api_url= api_url
        if api_key is not None:
            self.api_key = api_key

    def extract_data(self,query_string,**kwargs):
            """Call the given API and extract data from any source 
            query_string: The part in the url for our API calls after the ? character. This is the part of the URL responsible for filtering the request
            kwargs: A dictionary of all the fields used in the query_string variable.
            This function allows the Extractor class to make personalised calls to the api based on the necessities of the user
            """
            try:       
                request_url =Formatter().format(self.api_url+"?"+query_string,**kwargs)
                payload = requests.get(request_url)
                return json.loads(payload.text)
                
            except Exception as error:
                print(f"Error when extracting data from API {self.api_name}: {error}")
    