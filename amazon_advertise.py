import sys
import time
import pickle
import logging
import requests
from pathlib import Path
from settings import config

BASE_DIR = Path(__file__).parent
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)


class ResponseHandler:
    """
    Class implementing a response processing decorator
    """
    @classmethod
    def handler(cls, func):
        def wrapper(*args, **kwargs):
            try:
                logging.info('Make request.')
                response = func(*args, **kwargs)
                response.raise_for_status()
            except requests.exceptions.HTTPError as http_error:
                logging.warning(f'{http_error} - Issue with request')
            except Exception:
                logging.warning(f'{sys.exc_info()} - issue in program')
            else:
                return response
        return wrapper


class AmazonAdvertise:
    """
    Base class
    """
    config = config['amazon_advertising']
    headers = {
        'Content-Type': 'application/json',
        'Amazon-Advertising-API-ClientId': config['client_id'],
        'Amazon-Advertising-API-Scope': config['profile_id'],
    }

    main_url = 'https://advertising-api.amazon.com{}'

    @classmethod
    def local_token(cls) -> str:
        """
        Method to load token from local pickle file

        # if file is missed or token is expired - call 2 methods
        # to request token via API and store in the local file

        :return: API token from local file
        """
        if not Path(BASE_DIR / 'token.pickle').is_file():
            logging.info('Local token missed.')
            cls.save_token(cls.request_token())

        with open(BASE_DIR / 'token.pickle', 'rb') as f:
            token_data = pickle.load(f)

        logging.info('Check token lifetime...')
        if time.time() - token_data['timestamp'] > token_data['expires_in'] - 10:
            logging.info('Token has been expired.')
            token_data = cls.save_token(cls.request_token())

        return token_data['access_token']

    @classmethod
    @ResponseHandler.handler
    def request_token(cls):
        """
        API method to get token from Amazon Advertise API using refresh token

        :return: response object that should be processed via save_token static method
        """
        url = 'https://api.amazon.com/auth/o2/token'
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'charset': 'UTF-8'
        }

        body = {
            'grant_type': 'refresh_token',
            'client_id': cls.config['client_id'],
            'refresh_token': cls.config['refresh_token'],
            'client_secret': cls.config['client_secret']
        }

        logging.info('Request for the new token.')
        return requests.post(url, headers=headers, data=body)

    @staticmethod
    def save_token(response) -> bool:
        """
        Method to save token to the local .pickle file

        :param response: response with token data that received via request_token method
        :return: token_data, dict
        """
        token_data = response.json()
        token_data.update({'timestamp': time.time()})

        with open(BASE_DIR / 'token.pickle', 'wb') as f:
            pickle.dump(file=f, obj=token_data)

        logging.info('Token has been saved to the file.')
        return token_data

    def sign_request(func):
        """
        Decorator to sign request with token and needed headers
        """
        async def sign(self, *args, **kwargs):
            logging.info('Try to sign request with auth token.')
            logging.info('Load token from local file.')
            headers = {
                'Authorization': f'Bearer {self.local_token()}',
                **self.headers
            }
            logging.info('Token added to the request.')
            return await func(self, *args, headers=headers, **kwargs)
        return sign

    @sign_request
    async def make_request(self, *, session=None, api_url='', **kwargs):
        """
        Async method to make request to Amazon Advertising API endpoint

        :param session: aiohttp.ClientSession()
        :param api_url: Amazon Advertising API endpoint url
        :param kwargs: any additional keywords params
        :return: aiohttp.ClientResponse
        """
        return await session.request(url=self.main_url.format(api_url), **kwargs)


class Reports(AmazonAdvertise):
    """
    Class to work with Reports endpoint
    """

    async def request(self, *, session=None, api_url='', body=None):
        """
        Request new report creation

        :param session: aiohttp.ClientSession()
        :param api_url: endpoint url
        :param body: body params. Must contains metrics and reportDate
        :return: aiohttp.ClientResponse
        """
        return await self.make_request(session=session, method='POST', api_url=api_url, data=body)

    async def status(self, *, session=None, report_id=''):
        """
        Check status of requested report

        :param session: aiohttp.ClientSession()
        :param report_id: report id, has been return in response by Reports.request() method
        :return: aiohttp.ClientResponse
        """
        api_url = f'/v2/reports/{report_id}'
        return await self.make_request(session=session, method='GET', api_url=api_url)

    async def download(self, *, session=None, report_id=''):
        """
        Download report by report id. Should be used only for report with "SUCCESS" status

        :param session: aiohttp.ClientSession()
        :param report_id: report id, has been return in response by Reports.request() method
        :return: aiohttp.ClientResponse
        """
        api_url = f'/v2/reports/{report_id}/download'
        return await self.make_request(session=session, method='GET', api_url=api_url)
