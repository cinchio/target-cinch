import requests
from singer import logger
import json


class Service():
    token = None
    email = None
    password = None
    host = None

    def __init__(self, email, password, environment=None):
        self.email = email
        self.password = password

        if environment == 'dev':
            self.host = 'https://engine.dev.aws.cinch.io'
        elif environment == 'local':
            self.host = 'http://localhost:8000'
        else:
            self.host = 'https://engine.cinch.io'

    def patch(self, url, data):
        logger.log_info(f'SENDING PATCH {url}')

        token = self._get_token()
        return requests.patch(
            f'{self.host}/{url}',
            json=data,
            headers={"Authorization": f"Token {token}"},
        )

    def post(self, url, data):
        logger.log_info(f'SENDING POST {url}')

        token = self._get_token()
        return requests.post(
            f'{self.host}/{url}',
            json=data,
            headers={"Authorization": f"Token {token}"},
        )


    def _bulk(self, url, records, retry=True):
        logger.log_info(f'SENDING BATCH {url}: {len(records)}')

        token = self._get_token()
        response = requests.patch(f'{self.host}/{url}/bulk?match=entity',
                       json=records,
                       headers={"Authorization": f"Token {token}"})

        # if we got logged out, try and log us back in and try again
        if response.status_code == 401 and retry:
            # We only want to do this once.
            self.token = None
            return self._bulk(url, records, retry=False)

        if response.status_code >= 400:
            # Something bad happened.
            logger.log_error(f'Request PATCH {self.host}/{url}/bulk?match=entity')
            logger.log_error('DATA:')
            logger.log_error(json.dumps(records, indent=2))
            logger.log_error(f'Response Code: {response.status_code}')
            logger.log_error(f'Response Text: {response.text}')

            raise Exception(f"Unable to communicate with server")

        logger.log_info('finished batch')

        return response

    def _get_token(self):
        if not self.token:
            response = requests.post(f'{self.host}/users/login', json={"email": self.email, "password": self.password})
            response = response.json()
            self.token = response['token']

        return self.token

    def post_locations(self, records):
        return self._bulk('locations', records)

    def post_customer_refs(self, records):
        return self._bulk('customer-refs', records)

    def post_schedules(self, records):
        return self._bulk('customer-refs/schedules', records)

    def post_transactions(self, records):
        return self._bulk('transactions', records)

    def post_transaction_details(self, records):
        return self._bulk('transactions/details', records)

    def post_transaction_coupons(self, records):
        return self._bulk('transactions/coupons', records)

    def post_carts(self, records):
        return self._bulk('carts', records)

    def post_cart_details(self, records):
        return self._bulk('carts/details', records)

    def post_cart_coupons(self, records):
        return self._bulk('carts/coupons', records)

    def post_engagements(self, records):
        return self._bulk('unsubscribes', records)

    def post_vehicles(self, records):
        return self._bulk('customer-refs/vehicles', records)

    def post_subscriptions(self, records):
        return self._bulk('customer-refs/subscriptions', records)
