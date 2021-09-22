import requests


class Service():
    token = None
    email = None
    password = None
    host = None

    def __init__(self, email, password, environment=None):
        self.email = email
        self.password = password

        if environment == 'dev':
            self.host = 'https://engine-dev.cinch.io'
        elif environment == 'local':
            self.host = 'http://app:8000'
        else:
            self.host = 'https://engine.cinch.io'

    def _patch(self, url, records):
        token = self._get_token()
        return requests.patch(f'{self.host}/{url}/bulk?match=entity',
                       json=records,
                       headers={"Authorization": f"Token {token}"})

    def _get_token(self):
        if not self.token:
            response = requests.post(f'{self.host}/users/login', json={"email": self.email, "password": self.password})
            response = response.json()
            self.token = response['token']

        return self.token

    def post_locations(self, records):
        return self._patch('locations', records)

    def post_customer_refs(self, records):
        return self._patch('customer-refs', records)

    def post_schedules(self, records):
        return self._patch('customer-refs/schedules', records)

    def post_transactions(self, records):
        return self._patch('transactions', records)

    def post_transaction_details(self, records):
        return self._patch('transactions/details', records)

    def post_engagements(self, records):
        return self._patch('unsubscribes', records)
