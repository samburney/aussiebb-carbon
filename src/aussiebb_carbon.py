import requests
import datetime
import pandas as pd
import netaddr


class Carbon:
    def __init__(
            self,
            api_prod_url=None,
            api_username=None,
            api_password=None,
            api_test_url=None,
            api_mode='prod'):

        # Config
        self.config = {}

        # Base URL
        if api_prod_url or api_test_url:
            if vars()[f'api_{api_mode}_url'] is None:
                raise ValueError(f"API mode is '{api_mode}' but api_{api_mode}_url has not been provided.")
            base_url = vars()[f'api_{api_mode}_url']
            self.config['base_url'] = base_url
        else:
            raise ValueError('api_prod_url or api_test_url must be provided.')

        # Authentication
        if api_username is None or api_password is None:
            raise ValueError('api_username and api_password must be provided.')
        self.config['username'] = api_username
        self.config['password'] = api_password

        # Session data
        self.session = None
        self.login_response = None
        self.login_expiry = 0
        self.access_token = None

        # API cached data
        self.customer = None
        self.services = None

        # Set up persistent session
        self.session = requests.Session()

    def make_endpoint_url(self, endpoint):
        """
        Build a URL to an API endpoint

        endpoint: API endpoint as a string

        Returns URL as a string
        """
        endpoint_url = f'{self.config['base_url']}/{endpoint}'

        return endpoint_url

    def make_get_request(self, endpoint, headers=None, **params):
        """
        Make a request to the provided endpoint

        endpoint: API endpoint as a string
        headers: API endpoint as a string
        params: Any additional parameters accepted by requests.session.get()

        Returns requests.Response object
        """
        endpoint_url = self.make_endpoint_url(endpoint)

        # Set default headers
        if headers is None:
            headers = {"Accept": "application/json"}

        return self.session.get(url=endpoint_url, headers=headers, **params)

    def do_login(self):
        """
        Log into Carbon API

        cookies: (Optional) CookieJar from previously successful login

        Returns requests.response object
        """
        timestamp_now = datetime.datetime.timestamp(datetime.datetime.now())

        if self.login_response is None and timestamp_now > self.login_expiry:
            url = self.make_endpoint_url('login')
            request_data = {
                'username': self.config['username'],
                'password': self.config['password'],
            }
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }

            response = self.session.post(url=url, json=request_data, headers=headers)

            if response.status_code == 200:
                self.login_response = response
                self.login_expiry = timestamp_now + self.login_response.json()['expiresIn']
            else:
                raise ConnectionError(f'A login error occured. (HTTP Response Code: {response.status_code}; Reason: {response.reason})')

        return self.login_response

    def do_logout(self):
        """
        Log out of Carbon API

        cookies: CookieJar from previously successful login

        Returns requests.response object
        """
        url = self.make_endpoint_url('login')
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        response = self.session.delete(url=url, headers=headers)

        self.login_response = None
        self.login_expiry = 0
        self.access_token = None

        return response

    def get_access_token(self):
        """
        Get a valid Carbon API access token

        Returns access token as a string
        """
        self.do_login()
        return self.login_response.json()['accessToken']

    def get_customer(self, use_cache=True):
        """
        Get wholesale customer data from API.

        Returns customer as a dict
        """
        if self.customer is None or use_cache is False:
            self.do_login()

            url = self.make_endpoint_url('customer')
            params = {"v": "2"}
            headers = {
                "Accept": "application/json"
            }

            response = self.session.get(url=url, headers=headers, params=params)

            if response.status_code == 200:
                self.customer = response.json()
            else:
                raise ConnectionError(f'An error occured making an API request. (HTTP Response Code: {response.status_code}; Reason: {response.reason})')

        return self.customer

    def get_all_services(self, use_cache=True):
        """
        Get all customer services from API.

        use_cache: Use pre-fetched data if available (Default: True)

        Returns list of all services as a DataFrame
        """
        if self.services is None or use_cache is False:
            self.do_login()

            url = self.make_endpoint_url('carbon/services')
            headers = {
                "Accept": "application/json"
            }

            response = self.session.get(url=url, headers=headers)

            if response.status_code == 200:
                # Expand data in some columns
                services = pd.DataFrame(response.json()['data'])

                # network column
                services_network = services['network'].apply(pd.Series).add_prefix('network_')
                services = services.assign(**services_network).drop(['network'], axis=1)

                # network_headend column
                services_headend = services['network_headend'].apply(pd.Series).add_prefix('headend_')
                services = services.assign(**services_headend).drop(['network_headend'], axis=1)

                self.services = services
            else:
                raise ConnectionError(f'An error occured making an API request. (HTTP Response Code: {response.status_code}; Reason: {response.reason})')

        return self.services

    def get_service(self, service_id, use_cache=True):
        """
        Get customer service detail

        service_id: Carbon API service id
        use_cache: Use pre-fetched data if available (Default: True)

        Returns service detail as a dict
        """
        services = self.get_all_services()
        return services.loc[(services.id == service_id)].to_dict(orient='records')[0]

    def get_service_by_avc(self, avc_id, use_cache=True):
        """
        Get customer service detail based on NBN AVC ID

        avc_id: NBN AVC ID
        use_cache: Use pre-fetched data if available (Default: True)

        Returns service detail as a dict
        """
        services = self.get_all_services()
        return services.loc[(services.service_identifier.str.upper() == avc_id.upper())].to_dict(orient='records')[0]

    def get_service_by_loc_id(self, loc_id, use_cache=True):
        """
        Get customer service detail based on NBN loc_id ID

        loc_id: NBN Location ID
        use_cache: Use pre-fetched data if available (Default: True)

        Returns service detail as a dict
        """
        services = self.get_all_services()
        return services.loc[(services.location_id.str.upper() == loc_id.upper())].to_dict(orient='records')[0]

    def get_service_ip_addresses(self, service_id, use_cache=True):
        """
        Get IP addresses assigned to a service

        service_id: Carbon API service id
        use_cache: Use pre-fetched data if available (Default: True)

        Returns assigned IP addresses as a list of dicts
        """
        service_ip_addresses = []
        services = self.get_all_services()

        network_ips = services.loc[(services.id == service_id)].reset_index()['network_ips'][0]

        for network_ip in network_ips:
            service_ip_address = network_ip

            if 'ip' in service_ip_address:
                service_ip_address['ip'] = netaddr.IPNetwork(service_ip_address['ip'])

            service_ip_addresses.append(service_ip_address)

        return service_ip_addresses
