import requests
from time import time
import pandas as pd
import netaddr
import pickle

from pathlib import Path


class Carbon:
    def __init__(
            self,
            api_prod_url=None,
            api_username=None,
            api_password=None,
            api_test_url=None,
            api_mode='prod',
            cache_type='file',
            cache_location=None,
            debug=False):

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

        # Cache to limit slow lookups
        if cache_type == 'file':
            self.config['cache_type'] = cache_type
        else:
            raise NotImplementedError("Only the 'file' cache type is implemented")

        if cache_location is not None:
            self.config['cache_location'] = Path(cache_location).resolve()
        else:
            script_root = Path(__file__).resolve().parents[1]
            cache_path = Path(script_root, 'carbon_cache')

            # Check cache directory exists
            if cache_path.exists() is False:
                cache_path.mkdir()
            elif cache_path.exists() is True and cache_path.is_dir() is False:
                raise RuntimeError(f'{cache_path} already exists and is not a directory.')

            self.config['cache_location'] = cache_path

        self.debug = debug

        # API session data
        self.login_response = self.cache_get('login_response', None)
        self.login_expiry = self.cache_get('login_expiry', 0)

        # Set up persistent session
        self.session = self.get_session()

    def get_session(self, use_cache=True):
        """
        Start a cached requests.Session

        use_cache: Use cached session as Boolean

        Returns requests.Session() object
        """
        session_data = self.cache_get('session_data')

        if use_cache is True and session_data is not None:
            return session_data

        session_data = requests.Session()
        self.cache_store('session_data', session_data)

        return session_data

    def cache_store(self, key, value):
        """
        Store value in cache

        key: Key of stored value
        value: Value to be stored

        Returns value
        """
        cache_filename = f'carbon_{key}.cache'
        cache_filepath = Path(self.config['cache_location'], cache_filename)

        # If cache file already exists, get existing cached data
        if cache_filepath.is_file() and cache_filepath.stat().st_size > 0:
            with cache_filepath.open('rb') as cache_file:
                cache_data = pickle.load(cache_file)
        else:
            cache_data = {}

        # Update cache_data
        with cache_filepath.open('wb') as cache_file:
            cache_data[key] = {
                'time': time(),
                'value': value,
            }

            # Update cache file
            pickle.dump(cache_data, cache_file)

            if self.debug is True:
                print(f'Cache stored: {key}')

        return value

    def cache_get(self, key, default_value=None, max_age=None):
        """
        Get cached value

        key: Key of stored value
        default_value: Value to return if stored value cannot be found, or cache has expired (Default: None)
        max_age: Maximum age in seconds

        Returns stored value or default value
        """
        value = None
        cache_filename = f'carbon_{key}.cache'
        cache_filepath = Path(self.config['cache_location'], cache_filename)
        cache_data = {}

        # If cache file already exists, get existing cached data
        if cache_filepath.is_file() and cache_filepath.stat().st_size > 0:
            with cache_filepath.open('rb') as cache_file:
                cache_data = pickle.load(cache_file)

                if key != 'login_expiry':
                    login_expiry = self.cache_get('login_expiry', 0)

                if key in cache_data and (key == 'login_expiry' or time() < login_expiry):
                    # Check age of stored data does not exceed max_age
                    if max_age is None or cache_data[key]['time'] + max_age > time():
                        if self.debug is True:
                            print(f'Cache hit: {key}')
                        value = cache_data[key]['value']
                    else:
                        if self.debug is True:
                            print(f'Cache expired: {key}')
                        value = default_value

        if value is None:
            if self.debug is True:
                print(f'Cache miss: {key}')
            value = default_value

        return value

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
        timestamp_now = time()

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
                self.login_response = self.cache_store('login_response', response)
                self.login_expiry = self.cache_store('login_expiry', timestamp_now + self.login_response.json()['expiresIn'])
                self.cache_store('session_data', self.session)
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
        customer = self.cache_get('customer', None)

        if customer is None or use_cache is False:
            self.do_login()

            url = self.make_endpoint_url('customer')
            params = {"v": "2"}
            headers = {
                "Accept": "application/json"
            }

            response = self.session.get(url=url, headers=headers, params=params)

            if response.status_code == 200:
                customer = self.cache_store('customer', response.json())
            else:
                raise ConnectionError(f'An error occured making an API request. (HTTP Response Code: {response.status_code}; Reason: {response.reason})')

        return customer

    def _process_services(self, services):
        # Expand data in some columns
        services = pd.DataFrame(services)

        # network column
        services_network = services['network'].apply(pd.Series).add_prefix('network_')
        services = services.assign(**services_network).drop(['network'], axis=1)

        # network_headend column
        if 'network_headend' in services.columns:
            services_headend = services['network_headend'].apply(pd.Series).add_prefix('headend_')
            services = services.assign(**services_headend).drop(['network_headend'], axis=1)

        return services

    def get_all_services(self, use_cache=True, cache_max_age=300):
        """
        Get all customer services from API.

        use_cache: Use pre-fetched data if available (Default: True)
        cache_max_age: Maximum age of cached data in seconds (Default: 300)

        Returns list of all services as a DataFrame
        """
        services = self.cache_get('services', None, max_age=cache_max_age)

        if services is None or use_cache is False:
            self.do_login()

            url = self.make_endpoint_url('carbon/services')
            headers = {
                "Accept": "application/json"
            }

            response = self.session.get(url=url, headers=headers)

            if response.status_code == 200:
                services = self._process_services(response.json()['data'])

                self.cache_store('services', services)
            else:
                raise ConnectionError(f'An error occured making an API request. (HTTP Response Code: {response.status_code}; Reason: {response.reason})')

        return services

    def get_services_by_tag(self, tag, use_cache=True, cache_max_age=300):
        """
        Get all customer services from API matching provided list of tags.

        tag: Single tag to match
        use_cache: Use pre-fetched data if available (Default: True)
        cache_max_age: Maximum age of cached data in seconds (Default: 300)

        Returns list of all services as a DataFrame
        """
        cache_name = f'services_tag_{tag}'
        services = self.cache_get(cache_name, None, max_age=cache_max_age)

        if services is None or use_cache is False:
            self.do_login()

            url = self.make_endpoint_url('carbon/services')
            headers = {
                "Accept": "application/json"
            }
            params = {
                'filter[tags]': tag
            }

            response = self.session.get(url=url, headers=headers, params=params)

            if response.status_code == 200:
                services_data = response.json()['data']
                if len(services_data) > 0:
                    services = self._process_services(services_data)
                    self.cache_store(cache_name, services)
            else:
                raise ConnectionError(f'An error occured making an API request. (HTTP Response Code: {response.status_code}; Reason: {response.reason})')

        return services

    def get_services_by_tags(self, tags, use_cache=True, cache_max_age=300):
        """
        Get all customer services from API matching provided list of tags.

        tag: List of tags to match
        use_cache: Use pre-fetched data if available (Default: True)
        cache_max_age: Maximum age of cached data in seconds (Default: 300)

        Returns list of all services as a DataFrame
        """
        cache_name = f'services_tags_{'_'.join(str(tag) for tag in tags)}'
        services = self.cache_get(cache_name, None, max_age=cache_max_age)

        if services is None or use_cache is False:
            tags_services = pd.DataFrame()
            for tag in tags:
                tag_services = self.get_services_by_tag(tag=tag, use_cache=use_cache, cache_max_age=cache_max_age)

                if tag_services is not None:
                    tags_services = pd.concat([tags_services, tag_services], ignore_index=True)

            if len(tags_services):
                services = tags_services
                self.cache_store(cache_name, services)

        return services

    def get_service(self, service_id, use_cache=True, cache_max_age=300):
        """
        Get customer service detail

        service_id: Carbon API service id
        use_cache: Use pre-fetched data if available (Default: True)
        cache_max_age: Maximum age of cached data in seconds (Default: 300)

        Returns service detail as a dict
        """
        cache_name = f'service_{service_id}'
        service = self.cache_get(cache_name, None, max_age=cache_max_age)

        if service is None or use_cache is False:
            request = self.make_get_request(f'carbon/services/{service_id}')

            if request.status_code == 200:
                service = request.json()
                self.cache_store(cache_name, service)

            else:
                raise LookupError(f'Service ID {service_id} could not be found.')

        return service

    def get_service_by_avc(self, avc_id, use_cache=True):
        """
        Get customer service detail based on NBN AVC ID

        avc_id: NBN AVC ID
        use_cache: Use pre-fetched data if available (Default: True)

        Returns service detail as a dict
        """
        services = self.get_all_services(use_cache)
        return services.loc[(services.service_identifier.str.upper() == avc_id.upper())].to_dict(orient='records')[0]

    def get_service_by_loc_id(self, loc_id, use_cache=True):
        """
        Get customer service detail based on NBN loc_id ID

        loc_id: NBN Location ID
        use_cache: Use pre-fetched data if available (Default: True)

        Returns service detail as a dict
        """
        services = self.get_all_services(use_cache)
        return services.loc[(services.location_id.str.upper() == loc_id.upper())].to_dict(orient='records')[0]

    def get_service_ip_addresses(self, service_id, use_cache=True):
        """
        Get IP addresses assigned to a service

        service_id: Carbon API service id
        use_cache: Use pre-fetched data if available (Default: True)

        Returns assigned IP addresses as a list of dicts
        """
        service_ip_addresses = []
        services = self.get_all_services(use_cache)

        network_ips = services.loc[(services.id == service_id)].reset_index()['network_ips'][0]

        for network_ip in network_ips:
            service_ip_address = network_ip

            if 'ip' in service_ip_address:
                service_ip_address['ip'] = netaddr.IPNetwork(service_ip_address['ip'])

            service_ip_addresses.append(service_ip_address)

        return service_ip_addresses
