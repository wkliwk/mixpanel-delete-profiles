"""
MIT License

Copyright (c) 2016 Sean Coonce

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

Author: Sean Coonce - https://github.com/cooncesean
Github repo: https://github.com/cooncesean/mixpanel-query-py
"""

import math
import itertools
from multiprocessing.pool import ThreadPool


class ConcurrentPaginator(object):
    """
    Concurrently fetches all pages in a paginated collection.

    Currently, only the people API (`/api/2.0/engage`) supports pagination.
    This class is designed to support the people API's implementation of
    pagination.
    """

    def __init__(self, get_func, concurrency=20):
        """
        Initialize with a function that fetches a page of results.
        `concurrency` controls the number of threads used to fetch pages.

        Example:
            client = MixpanelQueryClient(...)
            ConcurrentPaginator(client.get_engage, concurrency=10)
        """
        self.get_func = get_func
        self.concurrency = concurrency

    def fetch_all(self, params=None):
        """
        Fetch all results from all pages, and return as a list.

        If params need to be sent with each request (in addition to the
        pagination) params, they may be passed in via the `params` kwarg.
        """
        params = params and params.copy() or {}

        first_page = self.get_func(params)
        results = first_page['results']
        params['session_id'] = first_page['session_id']

        start, end = self._remaining_page_range(first_page)
        fetcher = self._results_fetcher(params)
        return results + self._concurrent_flatmap(fetcher, list(range(start, end)))

    def _results_fetcher(self, params):
        def _fetcher_func(page):
            req_params = dict(list(params.iteritems()) + [('page', page)])
            return self.get_func(req_params)['results']
        return _fetcher_func

    def _concurrent_flatmap(self, func, iterable):
        pool = ThreadPool(processes=self.concurrency)
        return list(itertools.chain(*pool.map(func, iterable)))

    def _remaining_page_range(self, response):
        num_pages = math.ceil(response['total'] / float(response['page_size']))
        return response['page'] + 1, int(num_pages)

import base64
import urllib  # for url encoding
import urllib2  # for sending requests
import cStringIO
import logging
import gzip
import shutil
import time
import os
import datetime
from inspect import isfunction
from itertools import chain
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
from ast import literal_eval
from copy import deepcopy
import csv

try:
    import ujson as json
except ImportError:
    import json

try:
    import ciso8601
except ImportError:
    pass


class Mixpanel(object):
    """An object for querying, importing, exporting and modifying Mixpanel data via their various APIs

    """
    FORMATTED_API = 'https://mixpanel.com/api'
    RAW_API = 'https://data.mixpanel.com/api'
    IMPORT_API = 'https://api.mixpanel.com'
    VERSION = '2.0'
    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.WARNING)
    sh = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    sh.setFormatter(formatter)
    LOGGER.addHandler(sh)

    def __init__(self, api_secret, token=None, timeout=120, pool_size=None, max_retries=10, debug=False):
        """Initializes the Mixpanel object

        :param api_secret: API Secret for your project
        :param token: Project Token for your project, required for imports
        :param timeout: Time in seconds to wait for HTTP responses
        :param pool_size: Number of threads to use for multiprocessing, default is cpu_count * 2
        :param max_retries: Maximum number of times to retry when a 503 HTTP response is received
        :param debug: Enable debug logging
        :type api_secret: str
        :type token: str
        :type timeout: int
        :type pool_size: int
        :type max_retries: int
        :type debug: bool

        """

        self.api_secret = api_secret
        self.token = token
        self.timeout = timeout
        if pool_size is None:
            # Default number of threads is system dependent
            pool_size = cpu_count() * 2
        self.pool_size = pool_size
        self.max_retries = max_retries
        log_level = Mixpanel.LOGGER.getEffectiveLevel()
        ''' The logger is a singleton for the Mixpanel class, so multiple instances of the Mixpanel class will use the
        same logger. Subsequent instances can upgrade the logging level to debug but they cannot downgrade it.
        '''
        if debug or log_level == 10:
            Mixpanel.LOGGER.setLevel(logging.DEBUG)
        else:
            Mixpanel.LOGGER.setLevel(logging.WARNING)

    @staticmethod
    def unicode_urlencode(params):
        """URL encodes a dict of Mixpanel parameters

        :param params: A dict containing Mixpanel parameter names and values
        :type params: dict
        :return: A URL encoded string
        :rtype: str

        """
        if isinstance(params, dict):
            params = params.items()
        for i, param in enumerate(params):
            if isinstance(param[1], list):
                params[i] = (param[0], json.dumps(param[1]),)

        result = urllib.urlencode([(k, isinstance(v, unicode) and v.encode('utf-8') or v) for k, v in params])
        return result

    @staticmethod
    def response_handler_callback(response):
        """Takes a Mixpanel API response and checks the status

        :param response: A Mixpanel API JSON response
        :type response: str
        :raises RuntimeError: Raises RuntimeError if status is not equal to 1

        """
        response_data = json.loads(response)
        if ('status' in response_data and response_data['status'] != 1) or ('status' not in response_data):
            Mixpanel.LOGGER.warning("Bad API response: " + response)
            raise RuntimeError('Import or Update Failed')
        Mixpanel.LOGGER.debug("API Response: " + response)

    @staticmethod
    def write_items_to_csv(items, output_file):
        """Writes a list of Mixpanel events or profiles to a csv file

        :param items: A list of Mixpanel events or profiles
        :param output_file: A string containing the path and filename to write to
        :type items: list
        :type output_file: str

        """
        # Determine whether the items are profiles or events based on the presence of a $distinct_id key
        if '$distinct_id' in items[0]:
            props_key = '$properties'
            initial_header_value = '$distinct_id'
        else:
            props_key = 'properties'
            initial_header_value = 'event'

        subkeys = set()
        # returns a list of lists of property names from each item
        columns = [item[props_key].keys() for item in items]
        # flattens to a list of property names
        columns = list(chain.from_iterable(columns))
        subkeys.update(columns)
        subkeys = sorted(subkeys)

        # Create the header
        header = [initial_header_value]
        for key in subkeys:
            header.append(key.encode('utf-8'))

        # Create the writer and write the header
        with open(output_file, 'w') as output:
            writer = csv.writer(output)

            writer.writerow(header)

            for item in items:
                row = []
                try:
                    row.append(item[initial_header_value])
                except KeyError:
                    row.append('')

                for subkey in subkeys:
                    try:
                        row.append((item[props_key][subkey]).encode('utf-8'))
                    except AttributeError:
                        row.append(item[props_key][subkey])
                    except KeyError:
                        row.append("")
                writer.writerow(row)

    @staticmethod
    def properties_from_csv_row(row, header, ignored_columns):
        """Converts a row from a csv file into a properties dict

        :param row: A list containing the csv row data
        :param header: A list containing the column headers (property names)
        :param ignored_columns: A list of columns (properties) to exclude
        :type row: list
        :type header: list
        :type ignored_columns: list

        """
        props = {}
        for h, prop in enumerate(header):
            # Handle a strange edge case where the length of the row is longer than the length of the header.
            # We do this to prevent an out of range error.
            x = h
            if x > len(row) - 1:
                x = len(row) - 1
            if row[x] == '' or prop in ignored_columns:
                continue
            else:
                try:
                    # We use literal_eval() here to de-stringify numbers, lists and objects in the CSV data
                    p = literal_eval(row[x])
                    props[prop] = p
                except (SyntaxError, ValueError) as e:
                    props[prop] = row[x]
        return props

    @staticmethod
    def event_object_from_csv_row(row, header, event_index=None, distinct_id_index=None, time_index=None):
        """Converts a row from a csv file into a Mixpanel event dict

        :param row: A list containing the Mixpanel event data from a csv row
        :param header: A list containing the csv column headers
        :param event_index: Index of the event name in row list, if None this method will determine the index
            (Default value = None)
        :param distinct_id_index: Index of the distinct_id in row list, if None this method will determine the index
            (Default value = None)
        :param time_index: Index of the time property in row list, if None this method will determine the index
            (Default value = None)
        :type row: list
        :type header: list
        :type event_index: int
        :type distinct_id_index: int
        :type time_index: int
        :return: A Mixpanel event object
        :rtype: dict

        """
        event_index = (header.index("event") if event_index is None else event_index)
        distinct_id_index = (header.index("distinct_id") if distinct_id_index is None else distinct_id_index)
        time_index = (header.index("time") if time_index is None else time_index)
        props = {'distinct_id': row[distinct_id_index], 'time': int(row[time_index])}
        props.update(Mixpanel.properties_from_csv_row(row, header, ['event', 'distinct_id', 'time']))
        event = {'event': row[event_index], 'properties': props}
        return event

    @staticmethod
    def people_object_from_csv_row(row, header, distinct_id_index=None):
        """Converts a row from a csv file into a Mixpanel People profile dict

        :param row: A list containing the Mixpanel event data from a csv row
        :param header: A list containing the csv column headers
        :param distinct_id_index: Index of the distinct_id in row list, if None this method will determine the index
            (Default value = None)
        :type row: list
        :type header: list
        :type distinct_id_index: int
        :return: A Mixpanel People profile object
        :rtype: dict

        """
        distinct_id_index = (header.index("$distinct_id") if distinct_id_index is None else distinct_id_index)
        props = Mixpanel.properties_from_csv_row(row, header, ['$distinct_id'])
        profile = {'$distinct_id': row[distinct_id_index], '$properties': props}
        return profile

    @staticmethod
    def list_from_argument(arg):
        """Returns a list given a string with the path to file of Mixpanel data or an existing list

        :param arg: A string file path or a list
        :type arg: list | str
        :return: A list of Mixpanel events or profiles
        :rtype: list

        """
        item_list = []
        if isinstance(arg, basestring):
            item_list = Mixpanel.list_from_items_filename(arg)
        elif isinstance(arg, list):
            item_list = arg
        else:
            Mixpanel.LOGGER.warning("data parameter must be a filename or a list of items")

        return item_list

    @staticmethod
    def list_from_items_filename(filename):
        """Returns a list of Mixpanel events or profiles given the path to a file containing such data

        :param filename: Path to a file containing a JSON array of Mixpanel event or People profile data
        :type filename: str
        :return: A list of Mixpanel events or profiles
        :rtype: list

        """
        item_list = []
        try:
            # Try to load the file as JSON and if there's an exception assume it's CSV
            with open(filename, 'rbU') as item_file:
                item_list = json.load(item_file)
        except ValueError:
            with open(filename, 'rbU') as item_file:
                reader = csv.reader(item_file, )
                header = reader.next()
                # Determine if the data is events or profiles based on keys in the header.
                # NOTE: this will fail if it were profile data with a property named 'event'
                if 'event' in header:
                    event_index = header.index("event")
                    distinct_id_index = header.index("distinct_id")
                    time_index = header.index("time")
                    for row in reader:
                        event = Mixpanel.event_object_from_csv_row(row, header, event_index, distinct_id_index,
                                                                   time_index)
                        item_list.append(event)
                elif '$distinct_id' in header:
                    distinct_id_index = header.index("$distinct_id")
                    for row in reader:
                        profile = Mixpanel.people_object_from_csv_row(row, header, distinct_id_index)
                        item_list.append(profile)
        except IOError:
            Mixpanel.LOGGER.warning("Error loading data from file: " + filename)

        return item_list

    @staticmethod
    def gzip_file(filename):
        """gzip an existing file

        :param filename: Path to a file to be gzipped
        :type filename: str

        """
        gzip_filename = filename + '.gz'
        with open(filename, 'rb') as f_in, gzip.open(gzip_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    @staticmethod
    def _export_data(data, output_file, format='json', compress=False):
        """Writes and optionally compresses Mixpanel data to disk in json or csv format

        :param data: A list of Mixpanel events or People profiles
        :param output_file: Name of file to write to
        :param format:  Output format can be 'json' or 'csv' (Default value = 'json')
        :param compress:  Option to gzip output (Default value = False)
        :type data: list
        :type output_file: str
        :type format: str
        :type compress: bool

        """
        with open(output_file, 'w+') as output:
            if format == 'json':
                json.dump(data, output)
            elif format == 'csv':
                Mixpanel.write_items_to_csv(data, output_file)
            else:
                msg = "Invalid format - must be 'json' or 'csv': format = " + str(format) + '\n' \
                      + "Dumping json to " + output_file
                Mixpanel.LOGGER.warning(msg)
                json.dump(data, output)

        if compress:
            Mixpanel.gzip_file(output_file)
            os.remove(output_file)

    @staticmethod
    def _prep_event_for_import(event, token, timezone_offset):
        """Takes an event dict and modifies it to meet the Mixpanel /import HTTP spec or dumps it to disk if it is invalid

        :param event: A Mixpanel event dict
        :param token: A Mixpanel project token
        :param timezone_offset: UTC offset (number of hours) of the timezone setting for the project that exported the
            data. Needed to convert the timestamp back to UTC prior to import.
        :type event: dict
        :type token: str
        :type timezone_offset: int
        :return: Mixpanel event dict with token added and timestamp adjusted to UTC
        :rtype: dict

        """
        # The /import API requires a 'time' and 'distinct_id' property, if either of those are missing we dump that
        # event to a log of invalid events and return
        if ('time' not in event['properties']) or ('distinct_id' not in event['properties']):
            Mixpanel.LOGGER.warning('Event missing time or distinct_id property, dumping to invalid_events.txt')
            with open('invalid_events.txt', 'a') as invalid:
                json.dump(event, invalid)
                invalid.write('\n')
                return
        event_copy = deepcopy(event)
        # transforms timestamp to UTC
        event_copy['properties']['time'] = int(int(event['properties']['time']) - (timezone_offset * 3600))
        event_copy['properties']['token'] = token
        return event_copy

    @staticmethod
    def _prep_params_for_profile(profile, token, operation, value, ignore_alias, dynamic):
        """Takes a People profile dict and returns the parameters for an /engage API update

        :param profile: A Mixpanel People profile dict
        :param token: A Mixpanel project token
        :param operation: A Mixpanel /engage API update operation
            https://mixpanel.com/help/reference/http#update-operations
        :param value: The value to use for the operation or a function that takes a People profile and returns the value
            to use
        :param ignore_alias: Option to bypass Mixpanel's alias lookup table
        :param dynamic: Should be set to True if value param is a function, otherwise false.
        :type profile: dict
        :type token: str
        :type operation: str
        :type ignore_alias: bool
        :type dynamic: bool
        :return: Parameters for a Mixpnael /engage API update
        :rtype: dict

        """
        # We use a dynamic flag parameter to avoid the overhead of checking the value parameter's type every time
        if dynamic:
            op_value = value(profile)
        else:
            op_value = value

        params = {
            '$ignore_time': True,
            '$ip': 0,
            '$ignore_alias': ignore_alias,
            '$token': token,
            '$distinct_id': profile['$distinct_id'],
            operation: op_value
        }

        return params

    @staticmethod
    def dt_from_iso(profile):
        """Takes a Mixpanel People profile and returns a datetime object for the $last_seen value or datetime.min if
        $last_seen is not set

        :param profile: A Mixpanel People profile dict
        :type profile: dict
        :return: A datetime representing the $last_seen value for the given profile
        :rtype: datetime

        """
        dt = datetime.datetime.min
        try:
            last_seen = profile["$properties"]["$last_seen"]
            try:
                # Try to use the MUCH faster ciso8601 library, if it's not installed use the built-in datetime library
                dt = ciso8601.parse_datetime_unaware(last_seen)
            except NameError:
                dt = datetime.datetime.strptime(last_seen, "%Y-%m-%dT%H:%M:%S")
        except KeyError:
            return dt
        return dt

    @staticmethod
    def sum_transactions(profile):
        """Returns a dict with a single key, 'Revenue' and the sum of all $transaction $amounts for the given profile as
        the value

        :param profile: A Mixpanel People profile dict
        :type profile: dict
        :return: A dict with key 'Revenue' and value containing the sum of all $transactions for the give profile
        :rtype: dict

        """
        total = 0
        try:
            transactions = profile['$properties']['$transactions']
            for t in transactions:
                total = total + t['$amount']
        except KeyError:
            pass
        return {'Revenue': total}

    def _get_engage_page(self, params):
        """Fetches and returns the response from an /engage request

        :param params: Query parameters for the /engage API
        :type params: dict
        :return: /engage API response object
        :rtype: dict

        """
        response = self.request(Mixpanel.FORMATTED_API, ['engage'], params)
        data = json.loads(response)
        if 'results' in data:
            return data
        else:
            Mixpanel.LOGGER.warning("Invalid response from /engage: " + response)
            return

    def _dispatch_batches(self, endpoint, item_list, prep_args):
        """Asynchronously sends batches of 50 items to the /import or /engage Mixpanel API endpoints

        :param endpoint: Can be either 'import' or 'engage'
        :param item_list: List of Mixpanel event data or People profiles
        :param prep_args: List of arguments to be provided to the appropriate _prep method in addition to the profile or
            event
        :type endpoint: str
        :type item_list: list
        :type prep_args: list

        """
        pool = ThreadPool(processes=self.pool_size)
        batch = []

        # Decide which _prep function to use based on the endpoint
        if endpoint == 'import':
            prep_function = Mixpanel._prep_event_for_import
        elif endpoint == 'engage':
            prep_function = Mixpanel._prep_params_for_profile
        else:
            Mixpanel.LOGGER.warning('endpoint must be "import" or "engage", found: ' + str(endpoint))
            return

        for item in item_list:
            # Insert the given item as the first argument to be passed to the _prep function determined above
            prep_args[0] = item
            params = prep_function(*prep_args)
            if params:
                batch.append(params)
            if len(batch) == 50:
                # Add an asynchronous call to _send_batch to the thread pool
                pool.apply_async(self._send_batch, args=(endpoint, batch), callback=Mixpanel.response_handler_callback)
                batch = []
        # If there are fewer than 50 updates left ensure one last call is made
        if len(batch):
            pool.apply_async(self._send_batch, args=(endpoint, batch), callback=Mixpanel.response_handler_callback)
        pool.close()
        pool.join()

    def _send_batch(self, endpoint, batch, retries=0):
        """POST a single batch of data to a Mixpanel API and return the response

        :param endpoint: Should be 'import' or 'engage'
        :param batch: List of Mixpanel event data or People updates to import.
        :param retries:  Max number of times to retry if we get a HTTP 503 response (Default value = 0)
        :type endpoint: str
        :type batch: list
        :type retries: int
        :raise: Raises for any HTTP error other than 503
        :return: HTTP response from Mixpanel API
        :rtype: dict

        """
        try:
            response = self.request(Mixpanel.IMPORT_API, [endpoint], batch, 'POST')
            msg = "Sent " + str(len(batch)) + " items on " + time.strftime("%Y-%m-%d %H:%M:%S") + "!"
            Mixpanel.LOGGER.debug(msg)
            return response
        except urllib2.HTTPError as err:
            # In the event of a 503 we will try to send again
            if err.code == 503:
                if retries < self.max_retries:
                    Mixpanel.LOGGER.warning("HTTP Error 503: Retry #" + str(retries + 1))
                    self._send_batch(endpoint, batch, retries + 1)
                else:
                    Mixpanel.LOGGER.warning("Failed to import batch, dumping to file import_backup.txt")
                    with open('import_backup.txt', 'a') as backup:
                        json.dump(batch, backup)
                        backup.write('\n')
            else:
                raise

    def request(self, base_url, path_components, params, method='GET'):
        """Base method for sending HTTP requests to the various Mixpanel APIs

        :param base_url: Ex: https://api.mixpanel.com
        :param path_components: endpoint path as list of strings
        :param params: dictionary containing the Mixpanel parameters for the API request
        :param method: GET or POST (Default value = 'GET')
        :type base_url: str
        :type path_components: list
        :type params: dict
        :type method: str
        :return: JSON data returned from API
        :rtype: str

        """
        if base_url == Mixpanel.IMPORT_API:
            base = [base_url]
        else:
            base = [base_url, str(Mixpanel.VERSION)]

        if method == 'POST':
            if 'jql' in path_components:
                payload = params
            else:
                payload = {"data": base64.b64encode(json.dumps(params)), "verbose": 1}
            data = Mixpanel.unicode_urlencode(payload)
            Mixpanel.LOGGER.debug('POST data: ' + data)
            request_url = '/'.join(base + path_components) + '/'
        else:
            data = None
            request_url = '/'.join(base + path_components) + '/?' + Mixpanel.unicode_urlencode(params)
        Mixpanel.LOGGER.debug("Request URL: " + request_url)
        headers = {'Authorization': 'Basic {encoded_secret}'.format(encoded_secret=base64.b64encode(self.api_secret))}
        request = urllib2.Request(request_url, data, headers)
        try:
            response = urllib2.urlopen(request, timeout=self.timeout)
        except urllib2.HTTPError as e:
            Mixpanel.LOGGER.warning('The server couldn\'t fulfill the request.')
            Mixpanel.LOGGER.warning('Error code: ' + str(e.code))
            Mixpanel.LOGGER.warning('Reason: ' + e.reason)
            if hasattr(e, 'read'):
                Mixpanel.LOGGER.warning('Response :' + e.read())
        except urllib2.URLError as e:
            Mixpanel.LOGGER.warning('We failed to reach a server.')
            Mixpanel.LOGGER.warning('Reason: ' + e.reason)
            if hasattr(e, 'read'):
                Mixpanel.LOGGER.warning('Response :' + e.read())
        else:
            response_data = response.read()
            return response_data

    def people_operation(self, operation, value, profiles=None, query_params=None, timezone_offset=None,
                         ignore_alias=False, backup=False, backup_file=None):
        """Base method for performing any of the People analytics update operations

        https://mixpanel.com/help/reference/http#update-operations

        :param operation: A string with name of a Mixpanel People operation, like $set or $delete
        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type operation: str
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str

        """
        assert self.token, "Project token required for People operation!"
        if profiles is not None and query_params is not None:
            Mixpanel.LOGGER.warning("profiles and query_params both provided, please use one or the other")
            return

        if profiles is not None:
            profiles_list = Mixpanel.list_from_argument(profiles)
        elif query_params is not None:
            profiles_list = self.query_engage(query_params, timezone_offset=timezone_offset)
        else:
            # If both profiles and query_params are None just fetch all profiles
            profiles_list = self.query_engage()

        if backup:
            if backup_file is None:
                backup_file = "backup_" + str(int(time.time())) + ".json"
            self._export_data(profiles_list, backup_file)

        # Set the dynamic flag to True if value is a function
        dynamic = isfunction(value)
        self._dispatch_batches('engage', profiles_list, [{}, self.token, operation, value, ignore_alias, dynamic])

    def people_delete(self, profiles=None, query_params=None, timezone_offset=None, ignore_alias=True, backup=True,
                      backup_file=None):
        """Deletes the specified People profiles with the $delete operation and optionally creates a backup file

        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = True)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int
        :type backup: bool
        :type backup_file: str

        """
        self.people_operation('$delete', '', profiles=profiles, query_params=query_params,
                              timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                              backup_file=backup_file)

    def people_set(self, value, profiles=None, query_params=None, timezone_offset=None, ignore_alias=False, backup=True,
                   backup_file=None):
        """Sets People properties for the specified profiles using the $set operation and optionally creates a backup file

        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str

        """
        self.people_operation('$set', value=value, profiles=profiles, query_params=query_params,
                              timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                              backup_file=backup_file)

    def people_set_once(self, value, profiles=None, query_params=None, timezone_offset=None, ignore_alias=False,
                        backup=False, backup_file=None):
        """Sets People properties for the specified profiles only if the properties do not yet exist, using the $set_once
        operation and optionally creates a backup file

        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str

        """
        self.people_operation('$set_once', value=value, profiles=profiles, query_params=query_params,
                              timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                              backup_file=backup_file)

    def people_unset(self, value, profiles=None, query_params=None, timezone_offset=None, ignore_alias=False,
                     backup=True, backup_file=None):
        """Unsets properties from the specified profiles using the $unset operation and optionally creates a backup file

        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type value: list | (profile) -> list
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str

        """
        self.people_operation('$unset', value=value, profiles=profiles, query_params=query_params,
                              timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                              backup_file=backup_file)

    def people_add(self, value, profiles=None, query_params=None, timezone_offset=None, ignore_alias=False, backup=True,
                   backup_file=None):
        """Increments numeric properties on the specified profiles using the $add operation and optionally creates a
        backup file

        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type value: dict[str, float] | (profile) -> dict[str, float]
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str

        """
        self.people_operation('$add', value=value, profiles=profiles, query_params=query_params,
                              timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                              backup_file=backup_file)

    def people_append(self, value, profiles=None, query_params=None, timezone_offset=None, ignore_alias=False,
                      backup=True, backup_file=None):
        """Appends values to list properties on the specified profiles using the $append operation and optionally creates
        a backup file.

        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type value: dict | (profile) -> dict
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str

        """
        self.people_operation('$append', value=value, profiles=profiles, query_params=query_params,
                              timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                              backup_file=backup_file)

    def people_union(self, value, profiles=None, query_params=None, timezone_offset=None, ignore_alias=False,
                     backup=True, backup_file=None):
        """Union a list of values with list properties on the specified profiles using the $union operation and optionally
        create a backup file

        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type value: dict[str, list] | (profile) -> dict[str, list]
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str

        """
        self.people_operation('$union', value=value, profiles=profiles, query_params=query_params,
                              timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                              backup_file=backup_file)

    def people_remove(self, value, profiles=None, query_params=None, timezone_offset=None, ignore_alias=False,
                      backup=True, backup_file=None):
        """Removes values from list properties on the specified profiles using the $remove operation and optionally
        creates a backup file

        :param value: Can be a static value applied to all profiles or a user-defined function (or lambda) that takes a
            profile as its only parameter and returns the value to use for the operation on the given profile
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type value: dict | (profile) -> dict
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str

        """
        self.people_operation('$remove', value=value, profiles=profiles, query_params=query_params,
                              timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                              backup_file=backup_file)

    def people_change_property_name(self, old_name, new_name, profiles=None, query_params=None, timezone_offset=None,
                                    ignore_alias=False, backup=True, backup_file=None, unset=True):
        """Copies the value of an existing property into a new property and optionally unsets the existing property.
        Optionally creates a backup file.

        :param old_name: The name of an existing property.
        :param new_name: The new name to replace the old_name with
        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. If both query_params and
            profiles are None all profiles with old_name set are targeted. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :param unset:  Option to unset the old_name property (Default value = True)
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str
        :type unset: bool


        """
        if profiles is None and query_params is None:
            query_params = {'selector': '(defined (properties["' + old_name + '"]))'}
        self.people_operation('$set', lambda p: {new_name: p['$properties'][old_name]}, query_params=query_params,
                              timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                              backup_file=backup_file)
        if unset:
            self.people_operation('$unset', [old_name], profiles=profiles, query_params=query_params,
                                  timezone_offset=timezone_offset, backup=False)

    def people_revenue_property_from_transactions(self, profiles=None, query_params=None, timezone_offset=None,
                                                  ignore_alias=False, backup=True, backup_file=None):
        """Creates a property named 'Revenue' for the specified profiles by summing their $transaction $amounts and
        optionally creates a backup file

        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles.
            Alternative to query_params. (Default value = None)
        :param query_params: Parameters to query /engage API. Alternative to profiles param. If both query_params and
            profiles are None, all profiles with $transactions are targeted. (Default value = None)
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type profiles: list | str
        :type query_params: dict
        :type timezone_offset: int
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str

        """
        if profiles is None and query_params is None:
            query_params = {'selector': '(defined (properties["$transactions"]))'}

        self.people_operation('$set', Mixpanel.sum_transactions, profiles=profiles, query_params=query_params,
                              timezone_offset=timezone_offset, ignore_alias=ignore_alias, backup=backup,
                              backup_file=backup_file)

    def deduplicate_people(self, profiles=None, prop_to_match='$email', merge_props=False, case_sensitive=False,
                           backup=True, backup_file=None):
        """Determines duplicate profiles based on the value of a specified property. The profile with the latest
        $last_seen is kept and the others are deleted. Optionally adds any properties from the profiles to be deleted to
        the remaining profile using $set_once. Backup files are always created.

        :param profiles: Can be a list of profiles or the name of a file containing a JSON array or CSV of profiles. If
            this is None all profiles with prop_to_match set will be downloaded. (Default value = None)
        :param prop_to_match: Name of property whose value will be used to determine duplicates
            (Default value = '$email')
        :param merge_props:  Option to call $set_once on remaining profile with all props from profiles to be deleted.
            This ensures that any properties that existed on the duplicates but not on the remaining profile are
            preserved. (Default value = False)
        :param case_sensitive:  Option to use case sensitive or case insensitive matching (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type profiles: list | str
        :type prop_to_match: str
        :type merge_props: bool
        :type case_sensitive: bool
        :type backup: bool
        :type backup_file: str

        """
        main_reference = {}
        update_profiles = []
        delete_profiles = []

        if profiles is not None:
            profiles_list = Mixpanel.list_from_argument(profiles)
        else:
            # Unless the user provides a list of profiles we only look at profiles which have the prop_to_match set
            selector = '(boolean(properties["' + prop_to_match + '"]) == true)'
            profiles_list = self.query_engage({'where': selector})

        if backup:
            if backup_file is None:
                backup_file = "backup_" + str(int(time.time())) + ".json"
            self._export_data(profiles_list, backup_file)

        for profile in profiles_list:
            try:
                match_prop = str(profile["$properties"][prop_to_match])
            except UnicodeError:
                match_prop = profile["$properties"][prop_to_match].encode('utf-8')
            except KeyError:
                continue
            finally:
                try:
                    if not case_sensitive:
                        match_prop = match_prop.lower()
                except NameError:
                    pass

            # Ensure each value for the prop we are matching on has a key pointing to an array in the main_reference
            if not main_reference.get(match_prop):
                main_reference[match_prop] = []

            # Append each profile to the array under the key corresponding to the value it has for prop we are matching
            main_reference[match_prop].append(profile)

        for matching_prop, matching_profiles in main_reference.iteritems():
            if len(matching_profiles) > 1:
                matching_profiles.sort(key=lambda dupe: Mixpanel.dt_from_iso(dupe))
                # We create a $delete update for each duplicate profile and at the same time create a
                # $set_once update for the keeper profile by working through duplicates oldest to newest
                if merge_props:
                    prop_update = {"$distinct_id": matching_profiles[-1]["$distinct_id"], "$properties": {}}
                for x in xrange(len(matching_profiles) - 1):
                    delete_profiles.append({'$distinct_id': matching_profiles[x]['$distinct_id']})
                    if merge_props:
                        prop_update["$properties"].update(matching_profiles[x]["$properties"])
                # Remove $last_seen from any updates to avoid weirdness
                if merge_props and "$last_seen" in prop_update["$properties"]:
                    del prop_update["$properties"]["$last_seen"]
                if merge_props:
                    update_profiles.append(prop_update)

        # The "merge" is really just a $set_once call with all of the properties from the deleted profiles
        if merge_props:
            self.people_operation('$set_once', lambda p: p['$properties'], profiles=update_profiles, ignore_alias=True,
                                  backup=False)

        self.people_operation('$delete', '', profiles=delete_profiles, ignore_alias=True, backup=False)

    def query_jql(self, script, params=None):
        """Query the Mixpanel JQL API

        https://mixpanel.com/help/reference/jql/api-reference#api/access

        :param script: String containing a JQL script to run
        :param params: Optional dict that will be made available to the script as the params global variable.
        :type script: str
        :type params: dict

        """
        query_params = {"script": script}
        if params is not None:
            query_params["params"] = json.dumps(params)

        response = self.request(Mixpanel.FORMATTED_API, ['jql'], query_params, method='POST')
        return json.loads(response)

    def jql_operation(self, jql_script, people_operation, update_value, jql_params=None, ignore_alias=False,
                      backup=True, backup_file=None):
        """Perform a JQL query to return a JSON array of objects that can then be used to dynamically construct People
            updates via the update_value

        :param jql_script: String containing a JQL script to run. The result should be an array of objects. Those
            objects should contain at least a $distinct_id key
        :param people_operation: A Mixpanel People update operation
        :param update_value: Can be a static value applied to all $distinct_ids or a user defined function or lambda
            that expects a dict containing at least a $distinct_id key as its only parameter and returns the value to
            use for the update on that $distinct_id
        :param jql_params: Optional dict that will be made available to the script as the params global variable.
            (Default value = None)
        :param ignore_alias: True or False (Default value = False)
        :param backup: True to create backup file otherwise False (default)
        :param backup_file: Optional filename to use for the backup file (Default value = None)
        :type jql_script: str
        :type people_operation: str
        :type jql_params: dict
        :type ignore_alias: bool
        :type backup: bool
        :type backup_file: str

        """

        jql_data = self.query_jql(jql_script, jql_params)

        if backup:
            if backup_file is None:
                backup_file = "backup_" + str(int(time.time())) + ".json"
            all_profiles = self.query_engage()
            self._export_data(all_profiles, backup_file)

        self.people_operation(people_operation, update_value, profiles=jql_data, ignore_alias=ignore_alias,
                              backup=False)

    def event_counts_to_people(self, from_date, events):
        """Sets the per user count of events in events list param as People properties

        :param from_date: A datetime or a date string of format YYYY-MM-DD to begin counting from
        :param events: A list of strings of event names to be counted
        :type from_date: datetime | str
        :type events: list[str]

        """

        jql_script = "function main() {var event_selectors_array = [];_.each(params.events, function(e) " \
                     "{event_selectors_array.push({'event':e});});return join(Events({from_date:params.from_date," \
                     "to_date:params.to_date,event_selectors:event_selectors_array}), People()).filter(" \
                     "function(tuple) {return tuple.user && tuple.event}).groupByUser(['event.name'], mixpanel." \
                     "reducer.count()).map(function(row) {return {$distinct_id:row.key[0], event:row.key[1], " \
                     "count:row.value}})}"

        to_date = datetime.datetime.today().strftime('%Y-%m-%d')

        if isinstance(from_date, datetime.date):
            from_date = from_date.strftime('%Y-%m-%d')

        params = {'from_date': from_date, 'to_date': to_date, 'events': events}
        self.jql_operation(jql_script, '$set', lambda x: {x['event']: x['count']}, jql_params=params, backup=False)

    def query_export(self, params):
        """Queries the /export API and returns a list of Mixpanel event dicts

        https://mixpanel.com/help/reference/exporting-raw-data#export-api-reference

        :param params: Parameters to use for the /export API request
        :type params: dict
        :return: A list of Mixpanel event dicts
        :rtype: list

        """
        response = self.request(Mixpanel.RAW_API, ['export'], params)
        try:
            file_like_object = cStringIO.StringIO(response)
        except TypeError as e:
            Mixpanel.LOGGER.warning('Error querying /export API')
            return
        raw_data = file_like_object.getvalue().split('\n')
        # Remove the last line which is only a newline
        raw_data.pop()
        events = []
        for line in raw_data:
            events.append(json.loads(line))
        return events

    def query_engage(self, params={}, timezone_offset=None):
        """Queries the /engage API and returns a list of Mixpanel People profile dicts

        https://mixpanel.com/help/reference/data-export-api#people-analytics

        :param params: Parameters to use for the /engage API request. Defaults to returning all profiles.
            (Default value = {})
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :type params: dict
        :type timezone_offset: int
        :raise RuntimeError: Raises Runtime error if params include behaviors and timezone_offset is None
        :return: A list of Mixpanel People profile dicts
        :rtype: list

        """
        if 'behaviors' in params and timezone_offset is None:
            raise RuntimeError("timezone_offset required if params include behaviors")
        elif 'behaviors' in params:
            params['as_of_timestamp'] = int(int(time.time()) + (timezone_offset * 3600))

        paginator = ConcurrentPaginator(self._get_engage_page, concurrency=self.pool_size)
        return paginator.fetch_all(params)

    def export_events(self, output_file, params, format='json', compress=False):
        """Queries the /export API and writes the Mixpanel event data to disk as a JSON or CSV file. Optionally gzip file.

        https://mixpanel.com/help/reference/exporting-raw-data#export-api-reference

        :param output_file: Name of the file to write to
        :param params: Parameters to use for the /export API request
        :param format: Can be either 'json' or 'csv' (Default value = 'json')
        :param compress: Option to gzip output_file (Default value = False)
        :type output_file: str
        :type params: dict
        :type format: str
        :type compress: bool

        """
        # Increase timeout to 15 minutes if it's still set to default
        if self.timeout == 120:
            self.timeout = 900
        events = self.query_export(params)
        Mixpanel._export_data(events, output_file, format=format, compress=compress)

    def export_people(self, output_file, params={}, timezone_offset=None, format='json', compress=False):
        """Queries the /engage API and writes the Mixpanel People profile data to disk as a JSON or CSV file. Optionally
        gzip file.

        https://mixpanel.com/help/reference/data-export-api#people-analytics

        :param output_file: Name of the file to write to
        :param params: Parameters to use for the /engage API request (Default value = {})
        :param timezone_offset: UTC offset in hours of project timezone setting, used to calculate as_of_timestamp
            parameter for queries that use behaviors. Required if query_params contains behaviors (Default value = None)
        :param format:  (Default value = 'json')
        :param compress:  (Default value = False)
        :type output_file: str
        :type params: dict
        :type timezone_offset: int
        :type format: str
        :type compress: bool

        """
        profiles = self.query_engage(params, timezone_offset=timezone_offset)
        Mixpanel._export_data(profiles, output_file, format=format, compress=compress)

    def import_events(self, data, timezone_offset):
        """Imports a list of Mixpanel event dicts or a file containing a JSON array of Mixpanel events.

        https://mixpanel.com/help/reference/importing-old-events

        :param data: A list of Mixpanel event dicts or the name of a file containing a JSON array or CSV of Mixpanel
            events
        :param timezone_offset: UTC offset (number of hours) for the project that exported the data. Used to convert the
            event timestamps back to UTC prior to import.
        :type data: list | str
        :type timezone_offset: int

        """
        self._import_data(data, 'import', timezone_offset=timezone_offset)

    def import_people(self, data, ignore_alias=False):
        """Imports a list of Mixpanel People profile dicts or a file containing a JSON array or CSV of Mixpanel People
            profiles

        https://mixpanel.com/help/reference/data-export-api#people-analytics

        :param data: A list of Mixpanel People profile dicts or the name of a file containing a JSON array of
            Mixpanel People profiles.
        :param ignore_alias: Option to bypass Mixpanel's alias lookup table (Default value = False)
        :type data: list | str
        :type ignore_alias: bool

        """
        self._import_data(data, 'engage', ignore_alias=ignore_alias)

    def _import_data(self, data, endpoint, timezone_offset=None, ignore_alias=False):
        """Base method to import either event data or People profile data as a list of dicts or from a JSON array
        file

        :param data: A list of event or People profile dicts or the name of a file containing a JSON array or CSV of
            events or People profiles
        :param endpoint: can be 'import' or 'engage'
        :param timezone_offset: UTC offset (number of hours) for the project that exported the data. Used to convert the
            event timestamps back to UTC prior to import. (Default value = 0)
        :param ignore_alias: Option to bypass Mixpanel's alias lookup table (Default value = False)
        :type data: list | str
        :type endpoint: str
        :type timezone_offset: int
        :type ignore_alias: bool

        """
        assert self.token, "Project token required for import!"
        item_list = Mixpanel.list_from_argument(data)
        # Create a list of arguments to be used in one of the _prep functions later
        args = [{}, self.token]
        if endpoint == 'import':
            args.append(timezone_offset)
        elif endpoint == 'engage':
            args.extend(['$set', lambda profile: profile['$properties'], ignore_alias, True])

        self._dispatch_batches(endpoint, item_list, args)


################################################################################
# Below this is the actual written script above is the mixpanel_api module #####
################################################################################
# mixpanel = Mixpanel ('API_Secret','TOKEN')
mixpanel = Mixpanel ('API_Secret','TOKEN')

#deduplicate_people(profiles=None, prop_to_match='$email', merge_props=False, case_sensitive=False, backup=True, backup_file=None)

#deduplicate_people(prop_to_match='$name',merge_props=True)

mixpanel.deduplicate_people(prop_to_match='userId',merge_props=False)