import logging
import json

import requests
import iso8601
import pandas as pd

logging = logging.getLogger(__name__)


class NDFD_v3(object):
    """
        National Digital Forecast Database
    """

    def __init__(self, lat, lon):

        self.lat = lat
        self.lon = lon

    def _get_grid_url(self):

        headers = {'accept': 'application/geo+json',
                   'user-agent': 'serchlights@fs.fed.us'}

        url = 'https://api.weather.gov/points/{},{}'.format(self.lat, self.lon)
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            logging.info(response.url)
            result = json.loads(response.content)
            return result['properties']['forecastGridData']
        except requests.exceptions.RequestException, e:
            logging.error(e)
            raise e

    def _get_grid_data(self, url):

        headers = {'accept': 'application/geo+json',
                   'user-agent': 'serchlights@fs.fed.us'}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            logging.info(response.url)
            result = json.loads(response.content)
            return result
        except requests.exceptions.RequestException, e:
            logging.error(e)
            raise e

    def _process_result(self, result, parameter):
        records = result['properties'][parameter]['values']
        timeindex = [pd.Timestamp(iso8601.parse_date(vt['validTime'].split('/')[0])) for vt in records]
        values = [v['value'] for v in records]
        return pd.DataFrame({parameter: values}, index=timeindex)


    def to_dataframe(self):

        result = self._get_grid_data(self._get_grid_url())

        df0 = self._process_result(result, 'temperature')
        df1 = self._process_result(result, 'windSpeed')
        df2 = self._process_result(result, 'skyCover')
        df3 = self._process_result(result, 'relativeHumidity')

        df = df0.join([df1, df2, df3], how='inner')
        return df