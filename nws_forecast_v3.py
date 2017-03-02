import logging
import json
import datetime

import requests
import iso8601
import isodate
import pandas as pd

logging = logging.getLogger(__name__)


class GridPoint(object):
    """
       https://forecast-v3.weather.gov/documentation
    """

    def __init__(self, lat, lon, user='serchlights@fs.fed.us'):

        self.lat = lat
        self.lon = lon
        self.headers = {'accept': 'application/geo+json',
                        'user-agent': user}

    def _get_grid_url(self):

        url = 'https://api.weather.gov/points/{},{}'.format(self.lat, self.lon)
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            logging.info(response.url)
            result = json.loads(response.content)
            return result['properties']['forecastGridData']
        except requests.exceptions.RequestException, e:
            logging.error(e)
            raise e

    def _get_grid_data(self, url):

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            logging.info(response.url)
            result = json.loads(response.content)
            return result
        except requests.exceptions.RequestException, e:
            logging.error(e)
            raise e

    def _process_result(self, result, property):
        records = result['properties'][property]['values']
        index = []
        values = []
        for record in records:
            timestamp = iso8601.parse_date(record['validTime'].split('/')[0])
            duration = isodate.parse_duration(record['validTime'].split('/')[1])
            while duration > datetime.timedelta(days=0, hours=0):
                index.append(pd.Timestamp(timestamp))
                values.append(record['value'])
                timestamp = timestamp + datetime.timedelta(hours=1)
                duration = duration - datetime.timedelta(hours=1)
        return pd.DataFrame({property: values}, index=index)


    def to_dataframe(self):

        df = pd.DataFrame()
        result = self._get_grid_data(self._get_grid_url())
        meta = ['@id', '@type', 'updateTime', 'validTimes', 'elevation',
                'forecastOffice', 'gridId', 'gridX', 'gridY']
        for property in result['properties'].keys():
            if property not in meta:
                if df.empty:
                    df = self._process_result(result, property)
                else:
                    df[property] = self._process_result(result, property)
        df.index = pd.to_datetime(df.index, utc=True)
        return df