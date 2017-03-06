import logging
import json
import datetime

import requests
import isodate
import pandas as pd

logging = logging.getLogger(__name__)


class GridPoint(object):
    """
       https://forecast-v3.weather.gov/documentation
    """

    def __init__(self, lat, lon, user='https://github.com/jgcobb3/nws-forecast-v3'):

        self.lat = lat
        self.lon = lon
        self.user = user

    def _grid_url(self):

        url = 'https://api.weather.gov/points/{},{}'.format(self.lat, self.lon)
        result = self._get(url)
        return result['properties']['forecastGridData']

    def _get(self, url):

        try:
            headers = {'accept': 'application/geo+json',
                       'user-agent': self.user}
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            logging.info(response.url)
            result = json.loads(response.content)
            return result
        except requests.exceptions.RequestException, e:
            logging.error(e)
            raise e

    def _read_property(self, result, property):

        df = pd.DataFrame(columns=[property])
        records = result['properties'][property]['values']
        for record in records:
            index = self._build_index(record['validTime'])
            values = [record['value']] * len(index)
            df = df.append(pd.DataFrame(values, index=index, columns=[property]))
        return df

    def _build_index(self, validTime):

        index = []
        timestamp = isodate.parse_datetime(validTime.split('/')[0])
        duration = isodate.parse_duration(validTime.split('/')[1])
        while duration > datetime.timedelta(days=0, hours=0):
            index.append(pd.Timestamp(timestamp))
            timestamp = timestamp + datetime.timedelta(hours=1)
            duration = duration - datetime.timedelta(hours=1)
        return pd.to_datetime(index, utc=True)

    def to_dataframe(self):

        result = self._get(self._grid_url())
        df = pd.DataFrame(index=self._build_index(result['properties']['validTimes']))
        meta = ['@id', '@type', 'updateTime', 'validTimes', 'elevation',
                'forecastOffice', 'gridId', 'gridX', 'gridY', 'weather']
        for property in result['properties'].keys():
            if property not in meta:
                df[property] = self._read_property(result, property)
        return df