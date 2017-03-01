import logging
import json
import re
import datetime

import requests
import iso8601
import isodate
import pandas as pd

logging = logging.getLogger(__name__)


class NDFD_v3(object):
    """
       https://forecast-v3.weather.gov/documentation
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
        return pd.DataFrame({parameter: values}, index=index)

    def to_dataframe(self):

        result = self._get_grid_data(self._get_grid_url())

        df0 = self._process_result(result, 'temperature')
        df1 = self._process_result(result, 'windSpeed')
        df2 = self._process_result(result, 'skyCover')
        df3 = self._process_result(result, 'relativeHumidity')
        df = df0.join([df1, df2, df3])
        return df


class CattleHeatStress(object):

    def __init__(self, lat, lon):
        self._chs = self._forecast(lat, lon)

    def forecast(self):
        return self._chs

    def daily_max(self):
        self._chs.index = pd.to_datetime(self._chs.index, utc=True)
        return self._chs.resample('D').max()

    def summary(self):
        return [{'date': day.strftime('%m/%d/%Y'),
                 'color': self.color_code(value),
                 'condition': self.categorize(value)}
                for day, value in self.daily_max().iteritems()]

    def _forecast(self, lat, lon):

        df = NDFD_v3(lat, lon).to_dataframe()
        return df.apply(lambda row: self.calculate(row.temperature,
                                                   row.relativeHumidity,
                                                   row.windSpeed,
                                                   row.skyCover), axis=1)

    @staticmethod
    def calculate(temperature, relative_humidity, wind_speed, cloud_amount):

        # Approximate solar radiation.
        solar = 1110 - (8.9 * cloud_amount)

        # Convert units
        temperature = temperature * 1.8 + 32
        wind_speed = wind_speed * 2.23694

        # Calculate cattle heat stress, see:
        # http://www.ars.usda.gov/Main/docs.htm?docid=15616
        breathing_rate = (2.83 * temperature) + (0.58 * relative_humidity) - (0.76 * wind_speed) + (0.039 * solar) - 196.4

        # Set minimum and maximum values.
        breathing_rate = 10 if breathing_rate < 10 else breathing_rate
        breathing_rate = 150 if breathing_rate > 150 else breathing_rate

        return breathing_rate

    @staticmethod
    def categorize(breathing_rate):

        if breathing_rate < 90:
            return 'NORMAL'
        elif 90 <= breathing_rate < 110:
            return 'ALERT'
        elif 110 <= breathing_rate < 130:
            return 'DANGER'
        elif 130 <= breathing_rate:
            return 'EMERGENCY'

    @staticmethod
    def color_code(breathing_rate):

        color = '#FFFFFF'
        if breathing_rate < 90:
            color = '#0099ff'
        elif 90 <= breathing_rate < 110:
            color = '#ffff00'
        elif 110 <= breathing_rate < 130:
            color = '#ff9933'
        elif 130 <= breathing_rate:
            color = '#ff0000'
        return color
