from nws_forecast_v3 import GridPoint

class CattleHeatStress(object):

    def __init__(self, lat, lon):
        self._chs = self._forecast(lat, lon)

    def forecast(self):
        return self._chs

    def daily_max(self):
        return self._chs.resample('D').max()

    def summary(self):
        return [{'date': day.strftime('%m/%d/%Y'),
                 'color': self.color_code(value),
                 'condition': self.categorize(value)}
                for day, value in self.daily_max().iteritems()]

    def _forecast(self, lat, lon):

        df = GridPoint(lat, lon).to_dataframe()
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