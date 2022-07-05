"""
Copyright (c) 2020, Battelle Memorial Institute
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
The views and conclusions contained in the software and documentation are those
of the authors and should not be interpreted as representing official policies,
either expressed or implied, of the FreeBSD Project.
This material was prepared as an account of work sponsored by an agency of the
United States Government. Neither the United States Government nor the United
States Department of Energy, nor Battelle, nor any of their employees, nor any
jurisdiction or organization that has cooperated in th.e development of these
materials, makes any warranty, express or implied, or assumes any legal
liability or responsibility for the accuracy, completeness, or usefulness or
any information, apparatus, product, software, or process disclosed, or
represents that its use would not infringe privately owned rights.
Reference herein to any specific commercial product, process, or service by
trade name, trademark, manufacturer, or otherwise does not necessarily
constitute or imply its endorsement, recommendation, or favoring by the
United States Government or any agency thereof, or Battelle Memorial Institute.
The views and opinions of authors expressed herein do not necessarily state or
reflect those of the United States Government or any agency thereof.

PACIFIC NORTHWEST NATIONAL LABORATORY
operated by BATTELLE for the UNITED STATES DEPARTMENT OF ENERGY
under Contract DE-AC05-76RL01830
"""


import os
import csv
import logging
from datetime import datetime, timedelta
from dateutil import parser
import dateutil.tz

from .information_service_model import InformationServiceModel
from .measurement_type import MeasurementType
from .interval_value import IntervalValue
from .utils.log import setup_logging
from .helpers import format_timestamp


setup_logging()
_log = logging.getLogger(__name__)


class TemperatureForecastModel(InformationServiceModel):
    """
    Predict hourly temperature (F)
    Use CSV as we don't have internet access for now. Thus keep the csv file as small as possible
    This can be changed to read from real-time data source such as WU
    """

    def __init__(self,
                 location: dict = None,
                 temperature_point_name: str = '',
                 tmy_fallback_path: str = '',
                 weather_file_path: str = None,
                 *args, **kwargs):
        super(TemperatureForecastModel, self).__init__(*args, **kwargs)
        self.weather_file_path = str(weather_file_path)
        self.weather_data = []

        # TODO: Generalize timezone handling.
        try:
            self.local_tz = dateutil.tz.tzlocal()
        except Exception:
            _log.warning("Problem automatically determining timezone! - Default to UTC.")
            self.local_tz = "US/Pacific"
            self.local_tz = dateutil.tz.gettz(self.local_tz)

        if self.weather_file_path is not None:
            self.init_weather_data()
            self.update_information = self.get_forecast_file
        else:
            self.update_information = self.get_forecast_weather_service
            self.location = [dict(location)] if location else []
            self.oat_point_name = str(temperature_point_name) if temperature_point_name else "air_temperature"
            if tmy_fallback_path:
                self.weather_file_path = str(tmy_fallback_path)
                self.init_weather_data()

    def init_weather_data(self):
        """
        To init or re-init weather data from file.
        :return:
        """
        # Get latest modified time
        cur_modified = os.path.getmtime(self.weather_file_path)

        if self.last_modified is None or cur_modified != self.last_modified:
            self.last_modified = cur_modified

            # Clear weather_data for re-init
            self.weather_data = []
            try:
                with open(self.weather_file_path) as f:
                    reader = csv.DictReader(f)
                    self.weather_data = [r for r in reader]
                    for rec in self.weather_data:
                        rec['Timestamp'] = parser.parse(rec['Timestamp']).replace(minute=0, second=0, microsecond=0)
                        rec['Value'] = float(rec['Value'])
            except Exception:
                self.weather_data = []
                _log.debug("WEATHER - problem parsing weather file!")

    def map_forecast_to_interval(self, mkt, weather_data):
        items = []
        predicted_values = []
        for ti in mkt.timeIntervals:
            # Find item which has the same timestamp as ti.timeStamp
            start_time = ti.startTime.replace(minute=0)
            previous_measurement = items
            items = [x[1] for x in weather_data if x[0] == start_time]
            # Create interval value and add it to predicted values
            if items:
                temp = items[0]
                interval_value = IntervalValue(self, ti, mkt, MeasurementType.PredictedValue, temp)
                predicted_values.append(interval_value)
            elif previous_measurement:
                temp = previous_measurement[0]
                interval_value = IntervalValue(self, ti, mkt, MeasurementType.PredictedValue, temp)
                predicted_values.append(interval_value)
                items = previous_measurement
            else:
                _log.debug("Cannot assign WEATHER information for interval: {}".format(ti))
        if len(mkt.timeIntervals) == len(predicted_values):
            self.predictedValues = predicted_values
            return True
        else:
            _log.debug("WEATHER data problem when assigning forecast {}".format(len(predicted_values)))
            return False

    def query_weather_data(self):
        """
        Implement query of remote weather data here.
        """
        return []

    def get_forecast_weather_service(self, mkt):
        """
        Gets weather data from a weather service using query_weather_data (which needs to be implemented in a subclass).
        :param mkt:
        :return:
        """
        if mkt.name.startswith("Day"):
            _log.debug("Starting Day Ahead Market reinitialize temperature predictions store.")
        if mkt.name.startswith("Real") and self.predictedValues:
            _log.debug("Realtime market, temperature predictions exist")
            return
        weather_data = self.query_weather_data()
        if not self.map_forecast_to_interval(mkt, weather_data):
            if self.predictedValues:
                hour_gap = mkt.timeIntervals[0].startTime - self.predictedValues[0].timeInterval.startTime
                max_hour_gap = timedelta(hours=72)
                if hour_gap > max_hour_gap:
                    self.predictedValues = []
                    if self.weather_data:
                        _log.debug("WEATHER using fallback1 tmy data for forecast!")
                        self.get_forecast_file(mkt)
                else:
                    predicted_values = []
                    for i in range(1, len(mkt.timeIntervals)):
                        interval_value = IntervalValue(self, mkt.timeIntervals[i], mkt, MeasurementType.PredictedValue,
                                                       self.predictedValues[i].value)
                        predicted_values.append(interval_value)
                    self.predictedValues = predicted_values
            else:
                if self.weather_data:
                    _log.debug("WEATHER using fallback2 tmy data for forecast!")
                    self.get_forecast_file(mkt)

    def get_forecast_file(self, mkt):
        self.init_weather_data()
        # Copy weather data to predictedValues
        self.predictedValues = []
        _log.info("get_forecast_file: {}".format(mkt.timeIntervals))
        for ti in mkt.timeIntervals:
            # Find item which has the same timestamp as ti.timeStamp
            start_time = ti.startTime.replace(minute=0)
            items = [x for x in self.weather_data if x['Timestamp'] == start_time]
            if len(items) == 0:
                trial_deltas = [-1, 1, -2, 2, -24, 24]
                for delta in trial_deltas:
                    items = [x for x in self.weather_data if x['Timestamp'] == (start_time - timedelta(hours=delta))]
                    if len(items) > 0:
                        break

                # None exist, raise exception
                if len(items) == 0:
                    raise Exception('No weather data for time: {}'.format(format_timestamp(ti.startTime)))

            # Create interval value and add it to predicted values
            temp = items[0]['Value']
            interval_value = IntervalValue(self, ti, mkt, MeasurementType.PredictedValue, temp)
            self.predictedValues.append(interval_value)


if __name__ == '__main__':
    from market import Market
    import helpers

    # TODO: Move to tests directory and provide a valid path for configuration.
    forecaster = TemperatureForecastModel(
        '/home/hngo/PycharmProjects/volttron-applications/pnnl/TNSAgent/campus_config.json')

    # Create market with some time intervals
    market = Market()
    market.marketClearingTime = datetime.now().replace(minute=0, second=0, microsecond=0)
    market.nextMarketClearingTime = market.marketClearingTime + market.marketClearingInterval

    market.check_intervals()

    # Test update_information
    forecaster.update_information(market)

    times = [helpers.format_ts(x.timeInterval.startTime) for x in forecaster.predictedValues]

    print(times)
