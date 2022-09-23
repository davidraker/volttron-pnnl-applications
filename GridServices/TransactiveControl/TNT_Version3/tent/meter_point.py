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


# A MeterPoint may correlate directly with a meter. It necessarily
# corresponds to one measurement type (see MeasurementType enumeration) and
# measurement location within the circuit. Therefore, a single physical
# meter might be the source of more than one MeterPoint.

import weakref
from datetime import timedelta
from math import ceil
from typing import Union

from .measurement_type import MeasurementType
from .measurement_unit import MeasurementUnit
from .timer import Timer
from .utils.time_series_buffer import TimeSeriesBuffer
from .helpers import format_ts, format_date


class MeterPoint(TimeSeriesBuffer):
    def __init__(self,
                 data_period_seconds: float = 10,
                 description: str = '',
                 max_buffer_seconds: float = 3600,
                 # TODO: Is measurement_interval intended as the market interval or the polling period?
                 measurement_interval: Union[timedelta, int] = timedelta(hours=1),
                 measurement_type: Union[MeasurementType, str, int] = MeasurementType.Unknown,
                 measurement_unit: Union[MeasurementUnit, str, int] = MeasurementUnit.Unknown,
                 name: str = '',
                 scale_factor: float = 1.0,
                 transactive_node=None
                 ):
        max_len = ceil(max_buffer_seconds / data_period_seconds) + 1
        super(MeterPoint, self).__init__(maxlen=max_len)
        self.tn = weakref.ref(transactive_node) if transactive_node else None

        # These are static properties that may be passed as parameters:
        self.description = str(description)
        self.measurementInterval = measurement_interval if isinstance(measurement_interval, timedelta)\
            else timedelta(seconds=measurement_interval)

        if isinstance(measurement_type, MeasurementType):
            self.measurementType = measurement_type
        elif isinstance(measurement_type, int):
            self.measurementType = MeasurementType(measurement_type)
        else:
            self.measurementType = MeasurementType[measurement_type]

        if isinstance(measurement_unit, MeasurementUnit):
            self.measurementUnit = measurement_unit
        elif isinstance(measurement_unit, int):
            self.measurementUnit = MeasurementUnit(measurement_unit)
        else:
            self.measurementUnit = MeasurementUnit[measurement_unit]
        self.name = str(name)
        self.scale_factor = float(scale_factor)  # Use -1 to reverse the sign of the meter.

        # These following properties are dynamically assigned and should not be assigned during meter configuration:
        # TODO: Replace with TimeSeriesBuffer functionality.
        self.current_hour_measurements = []
        self.filtered_measurement = None  # Average or some other aggregate metric
        self.current_measurement = None  # Last actual value.
        self.lastUpdate = None

    def set_meter_value(self, value, last_update=None):
        last_update = last_update if last_update else Timer.get_cur_time()
        self.append(self.scale_factor * value, last_update)
        self.lastUpdate = last_update

    def filter_measurements(self):
        # TODO: Make this interval agnostic. Replace with TSBuffer functionality.
        if len(self.current_hour_measurements) > 30:
            self.filtered_measurement = sum(self.current_hour_measurements) / len(self.current_hour_measurements)
            self.current_hour_measurements = []
        return self.filtered_measurement

    def store(self):
        """
        Store last measurement into historian
        The default approach here could be to append a text record file. If the file is reserved for one meterpoint,
        little or no metadata need be repeated in records. Minimum content should be reading time and datum.
        Implementers will be found to have diverse practices for historians.
        """
        pass

    def getDict(self):
        meter_point_dict = {
            "description": self.description,
            "measurementInterval": self.measurementInterval,
            "measurementType": self.measurementType,
            "measurementUnit": self.measurementUnit,
            "meter_point_name": self.name,
            "lastUpdate": self.lastUpdate
        }
        return meter_point_dict

    @classmethod
    def get_meters_by_name(cls, meter_list, name):
        return [m for m in meter_list if m.name == name and isinstance(m, cls)]

    @classmethod
    def get_meter_by_name(cls, meter_list, name):
        return MeterPoint.get_meters_by_name(meter_list, name)[0]

    @classmethod
    def get_meters_by_measurement_type(cls, meter_list, measurement_type):
        return [m for m in meter_list if m.measurement_type == measurement_type and isinstance(m, cls)]

    @classmethod
    def get_meter_by_measurement_type(cls, meter_list, measurement_type):
        return MeterPoint.get_meters_by_measurement_type(meter_list, measurement_type)[0]