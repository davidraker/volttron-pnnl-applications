import logging
from datetime import timedelta


from tent.meter_point import MeterPoint, MeasurementType, MeasurementUnit
from tent.helpers import *
from utils.time_series_buffer import TimeSeriesBuffer

setup_logging()
_log = logging.getLogger(__name__)


class PullMeterPoint(MeterPoint):
    def __init__(self, *args, **kwargs):
        super(PullMeterPoint, self).__init__(*args, **kwargs)

    def read_meter(self, obj):
        # Read the meter point at scheduled intervals
        #
        # MeterPoints are updated on a schedule. Properties have been defined to keep track of the time of the last
        # update and the interval between updates.
        #
        # While this seems easy, meters will be found to be diverse and may use diverse standards and protocols. Create
        # subclasses and redefine this function as needed to handle unique conditions.
        _log.debug('Made it to MeterPoint.read_meter() for ' + obj.name)
