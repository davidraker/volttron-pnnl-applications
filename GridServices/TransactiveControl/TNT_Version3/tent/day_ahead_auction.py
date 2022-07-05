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


from .auction import Auction
from .market_state import MarketState
from datetime import timedelta
from .real_time_auction import RealTimeAuction
from .data_manager import *


class DayAheadAuction(Auction):

    def __init__(self, *args, **kwargs):
        super(DayAheadAuction, self).__init__(*args, **kwargs)

    def spawn_markets(self, this_transactive_node, new_market_clearing_time):

        # 200910DJH: This is where you may change between 15-minute and 60-minute real-time refinement intervals.
        real_time_market_duration = self.real_time_duration

        # First, go ahead and use the base method and current market to create the next member of this market series,
        # as was intended. This should instantiate the new market,  create its time intervals, and initialize marginal
        # prices for the new market.
        Auction.spawn_markets(self, this_transactive_node, new_market_clearing_time)

        # Next, the new day-ahead market instantiates all the real-time markets that will correct the new day-ahead
        # intervals. There are several ways to retrieve the market that was just created, but this approach should be
        # pretty foolproof.
        market = [x for x in this_transactive_node.markets if x.marketClearingTime == self.nextMarketClearingTime and
                  x.marketSeriesName == self.marketSeriesName][0]

        # Something is seriously wrong if the recently instantiated market cannot be found. Raise an error and stop.
        # if market is None or len(market) == 0:
        if market is None:
            raise ('No predecessor of ' + self.name + ' could be found.')

        # Gather all the day-ahead market period start times and order them.
        market_interval_start_times = [x.startTime for x in market.timeIntervals]
        market_interval_start_times.sort()

        # 210401DJH: This initialization of new_market_list had been misplaced, thus causing only the final hour's Real-
        #            Time markets to be captured as CSV records. It should be initialized prior to looping through the
        #            Day-Ahead market hours, as placed here.
        new_market_list = []

        for i in range(len(market_interval_start_times)):

            interval_start = market_interval_start_times[i]
            interval_end = interval_start + market.intervalDuration

            # 210401DJH: This initialization of new_market_list is misplaced, thus causing only the final hour's Real-
            #            Time markets to be captured as CSV records.
            # new_market_list = []
            while interval_start < interval_end:
                # Find the prior market in this series. It should be the one that shares the same market series name and
                # is, until now, the newest market in the series.
                prior_market = [x for x in this_transactive_node.markets
                                if x.marketSeriesName == "Real-Time Auction"
                                and x.isNewestMarket is True]

                if prior_market is None or len(prior_market) == 0:
                    # This is most likely a startup issue when the prior market in the series cannot be found.
                    Warning('No prior markets were found in market series: Real-Time Auction')
                    price_model = self.priceModel
                    default_price = self.defaultPrice
                    future_horizon = real_time_market_duration
                    prior_market_in_series = None

                else:
                    # The prior market was found. These attributes may be adopted from the prior market in the series.
                    prior_market = prior_market[0]
                    prior_market.isNewestMarket = False
                    price_model = prior_market.priceModel
                    default_price = prior_market.defaultPrice
                    future_horizon = prior_market.futureHorizon
                    prior_market_in_series = prior_market

                # TODO: Many of these (lead times, for instance) are hard-coded. Perhaps this should come from updating
                #  a stored config and passing it to the constructor as **config?
                # Instantiate a new real-time market.
                new_market = RealTimeAuction(
                    market_to_be_refined=market,
                    market_clearing_interval=real_time_market_duration,
                    market_series_name="Real-Time Auction",
                    delivery_lead_time=timedelta(minutes=5),
                    market_lead_time=timedelta(minutes=5),
                    negotiation_lead_time=timedelta(minutes=5),
                    default_price=default_price,
                    future_horizon=future_horizon,
                    prior_market_in_series=prior_market_in_series,
                    commitment=False,
                    initial_market_state=MarketState.Inactive,
                    interval_duration=real_time_market_duration,
                    intervals_to_clear=1,
                    market_order=2,
                    method='Interpolation',
                    market_clearing_time=interval_start - timedelta(minutes=5),
                )

                new_market.priceModel = price_model
                # TODO: This intervalToBeRefined does not seem to be used anywhere....
                new_market.intervalToBeRefined = [x for x in market.timeIntervals
                                                  if x.startTime == market_interval_start_times[i]]

                # Pass the flag for the newest market in the market series. This important flag will be needed
                # to find this new market when the succeeding one is being instantiated and configured.
                new_market.isNewestMarket = True  # This new market now assumes the flag as newest market
                new_market.marketState = new_market.initialMarketState

                # Initialize the new market object's time intervals.
                new_market.check_intervals()

                # Initialize the marginal prices in the new market object's time intervals.
                new_market.check_marginal_prices(this_transactive_node)

                # Append the new market object to the list of active market objects that is maintained by the agent.
                this_transactive_node.markets.append(new_market)

                # Calculate the next interval's start time.
                interval_start = interval_start + new_market.intervalDuration

                # 210127DJH: Add the newly created market to a list.
                new_market_list.append(new_market)

        # 210127DJH: Capture the new markets to a formatted csv datafile.
        append_table(obj=new_market_list)
