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


class TransactiveNode(object):
    """
    TransactiveNode is the local perspective of the computational agent among a network of such nodes.
    """

    def __init__(self,
                 description='',
                 mechanism='consensus',
                 name='',
                 status='unknown',
                 information_service_models=None,
                 local_assets=None,
                 markets=None,
                 meter_points=None,
                 neighbors=None,
                 ):

        super(TransactiveNode, self).__init__()

        self.description = description                          # [text]
        self.mechanism = mechanism                              # future, unused in Version 2
        self.name = name                                        # [text]
        self.status = status                                    # future: will be enumeration

        # The agent must keep track of various devices and their models that are listed among these properties.
        self.informationServiceModels = information_service_models if information_service_models else []
        self.localAssets = local_assets if local_assets else []
        self.markets = markets if markets else []
        self.meterPoints = meter_points if meter_points else []
        self.neighbors = neighbors if neighbors else []

    def get_meter_points_by_name(self, meter_points):
        meter_point_list = []
        available_mps = [mp.name for mp in self.meterPoints]
        for mp in meter_points:
            if mp not in available_mps:
                raise ValueError(f'Meter point {mp} is not available.')
            else:
                meter_point_list.append(self.meterPoints[available_mps.index(mp)])
        return meter_point_list

    def get_information_services_by_name(self, information_services):
        ism_list = []
        available_isms = [ism.name for ism in self.informationServiceModels]
        for ism in information_services:
            if ism not in available_isms:
                raise ValueError(f'Information Service {ism} is not available.')
            else:
                ism_list.append(self.informationServiceModels[available_isms.index(ism)])
        return ism_list

    def get_market_by_name(self, market_name):
        matches = [m for m in self.markets if m.name == market_name]
        return matches[0] if len(matches) > 0 else None

