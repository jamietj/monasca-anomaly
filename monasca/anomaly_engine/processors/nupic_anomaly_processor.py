# Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import json
import simplejson
from nupic.data.inference_shifter import InferenceShifter
from nupic.frameworks.opf.modelfactory import ModelFactory
from nupic.algorithms.anomaly_likelihood import AnomalyLikelihood
from oslo_config import cfg
from openstack.common import log
from anomaly_processor import AnomalyProcessor


LOG = log.getLogger(__name__)


class NupicAnomalyProcessor(AnomalyProcessor):
    """
    NuPIC processor
    """

    def __init__(self):
        AnomalyProcessor.__init__(self, cfg.CONF.nupic.kafka_group)
        self._models = {}
        self._shifters = {}
        self._anomaly_likelihood = {}


        # Load the model params JSON
        with open(cfg.CONF.nupic.model_params) as fp:
            self.model_params = json.load(fp)

            # Update the min/max value for the encoder
            # sensor_params = self.model_params['modelParams']['sensorParams']
            # sensor_params['encoders']['value']['maxval'] = options.max
            # sensor_params['encoders']['value']['minval'] = options.min

    def _send_predictions(self, metric_id, metric_envelope):
        if metric_id not in self._models:
            self._models[metric_id] = ModelFactory.create(self.model_params)
            self._models[metric_id].enableInference({'predictedField': 'value'})
            self._shifters[metric_id] = InferenceShifter()
            self._anomaly_likelihood[metric_id] = AnomalyLikelihood()

        model = self._models[metric_id]
        shifter = self._shifters[metric_id]

        modelInput = {
            # 'dttm': value['metric']['timestamp'],
            'dttm': datetime.datetime.now(),
            'value': metric_envelope['metric']['value']
        }

        result = shifter.shift(model.run(modelInput))
        inferences = result.inferences
        inference = inferences['multiStepBestPredictions'][5]

        metric = metric_envelope['metric']
        metric_name = metric['name']

        if inference is not None:
            metric['name'] = metric_name + '.nupic.predicted'
            metric['value'] = inference
            str_value = simplejson.dumps(metric_envelope)
            self._producer.send_messages(self._topic, str_value)

        if 'anomalyScore' in inferences:
            metric['name'] = metric_name + '.nupic.anomaly_score'
            metric['value'] = inferences['anomalyScore']
            str_value = simplejson.dumps(metric_envelope)
            self._producer.send_messages(self._topic, str_value)

            anomalyLikelihood = self._anomaly_likelihood[metric_id]
            likelihood = anomalyLikelihood.anomalyProbability(
                modelInput['value'], inferences['anomalyScore'], datetime.datetime.now()
            )

            metric['name'] = metric_name + '.nupic.anomaly_likelihood'
            metric['value'] = likelihood
            str_value = simplejson.dumps(metric_envelope)
            self._producer.send_messages(self._topic, str_value)
