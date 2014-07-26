# Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import kafka.client
import kafka.consumer
import kafka.producer
import logging

from mon_anomaly.processors import BaseProcessor

from nupic.data.inference_shifter import InferenceShifter
from nupic.frameworks.opf.modelfactory import ModelFactory
#from nupic.algorithms import anomaly_likelihood
import model_params
import simplejson

log = logging.getLogger(__name__)


class AnomalyProcessor(BaseProcessor):
    """
    """
    def __init__(self, kafka_url, group, topic):
        """
        Init
        kafka_url, group, topic - kafka connection details
        """
        self.topic = topic
        self.kafka = kafka.client.KafkaClient(kafka_url)
        self.consumer = kafka.consumer.SimpleConsumer(self.kafka, group, topic, auto_commit=True)
        self.consumer.seek(0, 2)
        self.consumer.provide_partition_info()  # Without this the partition is not provided in the response
        self.models = {}
        self.shifters = {}

        self.producer = kafka.producer.SimpleProducer(self.kafka,
                                              async=False,
                                              req_acks=kafka.producer.SimpleProducer.ACK_AFTER_LOCAL_WRITE,
                                              ack_timeout=2000)

    def run(self):
        """
        Consume from kafka, evaluate anomaly likelihood and anomaly score, publish to kafka.
        """
        for message in self.consumer:
            log.debug("Consuming metrics from kafka, partition %d, offset %d" % (message[0], message[1].offset))

            metric = message[1].message
            str_value = metric.value
            value = simplejson.loads(str_value)
            name = value['metric']['name']

            if '.prediction' in name or '.anomaly_score' in name:
                continue

            dimensions = value['metric']['dimensions']
            dimensions_str = simplejson.dumps(dimensions)
            metric_id = name + dimensions_str

            if metric_id not in self.models:
                self.models[metric_id] = ModelFactory.create(model_params.MODEL_PARAMS)
                self.models[metric_id].enableInference({'predictedField': 'value'})
                self.shifters[metric_id] = InferenceShifter()

            model = self.models[metric_id]
            shifter = self.shifters[metric_id]

            modelInput = {
                'timestamp': value['metric']['timestamp'],
                'value': value['metric']['value']
            }

            result = shifter.shift(model.run(modelInput))
            inferences = result.inferences
            inference = inferences['multiStepBestPredictions'][5]

            if inference is not None:
                value['metric']['name'] = name + '.prediction'
                value['metric']['value'] = inference
                str_value = simplejson.dumps(value)
                self.producer.send_messages(self.topic, str_value)

            if 'anomalyScore' in inferences:
                value['metric']['name'] = name + '.anomaly_score'
                value['metric']['value'] = inferences['anomalyScore']
                str_value = simplejson.dumps(value)
                self.producer.send_messages(self.topic, str_value)
