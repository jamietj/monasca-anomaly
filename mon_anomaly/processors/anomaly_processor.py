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
    def __init__(self, finished_queue, kafka_url, group, topic, initial_offsets=None):
        """
        Init
        kafka_url, group, topic - kafka connection details
        """
        self.topic = topic
        self.finished_queue = finished_queue
        self.kafka = kafka.client.KafkaClient(kafka_url)

        # No autocommit, it does not work with kafka 0.8.0 - see https://github.com/mumrah/kafka-python/issues/118
        self.consumer = kafka.consumer.SimpleConsumer(self.kafka, group, topic, auto_commit=False)
        self.consumer.provide_partition_info()  # Without this the partition is not provided in the response

        if initial_offsets is not None:
            # Set initial offsets directly in the consumer, there is no method for this so I have to do it here
            self.consumer.offsets.update(initial_offsets)
            # fetch offsets are +1 of standard offsets
            for partition in initial_offsets:
                self.consumer.fetch_offsets[partition] = initial_offsets[partition] + 1

        self.model = ModelFactory.create(model_params.MODEL_PARAMS)
        self.model.enableInference({'predictedField': 'cpu'})
        self.shifter = InferenceShifter()

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

            # TODO: Add evaluation here.
            metric = message[1].message
            str_value = metric.value
            value = simplejson.loads(str_value)
            name = value['metric']['name']

            if name == 'cpu_user_perc':
                modelInput = {
                    'timestamp': value['metric']['timestamp'],
                    'cpu': value['metric']['value']
                }
                result = self.shifter.shift(self.model.run(modelInput))

                inferences = result.inferences
                inference = inferences['multiStepBestPredictions'][5]
                if inference is not None:
                    value['metric']['name'] = name + '.prediction'
                    value['metric']['value'] = inference
                    str_value = simplejson.dumps(value)
                    self.producer.send_messages(self.topic, str_value)

                if 'anomalyScore' in inferences:
                    value['metric']['name'] = name + '.anomaly'
                    value['metric']['value'] = inferences['anomalyScore']
                    str_value = simplejson.dumps(value)
                    self.producer.send_messages(self.topic, str_value)


            self._add_to_queue(self.finished_queue, 'finished', (message[0], message[1].offset))


