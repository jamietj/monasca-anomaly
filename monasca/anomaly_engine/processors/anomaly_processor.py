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

import kafka.client
import kafka.consumer
import kafka.producer
from oslo.config import cfg
from openstack.common import log
import simplejson
import urllib

LOG = log.getLogger(__name__)
METRIC_NAME_SUFFIXES = ['.predicted', '.anomaly_score', '.anomaly_likelihood']


class AnomalyProcessor(object):
    '''
    Base class for anomaly processors
    '''

    def __init__(self, group):
        self._topic = cfg.CONF.kafka.metrics_topic
        self._metric_names = cfg.CONF.metrics.names
        self._kafka = kafka.client.KafkaClient(cfg.CONF.kafka.url)
        self._consumer = kafka.consumer.SimpleConsumer(self._kafka, group, self._topic, auto_commit=True)
        self._consumer.seek(0, 2)
        self._consumer.provide_partition_info()  # Without this the partition is not provided in the response
        self._producer = kafka.producer.SimpleProducer(self._kafka,
                                                       async=False,
                                                       req_acks=kafka.producer.SimpleProducer.ACK_AFTER_LOCAL_WRITE,
                                                       ack_timeout=2000)

    def _send_predictions(self, metric_id, metric_envelope):
        '''
        Template method for evaluating and sending predictions
        '''
        raise NotImplemented()

    def run(self):
        """
        Consume from kafka, evaluate anomaly likelihood and anomaly score, publish to kafka.
        """

        try:
            for message in self._consumer:
                LOG.debug("Consuming metrics from kafka, partition %d, offset %d" % (message[0], message[1].offset))

                metric_message = message[1].message
                metric_message_value = metric_message.value
                metric_envelope = simplejson.loads(metric_message_value)
                metric = metric_envelope['metric']
                metric_name = metric['name']

                if metric_name not in self._metric_names:
                    continue

                if any(suffix in metric_name for suffix in METRIC_NAME_SUFFIXES):
                    continue

                region = metric_envelope['meta']['region']
                tenant_id = metric_envelope['meta']['tenantId']
                dimensions = metric_envelope['metric']['dimensions']

                # TODO: Following code should be in monasca-common as it is also in the Python monasca-persister.
                metric_id = (
                    urllib.quote(metric_name.encode('utf8'), safe='')
                    + '?' + urllib.quote(tenant_id.encode('utf8'), safe='')
                    + '&' + urllib.quote(region.encode('utf8'), safe=''))

                for dimension_name in dimensions:
                    metric_id += ('&'
                                  + urllib.quote(
                        dimension_name.encode('utf8'), safe='')
                                  + '='
                                  + urllib.quote(
                        dimensions[dimension_name].encode('utf8'), safe=''))

                self._send_predictions(metric_id, metric_envelope)

        except Exception as ex:
            LOG.exception(ex)