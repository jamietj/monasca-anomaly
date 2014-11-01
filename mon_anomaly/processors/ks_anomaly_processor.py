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
import time
from mon_anomaly.processors import BaseProcessor
import simplejson
import scipy
import statsmodels.api as sm
import collections

log = logging.getLogger(__name__)

class KsAnomalyProcessor(BaseProcessor):
    """
    """
    def __init__(self, kafka_url, group, topic):
        """
        Init kafka_url, group, topic - kafka connection details
        """
        self.topic = topic
        self.kafka = kafka.client.KafkaClient(kafka_url)
        self.consumer = kafka.consumer.SimpleConsumer(self.kafka, group, topic, auto_commit=True)
        self.consumer.seek(0, 2)
        self.consumer.provide_partition_info()  # Without this the partition is not provided in the response
        self.timeseries = {}
        self.producer = kafka.producer.SimpleProducer(self.kafka,
                                              async=False,
                                              req_acks=kafka.producer.SimpleProducer.ACK_AFTER_LOCAL_WRITE,
                                              ack_timeout=2000)

    def run(self):
        """
        Consume from kafka, evaluate KS score and publish result to metrics topic.
        """
        for message in self.consumer:
            log.debug("Consuming metrics from kafka, partition %d, offset %d" % (message[0], message[1].offset))

            metric = message[1].message
            str_value = metric.value
            value = simplejson.loads(str_value)
            name = value['metric']['name']

            if 'cpu.user_perc' not in name:
                continue

            if '.predicted' in name or '.anomaly_score' in name or '.anomaly_likelihood' in name:
                continue

            dimensions = value['metric']['dimensions']
            dimensions_str = simplejson.dumps(dimensions)
            metric_id = name + dimensions_str

            if metric_id not in self.timeseries:
                self.timeseries[metric_id] = collections.deque(maxlen=210)
                print len(self.timeseries)

            #'dttm': value['metric']['timestamp'],
            #'dttm': datetime.datetime.now(),
            #'value': value['metric']['value']

            time_series = self.timeseries[metric_id]
            time_series.append([value['metric']['timestamp'], value['metric']['value']])
            result = self.ks_test(time_series)

            value['metric']['name'] = name + '.ks.anomaly_score'
            value['metric']['value'] = result
            str_value = simplejson.dumps(value)
            self.producer.send_messages(self.topic, str_value)

    def ks_test(self, timeseries):
        """
        A timeseries is anomalous if 2 sample Kolmogorov-Smirnov test indicates
        that data distribution for last 10 minutes is different from last hour.
        It produces false positives on non-stationary series so Augmented
        Dickey-Fuller test applied to check for stationarity.
        """
        now = int(time.time())
        hour_ago = now - 3600
        ten_minutes_ago = now - 600

        reference = scipy.array([x[1] for x in timeseries if x[0] >= hour_ago and x[0] < ten_minutes_ago])
        probe = scipy.array([x[1] for x in timeseries if x[0] >= ten_minutes_ago])

        if reference.size < 15 or probe.size < 15:
            return 0.0

        ks_d, ks_p_value = scipy.stats.ks_2samp(reference, probe)

        if ks_p_value < 0.05 and ks_d > 0.5:
            adf = sm.tsa.stattools.adfuller(reference, 10)
            if adf[1] < 0.05:
                return 1.0

        return 0.0


