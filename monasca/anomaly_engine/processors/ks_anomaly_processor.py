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

import time
import collections

import simplejson
import scipy
import statsmodels.api as sm
from oslo.config import cfg
from openstack.common import log
from anomaly_processor import AnomalyProcessor

LOG = log.getLogger(__name__)


class KsAnomalyProcessor(AnomalyProcessor):
    """
    Kolmogorov-Smirnov two sample test
    """

    def __init__(self):
        AnomalyProcessor.__init__(self, cfg.CONF.ks.kafka_group)
        ks_config = cfg.CONF.ks
        self._reference_duration = ks_config.reference_duration
        self._probe_duration = ks_config.probe_duration
        self._ks_d = ks_config.ks_d
        self._min_samples = ks_config.min_samples
        self._timeseries = {}

    def _send_predictions(self, metric_id, metric_envelope):
        metric = metric_envelope['metric']

        if metric_id not in self._timeseries:
            self._timeseries[metric_id] = collections.deque(maxlen=256)

        time_series = self._timeseries[metric_id]
        time_series.append((metric['timestamp'], metric['value']))
        result = self._ks_test(time_series)

        metric_name = metric['name']

        metric['name'] = metric_name + '.ks.anomaly_score'
        metric['value'] = result
        str_value = simplejson.dumps(metric_envelope)

        self._producer.send_messages(self._topic, str_value)

    def _ks_test(self, timeseries):
        """
        A timeseries is anomalous if 2 sample Kolmogorov-Smirnov test indicates
        that data distribution for last 10 minutes is different from last hour.
        It produces false positives on non-stationary series so Augmented
        Dickey-Fuller test applied to check for stationarity.
        """
        now = int(time.time())
        reference_start_time = now - self._reference_duration
        probe_start_time = now - self._probe_duration

        reference = scipy.array([x[1] for x in timeseries if x[0] >= reference_start_time and
                                 x[0] < probe_start_time])
        probe = scipy.array([x[1] for x in timeseries if x[0] >= probe_start_time])

        if reference.size < self._min_samples or probe.size < self._min_samples:
            return 0.0

        ks_d, ks_p_value = scipy.stats.ks_2samp(reference, probe)

        if ks_p_value < 0.05 and ks_d > self._ks_d:
            results = sm.tsa.stattools.adfuller(reference)
            pvalue = results[1]
            if pvalue < 0.05:
                return 1.0

        return 0.0