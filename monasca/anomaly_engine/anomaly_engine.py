#!/usr/bin/env python
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

"""
Consumes metrics from Kafka, evaluates the predicted, anomaly likelihood and anomaly score, and publishes
them as metrics to Kafka.

TODO: Read metrics from the metrics database when starting to process a new metric to reduce the amount of time that
it takes to prime the pipeline.
TODO: Check-point the nupic objects in MySQL to conserve memory.
TODO: Add support for specifying the dimensions to filter on.
TODO: Create an anomaly API
TODO: Add support for multiple consumers. Metrics need to be consistently routed to the same consumer to scale-up.
TODO: Resolve the service start-up issue.
TODO: Manually ACK messages. Currently, messages are auto ACK'd
"""

import multiprocessing
import os
import signal
import sys
import time

from oslo_config import cfg
from openstack.common import log
from openstack.common import service as os_service

from processors.nupic_anomaly_processor import NupicAnomalyProcessor
from processors.ks_anomaly_processor import KsAnomalyProcessor
from processors.rde_anomaly_processor import RDEAnomalyProcessor
import service

LOG = log.getLogger(__name__)

kafka_opts = [
    cfg.StrOpt('url'),
    cfg.StrOpt('metrics_topic')
]

kafka_group = cfg.OptGroup(name='kafka',
                           title='kafka')

cfg.CONF.register_group(kafka_group)
cfg.CONF.register_opts(kafka_opts, kafka_group)

mysql_opts = [
    cfg.StrOpt('db'),
    cfg.StrOpt('host'),
    cfg.StrOpt('user'),
    cfg.StrOpt('passwd')
]

mysql_group = cfg.OptGroup(name='mysql', title='mysql')
cfg.CONF.register_group(mysql_group)
cfg.CONF.register_opts(mysql_opts, mysql_group)

nupic_opts = [
    cfg.IntOpt('num_processors', default=1),
    cfg.StrOpt('model_params'),
    cfg.StrOpt('kafka_group'),
]

nupic_group = cfg.OptGroup(name='nupic', title='nupic')
cfg.CONF.register_group(nupic_group)
cfg.CONF.register_opts(nupic_opts, nupic_group)

ks_opts = [
    cfg.IntOpt('num_processors', default=1),
    cfg.StrOpt('kafka_group'),
    cfg.IntOpt('reference_duration', default=3600),
    cfg.IntOpt('probe_duration', default=600),
    cfg.FloatOpt('ks_d', default=0.5),
    cfg.IntOpt('min_samples', default=15)
]

ks_group = cfg.OptGroup(name='ks', title='ks')
cfg.CONF.register_group(ks_group)
cfg.CONF.register_opts(ks_opts, ks_group)

rde_opts = [
   cfg.ListOpt('instances')
]

rde_group = cfg.OptGroup(name='rde', title='rde')
cfg.CONF.register_group(rde_group)
cfg.CONF.register_opts(rde_opts, rde_group)

LOG = log.getLogger(__name__)
processors = []  # global list to facilitate clean signal handling
exiting = False

def clean_exit(signum, frame=None):
    """
    Exit all processes attempting to finish uncommited active work before exit.
    Can be called on an os signal or no zookeeper losing connection.
    """
    global exiting
    if exiting:
        # Since this is set up as a handler for SIGCHLD when this kills one child it gets another signal, the global
        # exiting avoids this running multiple times.
        LOG.debug('Exit in progress clean_exit received additional signal %s' % signum)
        return

    LOG.info('Received signal %s, beginning graceful shutdown.' % signum)
    exiting = True

    for process in processors:
        try:
            if process.is_alive():
                process.terminate()
        except Exception:
            pass

    # Kill everything, that didn't already die
    for child in multiprocessing.active_children():
        LOG.debug('Killing pid %s' % child.pid)
        try:
            os.kill(child.pid, signal.SIGKILL)
        except Exception:
            pass

    sys.exit(0)

def main(argv=['--config-file','/etc/monasca/anomaly-engine.yaml']):
    log_levels = (cfg.CONF.default_log_levels)
    cfg.set_defaults(log.log_opts, default_log_levels=log_levels)
    cfg.CONF(['--config-file', '/etc/monasca/anomaly-engine.yaml'], project='monasca-anomaly')
    log.setup('monasca-anomaly')

    for instance in cfg.CONF.rde.instances:
	#get instance config
	instance_opts = [
            cfg.StrOpt('kafka_group'),
	    cfg.BoolOpt('normalized'),
	    cfg.BoolOpt('ad3'),
	    cfg.FloatOpt('anom_threshold'),
	    cfg.FloatOpt('normal_threshold'),
	    cfg.IntOpt('fault_ittr'),
	    cfg.IntOpt('normal_ittr'),
            cfg.StrOpt('sample_name'),
            cfg.ListOpt('dimension_match'),
            cfg.ListOpt('sample_metrics')
    	]
	instance_group = cfg.OptGroup(name=instance, title=instance)
    	cfg.CONF.register_group(instance_group)
    	cfg.CONF.register_opts(instance_opts, instance_group)
	
	#start and add to processors
	rde_anomaly_processor = multiprocessing.Process(target=RDEAnomalyProcessor(instance
).run)
	processors.append(rde_anomaly_processor)

    #nupic_anomaly_processor = multiprocessing.Process(target=NupicAnomalyProcessor().run)
    #processors.append(nupic_anomaly_processor)

    #ks_anomaly_processor = multiprocessing.Process(target=KsAnomalyProcessor().run)
    #processors.append(ks_anomaly_processor)

    try:
        LOG.info('Starting processes')
        for process in processors:
            process.start()

        # The signal handlers must be added after the processes start otherwise they run on all processes
        signal.signal(signal.SIGCHLD, clean_exit)
        signal.signal(signal.SIGINT, clean_exit)
        signal.signal(signal.SIGTERM, clean_exit)

        while True:
            time.sleep(5)

    except Exception:
        LOG.exception('Error! Exiting.')
        for process in processors:
            process.terminate()

class AnomalyEngine(os_service.Service):
    def __init__(self, threads=1):
        super(AnomalyEngine, self).__init__(threads)

    def start(self):
        main(sys.argv)


def mainService():
    service.prepare_service()
    launcher = os_service.ServiceLauncher()
    launcher.launch_service(AnomalyEngine())
    launcher.wait()

if __name__ == "__main__":
    sys.exit(main(sys.argv))
