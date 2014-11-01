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
Anomaly Engine
Consumes metrics from Kafka, evaluates the anomaly likelihood and anomaly score, and publishes them as metrics to Kafka.
"""

import logging
import logging.config
import multiprocessing
import os
import signal
import sys
import threading
import time
import yaml

from processors.nupic_anomaly_processor import NupicAnomalyProcessor
from processors.ks_anomaly_processor import KsAnomalyProcessor

log = logging.getLogger(__name__)
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
        log.debug('Exit in progress clean_exit received additional signal %s' % signum)
        return

    log.info('Received signal %s, beginning graceful shutdown.' % signum)
    exiting = True

    for process in processors:
        try:
            if process.is_alive():
                process.terminate()
        except Exception:
            pass

    # Kill everything, that didn't already die
    for child in multiprocessing.active_children():
        log.debug('Killing pid %s' % child.pid)
        try:
            os.kill(child.pid, signal.SIGKILL)
        except Exception:
            pass

    sys.exit(0)


def main(argv=None):
    if argv is None:
        argv = sys.argv
    if len(argv) == 2:
        config_file = argv[1]
    elif len(argv) > 2:
        print("Usage: " + argv[0] + " <config_file>")
        print("Config file defaults to /etc/mon/anomaly.yaml")
        return 1
    else:
        config_file = '/etc/mon/anomaly.yaml'

    config = yaml.load(open(config_file, 'r'))

    # Setup logging
    logging.config.dictConfig(config['logging'])

    # AnomalyProcessor
    nupic_anomaly_processor = multiprocessing.Process(
        target=NupicAnomalyProcessor(
            config['kafka']['url'],
            config['kafka']['group'] + '.nupic',
            config['kafka']['metrics_topic']
        ).run
    )

    processors.append(nupic_anomaly_processor)

    ks_anomaly_processor = multiprocessing.Process(
        target=KsAnomalyProcessor(
            config['kafka']['url'],
            config['kafka']['group'] + '.ks',
            config['kafka']['metrics_topic']
        ).run
    )

    processors.append(ks_anomaly_processor)

    ## Start
    try:
        log.info('Starting processes')
        for process in processors:
            process.start()

        # The signal handlers must be added after the processes start otherwise they run on all processes
        signal.signal(signal.SIGCHLD, clean_exit)
        signal.signal(signal.SIGINT, clean_exit)
        signal.signal(signal.SIGTERM, clean_exit)

        while True:
            time.sleep(5)

    except Exception:
        log.exception('Error! Exiting.')
        for process in processors:
            process.terminate()

if __name__ == "__main__":
    sys.exit(main())