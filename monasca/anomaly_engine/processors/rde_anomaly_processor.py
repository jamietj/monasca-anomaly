import sys
import numpy
import math
import simplejson
import random

from oslo.config import cfg
from monasca.openstack.common import log
from anomaly_processor import AnomalyProcessor

LOG = log.getLogger(__name__)

class AD3AnomalyProcessor(AnomalyProcessor):

	def __init__(self):
		AnomalyProcessor.__init__(self, cfg.CONF.ad3.kafka_group)

		self.processorID = random.randrange(1, 100)

		self._anom_vaules = {}

		#params
		self.anom_threshold 	= 0.7	
		self.fault_threshold 	= 1
		self.normal_threshold 	= 3

		#flags
		self.fault_flag		= 0
		self.normal_flag	= 0  
	
		self.k 	= float(1)
		self.ks = float(1)

		# operation status
		# 0 - normal
		# 1 - fault 
		self.status	= 0

		#density collections
		self.density 	= {}
		self.anom	= {}

	def _send_predictions(self, metric_id, metric_envelope):
		#get metric
		metric = metric_envelope['metric']
		value = metric['value']

		if metric_id not in self._anom_values:
			self._anom_values[metric_id] = {
				'mean': value, 
				'density': 1.0,
				'mean_density': 1.0, 
				'scalar': numpy.linalg.norm(numpy.array(value))**2,
				'k': 2,
				'ks': 2,
				'status': 0,
				'normal_falg': 0,
				'fault_flag': 0
			}
		else:
			anom_values = _anom_values[metric_id]
			
			anom_values['mean'] 		= (((anom_values['k']-1)/anom_values['k'])*anom_values['mean'])+((1/anom_values['k'])*value)	 
			anom_values['scalar'] 		= (((anom_values['k']-1)/anom_values['k'])*anom_values['scalar'])+((1/anom_values['k'])*numpy.linalg.norm(numpy.array(value))**2)
			anom_values['p_density']	= anom_values['density']
			anom_values['density']		= 1/(1 + ((numpy.linalg.norm(numpy.array(value - anom_values['mean'])))**2) + anom_values['scalar'] - (numpy.linalg.norm(numpy.array(anom_values['mean']))**2))
			diff				= abs(anom_values['density'] - anom_values['p_density'])
			anom_values['mean_density']	= ((((anom_values['ks']-1)/anom_values['ks'])*anom_values['mean_density'])+((1/anom_values['ks'])*anom_values['density']))*(1-diff)+(anom_values['density']*diff)
	
			#anomaly detection
			if anom_values['status'] == 0:
				if anom_values['density'] < anom_values['mean_density'] * self.anom_threshold:
					anom_values['fault_flag'] += 1
					if anom_values['fault_flag'] >= self.fault_threshold:
						anom_values['status'] 		= 1
						anom_values['ks'] 		= 0
						anom_values['fault_flag'] 	= 0
			else:
				if anom_vlaues['density'] >= anom_values['mean_density']:
					anom_values['normal_flag'] = += 1;
					if anom_values['normal_flag'] >= self.normal_threshold:
						anom_values['status'] 		= 0
						anom_values['ks'] 		= 0
						anom_values['normal_flag']	= 0

			anom_values['ks'] += 1
			anom_values['k'] += 1

			#publish prediction - operation status - either 0 or 1 for now 
			metric_name = metric['name']

	        	metric['name'] = metric_name + '.rde.anomaly_score'
	        	metric['value'] = anom_values['status']
	        	str_value = simplejson.dumps(metric_envelope)

	        	self._producer.send_messages(self._topic, str_value)

	
