# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from time import sleep

from airflow.exceptions import AirflowException, AirflowSensorTimeout, \
            AirflowSkipException
from airflow.models import BaseOperator, SkipMixin

import logging
import json
from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException
from airflow.utils import timezone
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.contrib.sensors.emr_base_sensor import EmrBaseSensor


class EmrCreateJobFlowOperatorAndSensor(BaseOperator, SkipMixin):
    """
    Creates an EMR JobFlow, reading the config from the EMR connection.
    A dictionary of JobFlow overrides can be passed that override the config from the connection.

    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    :param emr_conn_id: emr connection to use
    :type emr_conn_id: str
    :param job_flow_overrides: boto3 style arguments to override emr_connection extra
    :type steps: dict
    """
    template_ext = ()
    ui_color = '#f9c915'
    NON_TERMINAL_STATES = ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING', 'TERMINATING']
    FAILED_STATE = 'TERMINATED_WITH_ERRORS'
    #template_fields = ['job_flow_id']


    @apply_defaults
    def __init__(
            self,
            aws_conn_id='s3_default',
            emr_conn_id='emr_default',
            soft_fail=False,
	    poke_interval=60,
            timeout=60 * 60 * 24 * 7,
            job_flow_overrides=None,
            *args, **kwargs):
        super(EmrCreateJobFlowOperatorAndSensor, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.emr_conn_id = emr_conn_id
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.soft_fail=soft_fail,
        if job_flow_overrides is None:
            job_flow_overrides = {}
        self.job_flow_overrides = job_flow_overrides

    
    def get_emr_response(self):
        emr = EmrHook(aws_conn_id=self.aws_conn_id).get_conn()

        self.log.info('Poking cluster %s', self.job_flow_id)
        return emr.describe_cluster(ClusterId=self.job_flow_id)
 

    def state_from_response(self, response):
        return response['Cluster']['Status']['State'] 
      
    def poke(self, context):
        response = self.get_emr_response()

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logging.info('Bad HTTP response: %s' % response)
            return False

        state = self.state_from_response(response)
        logging.info('Job flow currently %s' % state)

        if state in self.NON_TERMINAL_STATES:
            return False

        if state == self.FAILED_STATE:
            raise AirflowException('EMR job failed')

        return True
  

    def execute(self, context):
        emr = EmrHook(aws_conn_id=self.aws_conn_id, emr_conn_id=self.emr_conn_id)

        logging.info('Creating JobFlow')
        response = emr.create_job_flow(self.job_flow_overrides)
        self.job_flow_id=response['JobFlowId']

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException('JobFlow creation failed: %s' % response)
        else:
            logging.info('Poking cluster %s' % response['JobFlowId'])
            started_at = timezone.utcnow()
            while not self.poke(context):
                if (timezone.utcnow() - started_at).total_seconds() > self.timeout:
                # If sensor is in soft fail mode but will be retried then
                # give it a chance and fail with timeout.
                # This gives the ability to set up non-blocking AND soft-fail sensors.
                    if self.soft_fail and not context['ti'].is_eligible_to_retry():
                        self._do_skip_downstream_tasks(context)
                        raise AirflowSkipException('Snap. Time is OUT.')
                    else:
                        raise AirflowSensorTimeout('Snap. Time is OUT.')
                sleep(self.poke_interval)
            self.log.info("Success criteria met. Exiting.")
 
       
    def _do_skip_downstream_tasks(self, context):
        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task_ids %s", downstream_tasks)
        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)
 
