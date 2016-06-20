# -*- coding: utf-8 -*- {{{
# vim: set fenc=utf-8 ft=python sw=4 ts=4 sts=4 et:
#
# Copyright (c) 2015, Battelle Memorial Institute
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are those
# of the authors and should not be interpreted as representing official policies,
# either expressed or implied, of the FreeBSD Project.
#

# This material was prepared as an account of work sponsored by an
# agency of the United States Government.  Neither the United States
# Government nor the United States Department of Energy, nor Battelle,
# nor any of their employees, nor any jurisdiction or organization
# that has cooperated in the development of these materials, makes
# any warranty, express or implied, or assumes any legal liability
# or responsibility for the accuracy, completeness, or usefulness or
# any information, apparatus, product, software, or process disclosed,
# or represents that its use would not infringe privately owned rights.
#
# Reference herein to any specific commercial product, process, or
# service by trade name, trademark, manufacturer, or otherwise does
# not necessarily constitute or imply its endorsement, recommendation,
# r favoring by the United States Government or any agency thereof,
# or Battelle Memorial Institute. The views and opinions of authors
# expressed herein do not necessarily state or reflect those of the
# United States Government or any agency thereof.
#
# PACIFIC NORTHWEST NATIONAL LABORATORY
# operated by BATTELLE for the UNITED STATES DEPARTMENT OF ENERGY
# under Contract DE-AC05-76RL01830

#}}}

import logging
import sys
import os
import gevent
from volttron.platform.vip.agent import Agent, Core, RPC, Unreachable, compat
from volttron.platform.agent import utils
from volttron.platform.agent import math_utils
from volttron.platform.messaging import topics
from driver import DriverAgent
import resource
from datetime import datetime, timedelta
from volttron.platform.messaging.utils import normtopic

from master_driver.scheduler import ScheduleManager
from volttron.platform.jsonrpc import RemoteError

from driver_locks import configure_socket_lock, configure_publish_lock

VALUE_RESPONSE_PREFIX = topics.ACTUATOR_VALUE()
REVERT_POINT_RESPONSE_PREFIX = topics.ACTUATOR_REVERTED_POINT()
REVERT_DEVICE_RESPONSE_PREFIX = topics.ACTUATOR_REVERTED_DEVICE()
ERROR_RESPONSE_PREFIX = topics.ACTUATOR_ERROR()

WRITE_ATTEMPT_PREFIX = topics.ACTUATOR_WRITE()

SCHEDULE_ACTION_NEW = 'NEW_SCHEDULE'
SCHEDULE_ACTION_CANCEL = 'CANCEL_SCHEDULE'

SCHEDULE_RESPONSE_SUCCESS = 'SUCCESS'
SCHEDULE_RESPONSE_FAILURE = 'FAILURE'

SCHEDULE_CANCEL_PREEMPTED = 'PREEMPTED'

utils.setup_logging()
_log = logging.getLogger(__name__)
__version__ = '0.1'


class LockError(StandardError):
    """Error raised when the user does not have a device scheuled
    and tries to use methods that require exclusive access."""
    pass


def master_driver_agent(config_path, **kwargs):

    config = utils.load_config(config_path)

    max_open_sockets = config.get('max_open_sockets', None)
    # Increase open files resource limit to max or 8192 if unlimited
    limit = None

    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    except OSError:
        _log.exception('error getting open file limits')
    else:
        if soft != hard and soft != resource.RLIM_INFINITY:
            try:
                limit = 8192 if hard == resource.RLIM_INFINITY else hard
                resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))
            except OSError:
                _log.exception('error setting open file limits')
            else:
                _log.debug('open file resource limit increased from %d to %d',
                           soft, limit)
        if soft == hard:
            limit = soft

    if max_open_sockets is not None:
        configure_socket_lock(max_open_sockets)
        _log.info("maximum concurrently open sockets limited to " + str(max_open_sockets))
    elif limit is not None:
        max_open_sockets = int(limit*0.8)
        _log.info("maximum concurrently open sockets limited to " + str(max_open_sockets) +
                  " (derived from system limits)")
        configure_socket_lock(max_open_sockets)
    else:
        configure_socket_lock()
        _log.warn("No limit set on the maximum number of concurrently open sockets. "
                  "Consider setting max_open_sockets if you plan to work with 800+ modbus devices.")

    #TODO: update the default after scalability testing.
    max_concurrent_publishes = config.get('max_concurrent_publishes', 10000)
    if max_concurrent_publishes < 1:
        _log.warn("No limit set on the maximum number of concurrent driver publishes. "
                  "Consider setting max_concurrent_publishes if you plan to work with many devices.")
    else:
        _log.info("maximum concurrent driver publishes limited to " + str(max_concurrent_publishes))
    configure_publish_lock(max_concurrent_publishes)

    identity = config.get('vip_identity', 'platform.actuator')
    kwargs['identity'] = identity

    heartbeat_toggle_interval = int(config.get('heartbeat_toggle_interval', 60))
    schedule_publish_interval = int(config.get('schedule_publish_interval', 60))
    schedule_state_file = config.get('schedule_state_file')
    preempt_grace_time = config.get('preempt_grace_time', 60)
    driver_config_list = config.get('driver_config_list')
    staggered_start = config.get('staggered_start', None)

    scalability_test = config.get('scalability_test', False)
    scalability_test_iterations = config.get('scalability_test_iterations', 3)

    return MasterDriverAgent(heartbeat_toggle_interval,
                             schedule_publish_interval,
                             schedule_state_file,
                             preempt_grace_time,
                             driver_config_list,
                             staggered_start,
                             scalability_test,
                             scalability_test_iterations,
                             heartbeat_autostart=True, **kwargs)


class MasterDriverAgent(Agent):
    def __init__(self, heartbeat_toggle_interval,
                 schedule_publish_interval,
                 schedule_state_file,
                 preempt_grace_time,
                 driver_config_list,
                 staggered_start,
                 scalability_test,
                 scalability_test_iterations,
                 **kwargs):

        super(MasterDriverAgent, self).__init__(**kwargs)
        self.instances = {}
        self._update_event = None
        self._device_states = {}

        self.heartbeat_toggle_interval = heartbeat_toggle_interval
        self.schedule_publish_interval = schedule_publish_interval
        self.schedule_state_file = schedule_state_file
        self.preempt_grace_time = preempt_grace_time
        self.driver_config_list = driver_config_list
        self.staggered_start = staggered_start

        self.scalability_test = scalability_test
        self.scalability_test_iterations = scalability_test_iterations
        if scalability_test:
            self.waiting_to_finish = set()
            self.test_iterations = 0
            self.test_results = []
            self.current_test_start = None

    @Core.receiver('onstart')
    def onstart(self, sender, **kwargs):
        start_delay = None
        if self.staggered_start is not None:
            start_delay = self.staggered_start / float(len(self.driver_config_list))
        for config_name in self.driver_config_list:
            _log.debug("Launching driver for config "+config_name)
            driver = DriverAgent(self, config_name)
            gevent.spawn(driver.core.run)
            if start_delay is not None:
                gevent.sleep(start_delay)

        self._setup_schedule()
        self.vip.pubsub.subscribe(peer='pubsub',
                                  prefix=topics.ACTUATOR_GET(),
                                  callback=self.handle_get)

        self.vip.pubsub.subscribe(peer='pubsub',
                                  prefix=topics.ACTUATOR_SET(),
                                  callback=self.handle_set)

        self.vip.pubsub.subscribe(peer='pubsub',
                                  prefix=topics.ACTUATOR_SCHEDULE_REQUEST(),
                                  callback=self.handle_schedule_request)

        self.vip.pubsub.subscribe(peer='pubsub',
                                  prefix=topics.ACTUATOR_REVERT_POINT(),
                                  callback=self.handle_revert_point)

        self.vip.pubsub.subscribe(peer='pubsub',
                                  prefix=topics.ACTUATOR_REVERT_DEVICE(),
                                  callback=self.handle_revert_device)

        self.core.periodic(self.heartbeat_toggle_interval, self._toggle_heartbeat)

    def _toggle_heartbeat(self):
        _log.debug("sending heartbeat")
        for device in self.instances.values():
            device.heart_beat()

    def _setup_schedule(self):
        now = datetime.now()
        self._schedule_manager = ScheduleManager(self.preempt_grace_time, now=now,
                                                 state_file_name=self.schedule_state_file)

        self._update_device_state_and_schedule(now)

    def device_startup_callback(self, topic, driver):
        _log.debug("Driver hooked up for "+topic)
        topic = topic.strip('/')
        self.instances[topic] = driver

    def scrape_starting(self, topic):
        if not self.scalability_test:
            return

        if not self.waiting_to_finish:
            #Start a new measurement
            self.current_test_start = datetime.now()
            self.waiting_to_finish = set(self.instances.iterkeys())

        if topic not in self.waiting_to_finish:
            _log.warning(topic + " started twice before test finished, increase the length of scrape interval and rerun test")

    def scrape_ending(self, topic):
        if not self.scalability_test:
            return

        try:
            self.waiting_to_finish.remove(topic)
        except KeyError:
            _log.warning(topic + " published twice before test finished, increase the length of scrape interval and rerun test")

        if not self.waiting_to_finish:
            end = datetime.now()
            delta = end - self.current_test_start
            delta = delta.total_seconds()
            self.test_results.append(delta)

            self.test_iterations += 1

            _log.info("publish {} took {} seconds".format(self.test_iterations, delta))

            if self.test_iterations >= self.scalability_test_iterations:
                #Test is now over. Button it up and shutdown.
                mean = math_utils.mean(self.test_results)
                stdev = math_utils.stdev(self.test_results)
                _log.info("Mean total publish time: "+str(mean))
                _log.info("Std dev publish time: "+str(stdev))
                sys.exit(0)

    def _check_lock(self, device, requester):
        _log.debug('_check_lock: {device}, {requester}'.format(device=device,
                                                              requester=requester))
        device = device.strip('/')
        if device in self._device_states:
            device_state = self._device_states[device]
            return device_state.agent_id == requester
        return False

    def _update_device_state_and_schedule(self, now):
        _log.debug("_update_device_state_and_schedule")
        # Sanity check now.
        # This is specifically for when this is running in a VM that gets suspeded and then resumed.
        # If we don't make this check a resumed VM will publish one event per minute of
        # time the VM was suspended for.
        test_now = datetime.now()
        if test_now - now > timedelta(minutes=3):
            now = test_now

        self._device_states = self._schedule_manager.get_schedule_state(now)
        schedule_next_event_time = self._schedule_manager.get_next_event_time(now)
        new_update_event_time = self._get_ajusted_next_event_time(now, schedule_next_event_time)

        for device, state in self._device_states.iteritems():
            header = self._get_headers(state.agent_id, time=utils.format_timestamp(now), task_id=state.task_id)
            header['window'] = state.time_remaining
            topic = topics.ACTUATOR_SCHEDULE_ANNOUNCE_RAW.replace('{device}', device)
            self.vip.pubsub.publish('pubsub', topic, headers=header)

        if self._update_event is not None:
            # This won't hurt anything if we are canceling ourselves.
            self._update_event.cancel()
        self._update_event = self.core.schedule(new_update_event_time,
                                                self._update_device_state_and_schedule,
                                                new_update_event_time)

    def _get_ajusted_next_event_time(self, now, next_event_time):
        _log.debug("_get_adjusted_next_event_time")
        latest_next = now + timedelta(seconds=self.schedule_publish_interval)
        # Round to the next second to fix timer goofyness in agent timers.
        if latest_next.microsecond:
            latest_next = latest_next.replace(microsecond=0) + timedelta(seconds=1)
        if next_event_time is None or latest_next < next_event_time:
            return latest_next
        return next_event_time

    def _handle_remote_error(self, ex, point, headers):
        try:
            exc_type = ex.exc_info['exc_type']
            exc_args = ex.exc_info['exc_args']
        except KeyError:
            exc_type = "RemoteError"
            exc_args = ex.message
        error = {'type': exc_type, 'value': str(exc_args)}
        self._push_result_topic_pair(ERROR_RESPONSE_PREFIX,
                                    point, headers, error)

        _log.debug('Actuator Agent Error: ' + str(error))

    def _handle_standard_error(self, ex, point, headers):
        error = {'type': ex.__class__.__name__, 'value': str(ex)}
        self._push_result_topic_pair(ERROR_RESPONSE_PREFIX,
                                    point, headers, error)
        _log.debug('Actuator Agent Error: ' + str(error))

    def _handle_unknown_schedule_error(self, ex, headers, message):
        _log.error(
            'bad request: {header}, {request}, {error}'.format(header=headers, request=message, error=str(ex)))
        results = {'result': "FAILURE",
                   'data': {},
                   'info': 'MALFORMED_REQUEST: ' + ex.__class__.__name__ + ': ' + str(ex)}
        self.vip.pubsub.publish('pubsub', topics.ACTUATOR_SCHEDULE_RESULT(), headers=headers, message=results)
        return results

    def _get_headers(self, requester, time=None, task_id=None):
        headers = {}
        if time is not None:
            headers['time'] = time
        else:
            utcnow = datetime.utcnow()
            headers = {'time': utils.format_timestamp(utcnow)}
        if requester is not None:
            headers['requesterID'] = requester
        if task_id is not None:
            headers['taskID'] = task_id
        return headers

    def _push_result_topic_pair(self, prefix, point, headers, value):
        topic = normtopic('/'.join([prefix, point]))
        self.vip.pubsub.publish('pubsub', topic, headers, message=value)

    def handle_get(self, peer, sender, bus, topic, headers, message):
        """
        Requests up to date value of a point.

        To request a value publish a message to the following topic:

        ``devices/actuators/get/<device path>/<actuation point>``

        with the fallowing header:

        .. code-block:: python

            {
                'requesterID': <Agent ID>
            }

        The ActuatorAgent will reply on the **value** topic
        for the actuator:

        ``devices/actuators/value/<full device path>/<actuation point>``

        with the message set to the value the point.

        """
        point = topic.replace(topics.ACTUATOR_GET() + '/', '', 1)
        requester = headers.get('requesterID')
        headers = self._get_headers(requester)
        try:
            value = self.get_point(point)
            self._push_result_topic_pair(VALUE_RESPONSE_PREFIX,
                                        point, headers, value)
        except RemoteError as ex:
            self._handle_remote_error(ex, point, headers)
        except (StandardError, Exception) as ex:
            # DriverInterfaceError is an exception
            self._handle_standard_error(ex, point, headers)

    def handle_set(self, peer, sender, bus, topic, headers, message):
        """
        Set the value of a point.

        To set a value publish a message to the following topic:

        ``devices/actuators/set/<device path>/<actuation point>``

        with the fallowing header:

        .. code-block:: python

            {
                'requesterID': <Agent ID>
            }

        The ActuatorAgent will reply on the **value** topic
        for the actuator:

        ``devices/actuators/value/<full device path>/<actuation point>``

        with the message set to the value the point.

        Errors will be published on

        ``devices/actuators/error/<full device path>/<actuation point>``

        with the same header as the request.

        """
        if sender == 'pubsub.compat':
            message = compat.unpack_legacy_message(headers, message)

        point = topic.replace(topics.ACTUATOR_SET() + '/', '', 1)
        requester = headers.get('requesterID')
        headers = self._get_headers(requester)
        if not message:
            error = {'type': 'ValueError', 'value': 'missing argument'}
            _log.debug('ValueError: ' + str(error))
            self._push_result_topic_pair(ERROR_RESPONSE_PREFIX,
                                        point, headers, error)
            return

        try:
            self.set_point(requester, point, message)
        except RemoteError as ex:
            self._handle_remote_error(ex, point, headers)
        except StandardError as ex:
            self._handle_standard_error(ex, point, headers)

    def handle_revert_point(self, peer, sender, bus, topic, headers, message):
        """
        Revert the value of a point.

        To revert a value publish a message to the following topic:

        ``actuators/revert/point/<device path>/<actuation point>``

        with the fallowing header:

        .. code-block:: python

            {
                'requesterID': <Agent ID>
            }

        The ActuatorAgent will reply on

        ``devices/actuators/reverted/point/<full device path>/<actuation point>``

        This is to indicate that a point was reverted.

        Errors will be published on

        ``devices/actuators/error/<full device path>/<actuation point>``

        with the same header as the request.
        """
        point = topic.replace(topics.ACTUATOR_REVERT_POINT()+'/', '', 1)
        requester = headers.get('requesterID')
        headers = self._get_headers(requester)

        try:
            self.revert_point(requester, point)
        except RemoteError as ex:
            self._handle_remote_error(ex, point, headers)
        except StandardError as ex:
            self._handle_standard_error(ex, point, headers)

    def handle_revert_device(self, peer, sender, bus, topic, headers, message):
        """
        Revert all the writable values on a device.

        To revert a device publish a message to the following topic:

        ``devices/actuators/revert/device/<device path>``

        with the fallowing header:

        .. code-block:: python

            {
                'requesterID': <Agent ID>
            }

        The ActuatorAgent will reply on the **value** topic
        for the actuator:

        ``devices/actuators/reverted/device/<full device path>``

        to indicate that a point was reverted.

        Errors will be published on

        ``devices/actuators/error/<full device path>/<actuation point>``

        with the same header as the request.
        """
        point = topic.replace(topics.ACTUATOR_REVERT_DEVICE()+'/', '', 1)
        requester = headers.get('requesterID')
        headers = self._get_headers(requester)

        try:
            self.revert_device(requester, point)
        except RemoteError as ex:
            self._handle_remote_error(ex, point, headers)
        except StandardError as ex:
            self._handle_standard_error(ex, point, headers)

    def handle_schedule_request(self, peer, sender, bus, topic, headers, message):
        """
        Schedule request pub/sub handler

        An agent can request a task schedule by publishing to the
        ``devices/actuators/schedule/request`` topic with the following header:

        .. code-block:: python

            {
                'type': 'NEW_SCHEDULE',
                'requesterID': <Agent ID>, #The name of the requesting agent.
                'taskID': <unique task ID>, #The desired task ID for this task. It must be unique among all other scheduled tasks.
                'priority': <task priority>, #The desired task priority, must be 'HIGH', 'LOW', or 'LOW_PREEMPT'
            }

        The message must describe the blocks of time using the format described in `Device Schedule`_.

        A task may be canceled by publishing to the
        ``devices/actuators/schedule/request`` topic with the following header:

        .. code-block:: python

            {
                'type': 'CANCEL_SCHEDULE',
                'requesterID': <Agent ID>, #The name of the requesting agent.
                'taskID': <unique task ID>, #The task ID for the canceled Task.
            }

        requesterID
            The name of the requesting agent.
        taskID
            The desired task ID for this task. It must be unique among all other scheduled tasks.
        priority
            The desired task priority, must be 'HIGH', 'LOW', or 'LOW_PREEMPT'

        No message is requires to cancel a schedule.

        """
        if sender == 'pubsub.compat':
            message = compat.unpack_legacy_message(headers, message)

        request_type = headers.get('type')
        _log.debug('handle_schedule_request: {topic}, {headers}, {message}'.
                   format(topic=topic, headers=str(headers), message=str(message)))

        requester_id = headers.get('requesterID')
        task_id = headers.get('taskID')
        priority = headers.get('priority')

        if request_type == SCHEDULE_ACTION_NEW:
            try:
                if len(message) == 1:
                    requests = message[0]
                else:
                    requests = message

                self.request_new_schedule(requester_id, task_id, priority, requests)
            except StandardError as ex:
                return self._handle_unknown_schedule_error(ex, headers, message)

        elif request_type == SCHEDULE_ACTION_CANCEL:
            try:
                self.request_cancel_schedule(requester_id, task_id)
            except StandardError as ex:
                return self._handle_unknown_schedule_error(ex, headers, message)
        else:
            _log.debug('handle-schedule_request, invalid request type')
            self.vip.pubsub.publish('pubsub', topics.ACTUATOR_SCHEDULE_RESULT(), headers,
                                    {'result': SCHEDULE_RESPONSE_FAILURE,
                                     'data': {},
                                     'info': 'INVALID_REQUEST_TYPE'})

    @RPC.export
    def get_point(self, topic, **kwargs):
        """
        RPC method

        Gets up to date value of a specific point on a device.
        Does not require the device be scheduled.

        :param topic: The topic of the point to grab in the
                      format <device topic>/<point name>
        :param \*\*kwargs: Any driver specific parameters
        :type topic: str
        :returns: point value
        :rtype: any base python type"""
        topic = topic.strip('/')
        _log.debug('handle_get: {topic}'.format(topic=topic))
        path, point_name = topic.rsplit('/', 1)
        return self.instances[path].get_point(point_name, **kwargs)

    @RPC.export
    def set_point(self, requester_id, topic, value, **kwargs):
        """RPC method

        Sets the value of a specific point on a device.
        Requires the device be scheduled by the calling agent.

        :param requester_id: Identifier given when requesting schedule.
        :param topic: The topic of the point to set in the
                      format <device topic>/<point name>
        :param value: Value to set point to.
        :param \*\*kwargs: Any driver specific parameters
        :type topic: str
        :type requester_id: str
        :type value: any basic python type
        :returns: value point was actually set to. Usually invalid values
                cause an error but some drivers (MODBUS) will return a different
                value with what the value was actually set to.
        :rtype: any base python type

        .. warning:: Calling without previously scheduling a device and not within
                     the time allotted will raise a LockError"""

        topic = topic.strip('/')
        _log.debug('handle_set: {topic},{requester_id}, {value}'.
                   format(topic=topic, requester_id=requester_id, value=value))

        path, point_name = topic.rsplit('/', 1)

        headers = self._get_headers(requester_id)
        if not isinstance(requester_id, str):
            raise TypeError("Agent id must be a nonempty string")
        if self._check_lock(path, requester_id):
            result = self.instances[path].set_point(point_name, value, **kwargs)
            headers = self._get_headers(requester_id)
            self._push_result_topic_pair(WRITE_ATTEMPT_PREFIX,
                                        topic, headers, value)
            self._push_result_topic_pair(VALUE_RESPONSE_PREFIX,
                                        topic, headers, result)
        else:
            raise LockError("caller ({}) does not have this lock".format(requester_id))

        return result

    @RPC.export
    def revert_point(self, requester_id, topic, **kwargs):
        """
        RPC method

        Reverts the value of a specific point on a device to a default state.
        Requires the device be scheduled by the calling agent.

        :param requester_id: Identifier given when requesting schedule.
        :param topic: The topic of the point to revert in the
                      format <device topic>/<point name>
        :param \*\*kwargs: Any driver specific parameters
        :type topic: str
        :type requester_id: str

        .. warning:: Calling without previously scheduling a device and not within
                     the time allotted will raise a LockError"""

        topic = topic.strip('/')
        _log.debug('handle_revert: {topic},{requester_id}'.
                   format(topic=topic, requester_id=requester_id))

        path, point_name = topic.rsplit('/', 1)

        headers = self._get_headers(requester_id)

        if self._check_lock(path, requester_id):
            self.instances[path].revert_point(point_name, **kwargs)

            headers = self._get_headers(requester_id)
            self._push_result_topic_pair(REVERT_POINT_RESPONSE_PREFIX,
                                        topic, headers, None)
        else:
            raise LockError("caller does not have this lock")

    @RPC.export
    def revert_device(self, requester_id, topic, **kwargs):
        """
        RPC method

        Reverts all points on a device to a default state.
        Requires the device be scheduled by the calling agent.

        :param requester_id: Identifier given when requesting schedule.
        :param topic: The topic of the device to revert
        :param \*\*kwargs: Any driver specific parameters
        :type topic: str
        :type requester_id: str

        .. warning:: Calling without previously scheduling a device and not within
                     the time allotted will raise a LockError"""

        topic = topic.strip('/')
        _log.debug('handle_revert: {topic},{requester_id}'.
                   format(topic=topic, requester_id=requester_id))

        path = topic

        headers = self._get_headers(requester_id)

        if self._check_lock(path, requester_id):
            self.instances[path].revert_all(**kwargs)

            headers = self._get_headers(requester_id)
            self._push_result_topic_pair(REVERT_DEVICE_RESPONSE_PREFIX,
                                        topic, headers, None)
        else:
            raise LockError("caller does not have this lock")

    @RPC.export
    def request_new_schedule(self, requester_id, task_id, priority, requests):
        """
        RPC method

        Requests one or more blocks on time on one or more device.

        :param requester_id: Requester name.
        :param task_id: Task name.
        :param priority: Priority of the task. Must be either "HIGH", "LOW", or "LOW_PREEMPT"
        :param requests: A list of time slot requests in the format described in `Device Schedule`_.

        :type requester_id: str
        :type task_id: str
        :type priority: str
        :type request: list
        :returns: Request result
        :rtype: dict

        :Return Values:

            The return values are described in `New Task Response`_.
        """

        now = datetime.now()

        topic = topics.ACTUATOR_SCHEDULE_RESULT()
        headers = self._get_headers(requester_id, task_id=task_id)
        headers['type'] = SCHEDULE_ACTION_NEW

        try:
            if requests and isinstance(requests[0], basestring):
                requests = [requests]
            requests = [[r[0].strip('/'),
                         utils.parse_timestamp_string(r[1]),
                         utils.parse_timestamp_string(r[2])] for r in requests]

        except StandardError as ex:
            return self._handle_unknown_schedule_error(ex, headers, requests)

        _log.debug("Got new schedule request: {}, {}, {}, {}".
                   format(requester_id, task_id, priority, requests))

        result = self._schedule_manager.request_slots(requester_id, task_id, requests, priority, now)
        success = SCHEDULE_RESPONSE_SUCCESS if result.success else SCHEDULE_RESPONSE_FAILURE

        # Dealing with success and other first world problems.
        if result.success:
            self._update_device_state_and_schedule(now)
            for preempted_task in result.data:
                preempt_headers = self._get_headers(preempted_task[0], task_id=preempted_task[1])
                preempt_headers['type'] = SCHEDULE_ACTION_CANCEL
                self.vip.pubsub.publish('pubsub', topic, headers=preempt_headers,
                                        message={'result': SCHEDULE_CANCEL_PREEMPTED,
                                                 'info': '',
                                                 'data': {'agentID': requester_id,
                                                          'taskID': task_id}})

        # If we are successful we do something else with the real result data
        data = result.data if not result.success else {}

        results = {'result': success,
                   'data': data,
                   'info': result.info_string}
        self.vip.pubsub.publish('pubsub', topic, headers=headers, message=results)

        return results

    @RPC.export
    def request_cancel_schedule(self, requester_id, task_id):
        """RPC method

        Requests the cancelation of the specified task id.

        :param requester_id: Requester name.
        :param task_id: Task name.

        :type requester_id: str
        :type task_id: str
        :returns: Request result
        :rtype: dict

        :Return Values:

        The return values are described in `Cancel Task Response`_.

        """
        now = datetime.now()
        headers = self._get_headers(requester_id, task_id=task_id)
        headers['type'] = SCHEDULE_ACTION_CANCEL

        result = self._schedule_manager.cancel_task(requester_id, task_id, now)
        success = SCHEDULE_RESPONSE_SUCCESS if result.success else SCHEDULE_RESPONSE_FAILURE

        topic = topics.ACTUATOR_SCHEDULE_RESULT()
        message = {'result': success,
                   'info': result.info_string,
                   'data': {}}
        self.vip.pubsub.publish('pubsub', topic,
                                headers=headers,
                                message=message)

        if result.success:
            self._update_device_state_and_schedule(now)

        return message


def main(argv=sys.argv):
    '''Main method called to start the agent.'''
    utils.vip_main(master_driver_agent)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass
