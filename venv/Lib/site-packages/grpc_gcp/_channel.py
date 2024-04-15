# Copyright 2018 gRPC-GCP authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import grpc
import grpc_gcp
import itertools
import threading

from grpc_gcp.proto import grpc_gcp_pb2

# The channel arg to distinguish different gRPC channels.
_CLIENT_CHANNEL_ID = 'grpc_gcp.client_channel.id'


class _RendezvousDoneCallback(object):
    """A callback which is guaranteed to be invoked when a rendezvous is done."""

    def __init__(self, call, channel, key):
        self._call = call
        self._channel = channel
        self._key = key

    def __call__(self, rendezvous):
        if rendezvous.code() is grpc.StatusCode.OK:
            self._call._postprocess(
                self._channel, self._key, rendezvous.result(), rendezvous)


class _Rendezvous(grpc.RpcError, grpc.Future, grpc.Call):
    """A proxy of grpc._Rendezvous which delegates all the methods to the rendezvous returned by the underlying
    grpc.Channel."""

    def __init__(self, rendezvous):
        super(_Rendezvous, self).__init__()
        self._rendezvous = rendezvous
        self._is_first_response_message_consumed = False
        self._on_first_response_message_callback = None
        self._is_stop_iteration_raised = False
        self._on_stop_iteration_callback = None

    def is_active(self):
        return self._rendezvous.is_active()

    def time_remaining(self):
        return self._rendezvous.time_remaining()

    def add_callback(self, callback):
        self._rendezvous.add_callback(callback)

    def initial_metadata(self):
        return self._rendezvous.initial_metadata()

    def trailing_metadata(self):
        return self._rendezvous.trailing_metadata()

    def code(self):
        return self._rendezvous.code()

    def details(self):
        return self._rendezvous.details()

    def cancel(self):
        return self._rendezvous.cancel()

    def cancelled(self):
        return self._rendezvous.cancelled()

    def running(self):
        return self._rendezvous.running()

    def done(self):
        return self._rendezvous.done()

    def result(self, timeout=None):
        return self._rendezvous.result(timeout)

    def exception(self, timeout=None):
        return self._rendezvous.exception(timeout)

    def traceback(self, timeout=None):
        return self._rendezvous.trackback(timeout)

    def add_done_callback(self, fn):
        self._rendezvous.add_done_callback(fn)

    def __iter__(self):
        return self

    def _next(self):
        """Hooks the next() method to invoke the on_first_response_message callback"""
        try:
            message = self._rendezvous._next()
        except StopIteration:
            if self._is_stop_iteration_raised is not True and self._on_stop_iteration_callback is not None:
                self._is_stop_iteration_raised = True
                self._on_stop_iteration_callback()
            raise

        if self._is_first_response_message_consumed is not True \
                and self._on_first_response_message_callback is not None:
            self._is_first_response_message_consumed = True
            self._on_first_response_message_callback(message)
        return message

    def __next__(self):
        return self._next()

    def next(self):
        return self._next()

    def __repr__(self):
        return repr(self._rendezvous)

    def __str__(self):
        return str(self._rendezvous)

    def _add_on_first_response_message_callback(self, callback):
        """Adds a callback to be invoked when consuming the first response message."""
        self._on_first_response_message_callback = callback

    def _add_on_stop_iteration_callback(self, callback):
        """Adds a callback to be invoked when StopIteration being raised."""
        self._on_stop_iteration_callback = callback


class _MultiCallableProcessor(object):
    """The base class which abstracts the channel management features."""

    def __init__(self, method, request_serializer, response_deserializer,
                 gcp_channel):
        self._method = method
        self._request_serializer = request_serializer
        self._response_deserializer = response_deserializer
        self._gcp_channel = gcp_channel
        self._affinity = gcp_channel._affinity_by_method.get(
            self._method, None)

    def method(self):
        return self._method

    def request_serializer(self):
        return self._request_serializer

    def response_deserializer(self):
        return self._response_deserializer

    def channel(self):
        return self._gcp_channel

    def _get_affinity_key_from_proto(self, proto):
        """Gets the affinity key from the given proto."""
        if self._affinity:
            names = self._affinity.affinity_key.split('.')
            if names:
                for name in names:
                    proto = getattr(proto, name)
                return proto
        raise KeyError('Cannot find the field in the proto, path: {}'.format(
            self._affinity.affinity_key))

    def _preprocess(self, request):
        """Pre-process the call by handling the channel management features before the
         actual gRPC call.

        Includes:
            1. If channel affinity is desired, get a gRPC channel bound to the affinity key.
            2. If channel affinity is not desired, get a gRPC channel from the channel pool.
            3. Tracks the affinity ref count.
            4. Tracks the active stream ref count.

        Returns:
            tuple of channel, affinity_key.
        """
        affinity_key = None
        if (request is not None and
                self._affinity and (
                self._affinity.command == grpc_gcp_pb2.AffinityConfig.BOUND or
                self._affinity.command == grpc_gcp_pb2.AffinityConfig.UNBIND
                )):
            affinity_key = self._get_affinity_key_from_proto(request)

        channel_ref = self._gcp_channel._get_channel_ref(affinity_key)
        channel_ref.active_stream_ref_incr()
        return channel_ref, affinity_key

    def _postprocess(self, channel_ref, key, response, rendezvous):
        """Post-process the call by handling the channel management features after the
         actual gRPC call.

        Includes:
            1. If `bind` command is specified, bind the gRPC channel with the given affinity key.
            2. If `unbind` command is specified, unbind the gRPC channel with the given affinity key.
            3. Tracks the affinity ref count.
            4. Tracks the active stream ref count. (This will be done in the sub-classes, to handle different RPC
             semantics, unary, bidi, etc.)
        """
        if self._affinity:
            if self._affinity.command == grpc_gcp_pb2.AffinityConfig.BIND:
                if rendezvous.code() is not grpc.StatusCode.OK:
                    return
                key = self._get_affinity_key_from_proto(response)
                self._gcp_channel._bind(channel_ref, key)
            elif self._affinity.command == grpc_gcp_pb2.AffinityConfig.UNBIND:
                self._gcp_channel._unbind(key)


class _UnaryUnaryMultiCallable(grpc.UnaryUnaryMultiCallable):
    def __init__(self, method, request_serializer, response_deserializer,
                 gcp_channel):
        self._multi_callable_processor = _MultiCallableProcessor(
            method, request_serializer, response_deserializer, gcp_channel)

    def __call__(self, request, timeout=None, metadata=None, credentials=None):
        response, _ = self.with_call(request, timeout, metadata, credentials)
        return response

    def _preprocess(self, request):
        return self._multi_callable_processor._preprocess(request)

    def _postprocess(self, channel_ref, key, response, rendezvous):
        self._multi_callable_processor._postprocess(channel_ref, key, response,
                                                    rendezvous)
        channel_ref.active_stream_ref_decr()

    def with_call(self, request, timeout=None, metadata=None,
                  credentials=None):
        channel_ref, affinity_key = self._preprocess(request)
        response, rendezvous = channel_ref.channel().unary_unary(
            self._multi_callable_processor.method(),
            self._multi_callable_processor.request_serializer(),
            self._multi_callable_processor.response_deserializer()).with_call(
                request, timeout, metadata, credentials)
        self._postprocess(channel_ref, affinity_key, response, rendezvous)
        return response, rendezvous

    def future(self, request, timeout=None, metadata=None, credentials=None):
        channel_ref, affinity_key = self._preprocess(request)
        rendezvous = channel_ref.channel().unary_unary(
            self._multi_callable_processor.method(),
            self._multi_callable_processor.request_serializer(),
            self._multi_callable_processor.response_deserializer()).future(
                request, timeout, metadata, credentials)
        callback = _RendezvousDoneCallback(self, channel_ref, affinity_key)
        rendezvous.add_done_callback(callback)
        return rendezvous


class _UnaryStreamMultiCallable(grpc.UnaryStreamMultiCallable):
    def __init__(self, method, request_serializer, response_deserializer,
                 gcp_channel):
        self._multi_callable_processor = _MultiCallableProcessor(
            method, request_serializer, response_deserializer, gcp_channel)

    def _preprocess(self, request):
        return self._multi_callable_processor._preprocess(request)

    def _postprocess(self, channel_ref, key, response, rendezvous):
        self._multi_callable_processor._postprocess(channel_ref, key, response,
                                                    rendezvous)

    def __call__(self, request, timeout=None, metadata=None, credentials=None):
        channel_ref, affinity_key = self._preprocess(request)
        rendezvous = _Rendezvous(channel_ref.channel().unary_stream(
            self._multi_callable_processor.method(),
            self._multi_callable_processor.request_serializer(),
            self._multi_callable_processor.response_deserializer())(
                request, timeout, metadata, credentials))
        rendezvous._add_on_first_response_message_callback(
            lambda response: self._postprocess(channel_ref, affinity_key, response, rendezvous)
        )
        rendezvous._add_on_stop_iteration_callback(
            lambda: channel_ref.active_stream_ref_decr())
        return rendezvous


class _StreamUnaryMultiCallable(grpc.StreamUnaryMultiCallable):
    def __init__(self, method, request_serializer, response_deserializer,
                 gcp_channel):
        self._multi_callable_processor = _MultiCallableProcessor(
            method, request_serializer, response_deserializer, gcp_channel)

    def _preprocess(self, request):
        return self._multi_callable_processor._preprocess(request)

    def _postprocess(self, channel_ref, key, response, rendezvous):
        self._multi_callable_processor._postprocess(channel_ref, key, response,
                                                    rendezvous)
        channel_ref.active_stream_ref_decr()

    def __call__(self,
                 request_iterator,
                 timeout=None,
                 metadata=None,
                 credentials=None):
        response, _ = self.with_call(request_iterator, timeout, metadata,
                                     credentials)
        return response

    def with_call(self,
                  request_iterator,
                  timeout=None,
                  metadata=None,
                  credentials=None):
        request = next(request_iterator)
        channel_ref, affinity_key = self._preprocess(request)
        response, rendezvous = channel_ref.channel().stream_unary(
            self._multi_callable_processor.method(),
            self._multi_callable_processor.request_serializer(),
            self._multi_callable_processor.response_deserializer()).with_call(
                itertools.chain([request], request_iterator), timeout,
                metadata, credentials)
        self._postprocess(channel_ref, affinity_key, response, rendezvous)
        return response, rendezvous

    def future(self,
               request_iterator,
               timeout=None,
               metadata=None,
               credentials=None):
        request = next(request_iterator)
        channel_ref, affinity_key = self._preprocess(request)
        rendezvous = channel_ref.channel().stream_unary(
            self._multi_callable_processor.method(),
            self._multi_callable_processor.request_serializer(),
            self._multi_callable_processor.response_deserializer()).future(
                itertools.chain([request], request_iterator), timeout,
                metadata, credentials)
        callback = _RendezvousDoneCallback(self, channel_ref, affinity_key)
        rendezvous.add_done_callback(callback)
        return rendezvous


class _StreamStreamMultiCallable(grpc.StreamStreamMultiCallable):
    def __init__(self, method, request_serializer, response_deserializer,
                 gcp_channel):
        self._multi_callable_processor = _MultiCallableProcessor(
            method, request_serializer, response_deserializer, gcp_channel)

    def _preprocess(self, request):
        return self._multi_callable_processor._preprocess(request)

    def _postprocess(self, channel_ref, key, response, rendezvous):
        self._multi_callable_processor._postprocess(channel_ref, key, response,
                                                    rendezvous)

    def __call__(self,
                 request_iterator,
                 timeout=None,
                 metadata=None,
                 credentials=None):
        request = next(request_iterator)
        channel_ref, affinity_key = self._preprocess(request)
        rendezvous = _Rendezvous(channel_ref.channel().stream_stream(
            self._multi_callable_processor.method(),
            self._multi_callable_processor.request_serializer(),
            self._multi_callable_processor.response_deserializer())(
                itertools.chain([request], request_iterator), timeout,
                metadata, credentials))
        rendezvous._add_on_first_response_message_callback(
            lambda response: self._postprocess(channel_ref, affinity_key, response, rendezvous)
        )
        rendezvous._add_on_stop_iteration_callback(
            lambda: channel_ref.active_stream_ref_decr())
        return rendezvous


class _ChannelRef(object):
    def __init__(self,
                 channel,
                 channel_id,
                 affinity_ref=0,
                 active_stream_ref=0):
        self._channel = channel
        self._channel_id = channel_id
        self._affinity_ref = affinity_ref
        self._active_stream_ref = active_stream_ref

    def affinity_ref_incr(self):
        self._affinity_ref += 1

    def affinity_ref_decr(self):
        self._affinity_ref -= 1

    def affinity_ref(self):
        return self._affinity_ref

    def active_stream_ref_incr(self):
        self._active_stream_ref += 1

    def active_stream_ref_decr(self):
        self._active_stream_ref -= 1

    def active_stream_ref(self):
        return self._active_stream_ref

    def channel(self):
        return self._channel


def _get_api_config_channel_arg(options):
    if not options:
        return None
    arg = [
        arg for arg in options
        if arg[0] == grpc_gcp.API_CONFIG_CHANNEL_ARG
    ]
    if arg is None or len(arg) == 0:
        return None
    options.remove(arg[0])
    return arg[0][1]


class Channel(grpc.Channel):
    """A dummy channel which is backed by a pool of managed channels."""

    def __init__(self, target, options=None, credentials=None):
        self._options = [] if options is None else list(options)
        self._config = _get_api_config_channel_arg(self._options)
        # Default to 10.
        self._max_size = 10
        # Default to 100
        self._max_concurrent_streams_low_watermark = 100

        if self._config is not None and self._config.channel_pool is not None:
            if self._config.channel_pool.max_size:
                # Use user defined values if max_size is configured
                self._max_size = self._config.channel_pool.max_size
            if self._config.channel_pool.max_concurrent_streams_low_watermark:
                # Use user defined values if max_concurrent_streams_low_watermark is configured
                self._max_concurrent_streams_low_watermark = \
                    self._config.channel_pool.max_concurrent_streams_low_watermark

        self._target = target
        self._credentials = credentials
        # A dict of {method name: affinity config}
        self._affinity_by_method = self._init_affinity_by_method_index()
        self._lock = threading.RLock()
        # A dict of {affinity key: channel_ref_data}.
        self._channel_ref_by_affinity_key = {}
        # A list of managed channel refs.
        self._channel_refs = []
        self._subscribers = []
        self._channel_pool_connectivity = None
        # Create a new idle channel
        self._get_channel_ref()
        return

    def _init_affinity_by_method_index(self):
        index = {}
        if self._config is not None:
            for method in self._config.method:
                # TODO(fengli): supports wildcard in method selector.
                for name in method.name:
                    index[name] = method.affinity
        return index

    def _bind(self, channel_ref, affinity_key):
        with self._lock:
            if self._channel_ref_by_affinity_key.get(affinity_key,
                                                     None) is None:
                self._channel_ref_by_affinity_key[affinity_key] = channel_ref
            self._channel_ref_by_affinity_key[affinity_key].affinity_ref_incr()
            return channel_ref

    def _unbind(self, affinity_key):
        with self._lock:
            channel_ref = self._channel_ref_by_affinity_key.pop(affinity_key, None)
            if channel_ref:
                channel_ref.affinity_ref_decr()
            return channel_ref

    def _get_channel_ref(self, affinity_key=None):
        """Returns a gRPC channel ref which has been bound to the given affinity
         key."""
        # TODO(fengli): Supports load reporting.
        with self._lock:
            if affinity_key:
                # Finds the gRPC channel according to the affinity key.
                channel_ref = self._channel_ref_by_affinity_key.get(
                    affinity_key)
                if channel_ref:
                    return channel_ref
                # TODO(fengli): If affinity key not found, log an error.

            # TODO(fengli): Creates new gRPC channels on demand, depends on the load reporting.
            num_channel_refs = len(self._channel_refs)
            sorted_channel_refs = sorted(
                self._channel_refs, key=lambda ref: ref.active_stream_ref())

            if (sorted_channel_refs and
                    sorted_channel_refs[0].active_stream_ref()
                    < self._max_concurrent_streams_low_watermark):
                # If there's a free channel with low active streams, use it.
                return sorted_channel_refs[0]

            if num_channel_refs < self._max_size:
                # Creates a new gRPC channel.
                options = self._options + [
                    (_CLIENT_CHANNEL_ID, num_channel_refs),
                ]
                if self._credentials:
                    channel = grpc.secure_channel(
                        self._target, self._credentials, options)
                else:
                    channel = grpc.insecure_channel(self._target, options)

                channel_ref = _ChannelRef(channel, num_channel_refs)
                self._channel_refs.append(channel_ref)
                if self._subscribers:
                    channel.subscribe(self._on_subscribe_callback)
                return channel_ref

            # If all channels are overloaded and the channel pool is full already,
            # return the channel with least active streams.
            return sorted_channel_refs[0]

    def unary_unary(self,
                    method,
                    request_serializer=None,
                    response_deserializer=None):
        return _UnaryUnaryMultiCallable(method, request_serializer,
                                        response_deserializer, self)

    def unary_stream(self,
                     method,
                     request_serializer=None,
                     response_deserializer=None):
        return _UnaryStreamMultiCallable(method, request_serializer,
                                         response_deserializer, self)

    def stream_unary(self,
                     method,
                     request_serializer=None,
                     response_deserializer=None):
        return _StreamUnaryMultiCallable(method, request_serializer,
                                         response_deserializer, self)

    def stream_stream(self,
                      method,
                      request_serializer=None,
                      response_deserializer=None):
        return _StreamStreamMultiCallable(method, request_serializer,
                                          response_deserializer, self)

    def _on_subscribe_callback(self, connectivity_state):
        del connectivity_state
        with self._lock:
            ready = 0
            idle = 0
            connecting = 0
            transient_failure = 0
            shutdown = 0
            for channel_ref in self._channel_refs:
                if channel_ref.channel(
                )._connectivity_state.connectivity is None:
                    continue
                elif channel_ref.channel(
                )._connectivity_state.connectivity == grpc.ChannelConnectivity.READY:
                    ready += 1
                elif channel_ref.channel(
                )._connectivity_state.connectivity == grpc.ChannelConnectivity.SHUTDOWN:
                    shutdown += 1
                elif channel_ref.channel(
                )._connectivity_state.connectivity == grpc.ChannelConnectivity.TRANSIENT_FAILURE:
                    transient_failure += 1
                elif channel_ref.channel(
                )._connectivity_state.connectivity == grpc.ChannelConnectivity.CONNECTING:
                    connecting += 1
                elif channel_ref.channel(
                )._connectivity_state.connectivity == grpc.ChannelConnectivity.IDLE:
                    idle += 1
            if ready > 0:
                current_connectivity = grpc.ChannelConnectivity.READY
            elif idle > 0:
                current_connectivity = grpc.ChannelConnectivity.IDLE
            elif connecting > 0:
                current_connectivity = grpc.ChannelConnectivity.CONNECTING
            elif transient_failure > 0:
                current_connectivity = grpc.ChannelConnectivity.TRANSIENT_FAILURE
            elif shutdown > 0:
                current_connectivity = grpc.ChannelConnectivity.SHUTDOWN
            else:
                current_connectivity = grpc.ChannelConnectivity.IDLE

            if current_connectivity != self._channel_pool_connectivity:
                self._channel_pool_connectivity = current_connectivity
                self._spawn_subscribe_callbacks(current_connectivity)

    def _spawn_subscribe_callbacks(self, state):
        for callback in self._subscribers:
            callback(state)

    def subscribe(self, callback, try_to_connect=False):
        with self._lock:
            for channel_ref in self._channel_refs:
                channel_ref.channel().unsubscribe(self._on_subscribe_callback)
                channel_ref.channel().subscribe(
                    self._on_subscribe_callback, try_to_connect)
            self._subscribers.append(callback)

    def unsubscribe(self, callback):
        with self._lock:
            self._subscribers.remove(callback)
            if not self._subscribers:
                for channel_ref in self._channel_refs:
                    channel_ref.channel().unsubscribe(
                        self._on_subscribe_callback)

    def close(self):
        with self._lock:
            for channel_ref in self._channel_refs:
                channel_ref.channel().close()
