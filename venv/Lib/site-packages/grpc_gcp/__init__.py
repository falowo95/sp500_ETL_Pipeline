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
from google.protobuf import text_format
from grpc_gcp import _channel
from grpc_gcp.proto import grpc_gcp_pb2

# The channel argument to for gRPC-GCP API config.
# Its value must be a valid ApiConfig proto defined in grpc_gcp.proto.
API_CONFIG_CHANNEL_ARG = 'grpc_gcp.api_config'


def secure_channel(target, credentials, options=None):
    """Creates a secure Channel to a server.

    Args:
      target: The server address.
      credentials: A ChannelCredentials instance.
      options: An optional list of key-value pairs (channel args
        in gRPC Core runtime) to configure the channel.

    Returns:
      A Channel object.
    """
    if options and [arg for arg in options
                    if arg[0] == API_CONFIG_CHANNEL_ARG]:
        return _channel.Channel(target, ()
                                if options is None else options, credentials)
    else:
        return grpc.secure_channel(target, credentials, options)


def insecure_channel(target, options=None):
    """Creates an insecure Channel to a server.

    Args:
      target: The server address.
      options: An optional list of key-value pairs (channel args
        in gRPC Core runtime) to configure the channel.

    Returns:
      A Channel object.
    """
    if options and [arg for arg in options
                    if arg[0] == API_CONFIG_CHANNEL_ARG]:
        return _channel.Channel(target, options)
    else:
        return grpc.insecure_channel(target, options)


def api_config_from_text_pb(text_pb):
    """Creates an instance of ApiConfig with provided api configuration.

    Args:
      text_pb: The text representation of a protocol message defining api
        configuration.

    Returns:
      A grpc_gcp_pb2.ApiConfig object.
    """
    config = grpc_gcp_pb2.ApiConfig()
    text_format.Merge(text_pb, config)
    return config
