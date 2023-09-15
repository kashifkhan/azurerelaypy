import logging
import threading
from typing import Any, Dict, Optional, Sequence
from urllib.parse import urlsplit

import requests
from azure.core.polling import LROPoller
from azure.core.polling.base_polling import (LongRunningOperation,
                                             LROBasePolling)

from azure.mgmt.core.polling.arm_polling import ARMPolling

from ._azure_relay_listener import HybridConnectionListener, RelayPollingMethod

LOG = logging.getLogger(__name__)

__all__ = [
    "HybridConnectionListener",
    "apply_dark_magic",
    ]




class RelayArmPollerMixin(LROBasePolling):
    def __init__(self, timeout, lro_algorithms, lro_options, path_format_arguments, **operation_config: Any):
        self.arn_endpoint = 'https://arn-cuseuap-nt.arn.core.windows.net/providers/Microsoft.ResourceNotifications/ephemeralEventsSubscription/ephemeralEventsSubscriptionID/getConnectionDetails?api-version=2023-08-01-Preview'
        #self._client._notification_channel = session
        #arn_endpoint_response = self._client.send_request(HttpRequest("POST", "https://management.azure.com/providers/Microsoft.ResourceNotifications/arn?api-version=2021-06-01"), headers= {'Authorization': f'Bearer {args[0].get_token("https://management.core.windows.net").token}'})
        super().__init__(timeout, lro_algorithms, lro_options, path_format_arguments, **operation_config)
        self.use_relay = True

    def initialize(self, client, initial_response, deserialization_callback, **kwargs):
        self.relay_polling_method = RelayPollingMethod(client.fully_qualified_name, client.entity_path, client.sas_token)
        self.relay_polling_method.initialize(None, None, None)
        self.event = threading.Event()
        self.thread = threading.Thread(target=self.relay_polling_method.run, args=(self.event,), daemon=True)
        self.thread.start()
        super().initialize(client, initial_response, deserialization_callback, **kwargs)

    def _poll(self) -> None:
        if self.relay_polling_method and not self.event.is_set():
            LOG.debug("Waiting for relay to fire")
            self.event.wait(60)
            if not self.event.is_set():
                LOG.debug("Relay did not fire, falling back polling")
                self.relay_polling_method = None
            else:
                LOG.debug("Relay recieved notification")
        super()._poll()


def make_notification_session_aware(client_type):
    class SessionManagementClient(client_type):
        def __init__(self, *args, **kwargs):
            session = kwargs.pop("session", None)
            super().__init__(*args, **kwargs)
            if session:
                self.arn_endpoint = 'https://arn-cuseuap-nt.arn.core.windows.net/providers/Microsoft.ResourceNotifications/ephemeralEventsSubscription/ephemeralEventsSubscriptionID/getConnectionDetails?api-version=2023-08-01-Preview'
                self._client._notification_channel = session
                #arn_endpoint_response = self._client.send_request(HttpRequest("POST", "https://management.azure.com/providers/Microsoft.ResourceNotifications/arn?api-version=2021-06-01"), headers= {'Authorization': f'Bearer {args[0].get_token("https://management.core.windows.net").token}'})
                resp = requests.post(url=self.arn_endpoint, headers= {'Authorization': f'Bearer {args[0].get_token("https://management.core.windows.net").token}'})
                response_json = resp.json()
                self.session_id = response_json['sessionId']
                connection_details = response_json['connectionDetails']
                self._client.sas_token = connection_details['sharedAccessSignature']
                endpoint_info = urlsplit(connection_details['endPoint'])
                self._client.fully_qualified_name = endpoint_info.hostname
                self._client.entity_path = endpoint_info.path[1:]
        
        def begin_create_or_update(self, *args, **kwargs):
            polling_method = RelayPollingMethod(self.fully_qualified_name, self.entity_path, self.sas_token)
            pipeline_response = "200" # lets pretend we send off a request
            return LROPoller(
                client=self,
                initial_response=pipeline_response,
                deserialization_callback=lambda x: x,
                polling_method=polling_method,
            )
    return SessionManagementClient

def apply_dark_magic():
    if not RelayArmPollerMixin in ARMPolling.__bases__:
        ARMPolling.__bases__ = (RelayArmPollerMixin,)

    import azure.mgmt.storage
    azure.mgmt.storage.StorageManagementClient = make_notification_session_aware(azure.mgmt.storage.StorageManagementClient)

    import azure.mgmt.compute
    azure.mgmt.compute.ComputeManagementClient = make_notification_session_aware(azure.mgmt.compute.ComputeManagementClient)
