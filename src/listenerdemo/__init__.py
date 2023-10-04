import logging
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit

import requests
from azure.core.polling.base_polling import LROBasePolling
from azure.mgmt.core.polling.arm_polling import ARMPolling

from ._azure_relay_listener import HybridConnectionListener

LOG = logging.getLogger(__name__)

__all__ = [
    "HybridConnectionListener",
    "apply_dark_magic",
    ]




class RelayArmPollerMixin(LROBasePolling):
    def __init__(self, timeout, lro_algorithms, lro_options, path_format_arguments, **operation_config: Any):
        super().__init__(timeout, lro_algorithms, lro_options, path_format_arguments, **operation_config)
        self.use_relay = True

    def _poll(self) -> None:
        if self.use_relay:
            id = self._initial_response.http_request.headers['x-ms-operation-identifier']
            LOG.debug(f"Waiting for relay to fire for {id}")
            notification = self._client.hybrid_connection_listener.wait(id, 600) # fallback to poll time
            LOG.debug("Either we got a response or we timed out")
            if notification is None:
                LOG.debug(f"Relay did not fire for {id}, falling back polling")
                self.use_relay = False
            else:
                self._client.event_time = datetime.fromisoformat(notification[0]['eventTime'])
                LOG.debug(f"Relay recieved notification {id} at {self._client.event_time}")
        super()._poll()

def make_notification_session_aware(client_type):
    class SessionManagementClient(client_type):
        def __init__(self, *args, **kwargs):
            session = kwargs.pop("session", None)
            super().__init__(*args, **kwargs)
            if session:
                self.arn_endpoint = f'https://arn-cuseuap-nt.arn.core.windows.net/providers/Microsoft.ResourceNotifications/ephemeralEventsSubscription/{session}/getConnectionDetails?api-version=2023-08-01-Preview'
                self._client._notification_channel = session
                #arn_endpoint_response = self._client.send_request(HttpRequest("POST", "https://management.azure.com/providers/Microsoft.ResourceNotifications/arn?api-version=2021-06-01"), headers= {'Authorization': f'Bearer {args[0].get_token("https://management.core.windows.net").token}'})
                resp = requests.post(url=self.arn_endpoint, headers= {'Authorization': f'Bearer {args[0].get_token("https://management.core.windows.net").token}'})
                response_json = resp.json()
                self._client.session_id = response_json['sessionId']
                self.session_id = response_json['sessionId']
                connection_details = response_json['connectionDetails']
                self._client.sas_token = connection_details['sharedAccessSignature']
                endpoint_info = urlsplit(connection_details['endPoint'])
                self._client.fully_qualified_name = endpoint_info.hostname
                self._client.entity_path = endpoint_info.path[1:]
                self._client.hybrid_connection_listener = HybridConnectionListener(self._client.fully_qualified_name, self._client.entity_path, sas_token = self._client.sas_token)
                self._client.hybrid_connection_listener.open()
    return SessionManagementClient

def apply_dark_magic():
    if not RelayArmPollerMixin in ARMPolling.__bases__:
        ARMPolling.__bases__ = (RelayArmPollerMixin,)

    import azure.mgmt.storage
    azure.mgmt.storage.StorageManagementClient = make_notification_session_aware(azure.mgmt.storage.StorageManagementClient)

    import azure.mgmt.compute
    azure.mgmt.compute.ComputeManagementClient = make_notification_session_aware(azure.mgmt.compute.ComputeManagementClient)