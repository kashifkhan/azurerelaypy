from urllib.parse import urlsplit
from ._azure_relay_listener import HybridConnectionListener
import typing
import requests


from azure.core.polling.base_polling import LROBasePolling
from azure.mgmt.core.polling.arm_polling import ARMPolling
from azure.core.rest import HttpRequest

__all__ = [
    "HybridConnectionListener",
    "apply_dark_magic",
    ]




class RelayArmPollerMixin(LROBasePolling):
    def _delay(self) -> None:
        return super()._delay()
    

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
                session_id = response_json['sessionId']
                connection_details = response_json['connectionDetails']
                sas_token = connection_details['sharedAccessSignature']
                endpoint_info = urlsplit(connection_details['endPoint'])
                fqn = endpoint_info.hostname
                entity_path = endpoint_info.path[1:]
    return SessionManagementClient

def apply_dark_magic():
    if not RelayArmPollerMixin in ARMPolling.__bases__:
        ARMPolling.__bases__ = (RelayArmPollerMixin,)

    import azure.mgmt.storage
    azure.mgmt.storage.StorageManagementClient = make_notification_session_aware(azure.mgmt.storage.StorageManagementClient)
