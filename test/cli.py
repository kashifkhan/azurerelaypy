from azure.identity import DefaultAzureCredential
import requests
from urllib.parse import urlsplit

from _azure_relay_listener import HybridConnectionListener

import logging

logging.basicConfig(level=logging.DEBUG)




if __name__ == "__main__":
    connection_details_url = 'https://arn-cuseuap-nt.arn.core.windows.net/providers/Microsoft.ResourceNotifications/ephemeralEventsSubscription/ephemeralEventsSubscriptionID/getConnectionDetails?api-version=2023-08-01-Preview'
    cred = DefaultAzureCredential()
    resp = requests.post(url=connection_details_url, headers= {'Authorization': f'Bearer {cred.get_token("https://management.core.windows.net").token}'})

    if resp.status_code != 200:
        raise Exception(f"Failed to get connection details from {connection_details_url}")
    
    response_json = resp.json()

    session_id = response_json['sessionId']

    connection_details = response_json['connectionDetails']

    sas_token = connection_details['sharedAccessSignature']
    endpoint_info = urlsplit(connection_details['endPoint'])

    fqn = endpoint_info.hostname
    entity_path = endpoint_info.path[1:]

    listener = HybridConnectionListener(fqn,entity_path,sas_token=sas_token)
    print(listener.listener_url)
    listener.receive(None)