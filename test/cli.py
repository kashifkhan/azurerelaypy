import logging
import os
import uuid

from azure.identity import DefaultAzureCredential
from dotenv import find_dotenv, load_dotenv

from listenerdemo import apply_dark_magic

logging.basicConfig(level=logging.DEBUG)

load_dotenv(find_dotenv())


if __name__ == "__main__":
    apply_dark_magic()
    subscription_id = os.environ['SUBSCRIPTION_ID']
    base_url = os.environ['BASE_URL']
    session_id = os.environ['SESSION_ID']

    from datetime import datetime, timezone

    import azure.mgmt.storage
    from azure.mgmt.storage.models import StorageAccountCreateParameters

    
    client = azure.mgmt.storage.StorageManagementClient(DefaultAzureCredential(), subscription_id, session=session_id, base_url=base_url, logging_enable=True)
    
    
    parameters = StorageAccountCreateParameters(sku={'name': 'Premium_LRS'}, kind='Storage', location='centralus')

    client._client.time_sent = datetime.now(timezone.utc)
    lros = []

    for i in range(1):
        lros.append(client.storage_accounts.begin_create('amduatest', f'amduatestlistenerdemo{i}', parameters=parameters,headers={
             "x-ms-operation-identifier": f"lightning={client.session_id}/{uuid.uuid4()}"
         }))
    
    for lro in lros:
        print("Result from the 1st storage account creation")
        print(lro.result())

    client.close()
    
    print(f'total time {(datetime.now(timezone.utc) - client._client.time_sent).seconds} seconds')

    