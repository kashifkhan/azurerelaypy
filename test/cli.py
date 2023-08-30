from azure.identity import DefaultAzureCredential

from urllib.parse import urlsplit

from listenerdemo import apply_dark_magic, HybridConnectionListener

import logging

logging.basicConfig(level=logging.DEBUG)

import requests




if __name__ == "__main__":
    apply_dark_magic()

    import azure.mgmt.storage
    from azure.mgmt.storage.models import StorageAccountCreateParameters

    client = azure.mgmt.storage.StorageManagementClient(DefaultAzureCredential(), 'faa080af-c1d8-40ad-9cce-e1a450ca5b57', session='12345', logging_enable=True)
    


    # listener = HybridConnectionListener(fqn,entity_path,sas_token=sas_token)
    # print(listener.listener_url)
    # listener.receive(None)

    # This needs to look like the following code
    # read the poc lightning doc for more info
    # user creaters a mgmt client and passed in a session kwarg. 
    # that kwarg is used to fire off steps 15 - 29
    # the client will then do LRO type activities such as creating a storage account
    # then iterate over the lro results ---> should come from relay instead of polling