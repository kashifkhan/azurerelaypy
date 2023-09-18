import logging

import uuid
import sys
from azure.identity import DefaultAzureCredential

from listenerdemo import apply_dark_magic

from dotenv import load_dotenv, find_dotenv
import os

logger = logging.getLogger('listenerdemo')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(stream=sys.stdout)
logger.addHandler(handler)

load_dotenv(find_dotenv())


if __name__ == "__main__":
    apply_dark_magic()

    import azure.mgmt.compute
    subscription_id = os.environ['SUBSCRIPTION_ID']
    base_url = os.environ['BASE_URL']
    session_id = os.environ['SESSION_ID']
    # client = azure.mgmt.compute.ComputeManagementClient(
    #     DefaultAzureCredential(), subscription_id, session=session_id, base_url=base_url
    # )
    # lros = []
    # for i in range(3):
    #     lros.append(client.virtual_machines.begin_create_or_update(
    #     resource_group_name="amduatest",
    #     vm_name="lightningABC",
    #     parameters={
    #         "location": "centraluseuap",
    #         "properties": {
    #             "hardwareProfile": {"vmSize": "Standard_DS1_v2"},
    #             "storageProfile": {
    #                 "imageReference": {
    #                     "publisher": "MicrosoftWindowsServer",
    #                     "offer": "WindowsServer",
    #                     "sku": "2016-datacenter-gensecond",
    #                     "version": "latest",
    #                     "exactVersion": "14393.6085.230705",
    #                 },
    #                 "osDisk": {
    #                     "osType": "Windows",
    #                     "createOption": "FromImage",
    #                     "caching": "ReadWrite",
    #                     "managedDisk": {"storageAccountType": "Premium_LRS"},
    #                     "deleteOption": "Detach",
    #                     "diskSizeGB": 127,
    #                 },
    #                 "dataDisks": [],
    #             },
    #             "osProfile": {
    #                 "adminUsername": "AdminTestTestTest",
    #                 "computerName": "ABC",
    #                 "adminPassword": "<password>",
    #             },
    #             "networkProfile": {
    #                 "networkInterfaces": [
    #                     {
    #                         "id": "/subscriptions/9de1303d-cac3-4232-9269-a7109121f58f/resourceGroups/amduatest/providers/Microsoft.Network/networkInterfaces/test123577_z1",
    #                         "properties": {"deleteOption": "Detach"},
    #                     }
    #                 ]
    #             },
    #         },
    #     },
    #     headers={
    #         "x-ms-operation-identifier": f"lightning={client.session_id}/{uuid.uuid4()}"
    #     },
    # ))
    
    # for lro in lros:
    #     print(lro.result())


    import azure.mgmt.storage
    from azure.mgmt.storage.models import StorageAccountCreateParameters

    client = azure.mgmt.storage.StorageManagementClient(DefaultAzureCredential(), subscription_id, session=session_id, base_url=base_url)
    parameters = StorageAccountCreateParameters(sku={'name': 'Premium_LRS'}, kind='Storage', location='centralus')

    lro = client.storage_accounts.begin_create('amduatest', f'amduatestlistenerdemo', parameters=parameters, headers={
            "x-ms-operation-identifier": f"lightning={client.session_id}/{uuid.uuid4()}"
        })

    print(lro.result())
