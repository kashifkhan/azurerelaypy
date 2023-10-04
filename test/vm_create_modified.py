import logging
import os
from datetime import datetime, timezone
import uuid

from azure.identity import DefaultAzureCredential
from dotenv import find_dotenv, load_dotenv

from listenerdemo import apply_dark_magic

logging.basicConfig(level=logging.DEBUG)

load_dotenv(find_dotenv())


if __name__ == "__main__":
    apply_dark_magic()
    import azure.mgmt.compute
    subscription_id = os.environ["SUBSCRIPTION_ID"]

    base_url = os.environ["BASE_URL"]

    session_id = os.environ["SESSION_ID"]

    client = azure.mgmt.compute.ComputeManagementClient(
        DefaultAzureCredential(), subscription_id, session=session_id, base_url=base_url, logging_enable=True
    )

    client._client.time_sent = datetime.now(timezone.utc)

    lros = []

    for i in range(1):
        lros.append(
            client.virtual_machines.begin_create_or_update(
                resource_group_name="amduatest",
                vm_name="lightningDemo",
                parameters={
                    "location": "centraluseuap",
                    "properties": {
                        "hardwareProfile": {"vmSize": "Standard_DS1_v2"},
                        "storageProfile": {
                            "imageReference": {
                                "publisher": "MicrosoftWindowsServer",
                                "offer": "WindowsServer",
                                "sku": "2016-datacenter-gensecond",
                                "version": "latest",
                                "exactVersion": "14393.6085.230705",
                            },
                            "osDisk": {
                                "osType": "Windows",
                                "createOption": "FromImage",
                                "caching": "ReadWrite",
                                "managedDisk": {"storageAccountType": "Premium_LRS"},
                                "deleteOption": "Detach",
                                "diskSizeGB": 127,
                            },
                            "dataDisks": [],
                        },
                        "osProfile": {
                            "adminUsername": "AdminTestTestTest",
                            "computerName": "ABC",
                            "adminPassword": "somethingLightning@8273",
                        },
                        "networkProfile": {
                            "networkInterfaces": [
                                {
                                    "id": "/subscriptions/9de1303d-cac3-4232-9269-a7109121f58f/resourceGroups/amduatest/providers/Microsoft.Network/networkInterfaces/lightningDemoCanary",
                                    "properties": {"deleteOption": "Detach"},
                                }
                            ]
                        },
                    },
                },
                headers={"x-ms-operation-identifier": f"lightning={client.session_id}/{uuid.uuid4()}"},
            )
        )

    print(f"Issued PUT for virtual machine created at {datetime.now(timezone.utc)}")

    for lro in lros:
        print(lro.result())
    
    print(f"total time {(datetime.now(timezone.utc) - client._client.time_sent).seconds} seconds")
    print(f"total time from event time {(client._client.event_time - client._client.time_sent).seconds} seconds")
    