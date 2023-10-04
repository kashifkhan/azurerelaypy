import logging
import os
from datetime import datetime

from azure.identity import DefaultAzureCredential
from azure.core.polling.base_polling import LROBasePolling
from azure.mgmt.core.polling.arm_polling import ARMPolling
from dotenv import find_dotenv, load_dotenv

logging.basicConfig(level=logging.DEBUG)

load_dotenv(find_dotenv())

subscription_id = os.environ['SUBSCRIPTION_ID']
base_url = os.environ['BASE_URL']
session_id = os.environ['SESSION_ID']
resource_group_name = os.environ['RESOURCE_GROUP']
storage_account = os.environ['STORAGE_ACCOUNT']



class RelayArmPollerMixin(LROBasePolling):
    def _sleep(self, delay):
        delay = 40
        super()._sleep(delay)

ARMPolling.__bases__ = (RelayArmPollerMixin,)

from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.storage.models import StorageAccountCreateParameters


client = StorageManagementClient(DefaultAzureCredential(), subscription_id, base_url=base_url, logging_enable=True)

#parameters = StorageAccountCreateParameters(sku={'name': 'Premium_LRS'}, kind='Storage', location='centraluseuap')
parameters = StorageAccountCreateParameters(sku={'name': 'Premium_LRS'}, kind='Storage', location='centralus')

time_started = datetime.now()

lros = []
for i in range(4,5):
    lros.append(client.storage_accounts.begin_create(resource_group_name, f'{storage_account}{i}', parameters=parameters,))

for lro in lros:
    print(lro.result())


print(f'total time {(datetime.now() - time_started).seconds} seconds')
