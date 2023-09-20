from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv, find_dotenv
import os
import azure.mgmt.storage
from azure.mgmt.storage.models import StorageAccountCreateParameters
from datetime import datetime

# import logging
# import sys
# logging.basicConfig(level=logging.DEBUG)

# logger = logging.getLogger('azure')
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler(stream=sys.stdout)


load_dotenv(find_dotenv())

subscription_id = os.environ['SUBSCRIPTION_ID']
base_url = os.environ['BASE_URL']
session_id = os.environ['SESSION_ID']
resource_group_name = os.environ['RESOURCE_GROUP']
storage_account = os.environ['STORAGE_ACCOUNT']

client = azure.mgmt.storage.StorageManagementClient(DefaultAzureCredential(), subscription_id, base_url=base_url)

parameters = StorageAccountCreateParameters(sku={'name': 'Premium_LRS'}, kind='Storage', location='centralus')

time_started = datetime.now()

lros = []
for i in range(20):
    lros.append(client.storage_accounts.begin_create(resource_group_name, f'{storage_account}{i}', parameters=parameters,))

for lro in lros:
    print(lro.result())


print(f'total time {(datetime.now() - time_started).seconds} seconds')

