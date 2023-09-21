from azure.identity import DefaultAzureCredential
import azure.mgmt.storage
import os

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

subscription_id = os.environ['SUBSCRIPTION_ID']
base_url = os.environ['BASE_URL']
session_id = os.environ['SESSION_ID']
resource_group_name = os.environ['RESOURCE_GROUP']
storage_account = os.environ['STORAGE_ACCOUNT']

client = azure.mgmt.storage.StorageManagementClient(DefaultAzureCredential(), subscription_id, base_url=base_url)
for i in range(20):
    client.storage_accounts.delete(resource_group_name, f'{storage_account}{i}')
