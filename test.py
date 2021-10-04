# from azure.batch import BatchServiceClient
# from azure.batch import batch_auth
# from azure.batch.batch_auth import SharedKeyCredentials
# from azure.identity import DefaultAzureCredential
# from azure.mgmt.batch import BatchManagementClient
# from azure.mgmt.resource import SubscriptionClient, ResourceManagementClient
# from azure.mgmt.storage import StorageManagementClient
# from azure.storage.blob import BlobServiceClient


# credential = DefaultAzureCredential()

# sub_cl = SubscriptionClient(credential)
# subscription = list(sub_cl.subscriptions.list())[0]
# print('Subscription', subscription.display_name)

# res_cl = ResourceManagementClient(credential, subscription.subscription_id)
# res_grp = list(res_cl.resource_groups.list())[1]
# print('Resource group', res_grp.name)

# resources = list(res_cl.resources.list_by_resource_group(res_grp.name))
# batch_res = [r for r in resources if r.type == 'Microsoft.Batch/batchAccounts'][0]
# storage_res = [r for r in resources if r.type == 'Microsoft.Storage/storageAccounts'][0]

# print('Batch account', batch_res.name)
# print('Storage account', storage_res.name)

# st_cl = StorageManagementClient(credential, subscription.subscription_id)
# st_keys = st_cl.storage_accounts.list_keys(res_grp.name, storage_res.name)
# st_key = st_keys.keys[0].value
# blob_cl = BlobServiceClient(account_url=f'https://{storage_res.name}.blob.core.windows.net', credential=st_key)

# bt_cl = BatchManagementClient(credential, subscription.subscription_id)
# bt_keys = bt_cl.batch_account.get_keys(res_grp.name, batch_res.name)
# bt_cred = SharedKeyCredentials(batch_res.name, bt_keys.primary)

# batch_cl = BatchServiceClient(bt_cred, batch_url=f'https://{batch_res.name}.{batch_res.location}.batch.azure.com')
# pools = list(batch_cl.pool.list())


from grevling import util

util.initialize_logging()

def zoopy():
    with util.log.with_context('omfglol'):
        hoopy('alpha')
    hoopy('bravo')

@util.with_context('{zomg}')
def hoopy(zomg):
    util.log.info('hi there')

for _ in range(100):
    zoopy()
