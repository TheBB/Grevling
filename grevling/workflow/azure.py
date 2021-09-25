from operator import attrgetter
import re

import inquirer

from azure.batch import BatchServiceClient
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import DefaultAzureCredential
from azure.mgmt.batch import BatchManagementClient
from azure.mgmt.resource import SubscriptionClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient

from .. import api, util


az_loggers = [
    'azure.core.pipeline.policies.http_logging_policy',
    'azure.identity._credentials.chained',
    'azure.identity._credentials.environment',
    'azure.identity._credentials.imds',
    'azure.identity._credentials.managed_identity',
    'azure.identity._internal.decorators',
    'azure.identity._internal.get_token_mixin',
    'azure.identity._credentials.default',
]


def get_one(choices, prompt, error, namer):
    if len(choices) == 0:
        util.log.error(error)
        return None
    if len(choices) == 1:
        return choices[0]
    question = inquirer.List(
        'x', message=prompt,
        choices=[namer(c) for c in choices]
    )
    answer = inquirer.prompt([question])
    if not answer:
        return None
    name = answer['x']
    return next(c for c in choices if namer(c) == name)


def get_credential():
    credential = DefaultAzureCredential()
    try:
        credential.get_token('https://management.azure.com/.default')
    except ClientAuthenticationError:
        util.log.error("Unable to authenticate with Azure")
        util.log.error("Please verify that you have enabled an authentication method as described:")
        util.log.error("https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential")
        return None
    return credential


def get_subscription(cred):
    client = SubscriptionClient(cred)
    subs = list(client.subscriptions.list())
    return get_one(
        subs, "Found multiple Azure subscriptions",
        "Successfully authenticated with Azure, but no subscriptions were found",
        attrgetter('display_name')
    )


def get_batch_account(cred, sub):
    client = BatchManagementClient(cred, sub.subscription_id)
    accounts = list(client.batch_account.list())
    return get_one(
        accounts, "Found multiple batch accounts",
        "Successfully accessed Azure subscription, but no batch accounts were found",
        attrgetter('name')
    )


def get_attached_storage(cred, sub, batch):
    try:
        storage_id = batch.auto_storage.storage_account_id
        res_grp = re.match(r'.*/resourceGroups/(?P<resgrp>[^/]*)', storage_id).groupdict()['resgrp']
        acct_name = re.match(r'.*/storageAccounts/(?P<acct>[^/]*)', storage_id).groupdict()['acct']
    except:
        util.log.error(f"Batch account {batch.name} does not appear to have an associated storage account, or its ID was not understood")
        return None

    client = StorageManagementClient(cred, sub)

    try:
        keys = client.storage_accounts.list_keys(res_grp, acct_name)
        assert keys.keys
        key = keys.keys[0].value
    except:
        util.log.error(f"Could not retrieve access keys to associated storage account")
        return None

    util.log.info(f"Using associated storage account '{acct_name}'")
    return BlobServiceClient(account_url='https://{acct_name}.blob.core.windows.net', credential=key)


def get_batch_client(cred, sub, batch):
    try:
        res_grp = re.match(r'.*/resourceGroups/(?P<resgrp>[^/]*)', batch.id).groupdict()['resgrp']
    except:
        util.log.error(f"Batch account {batch.name} does not appear to have an associated resource group, or its ID was not understood")
        return None

    client = BatchManagementClient(cred, sub)
    key = client.batch_account.get_keys(res_grp, batch.name).primary
    return BatchServiceClient(key, batch_url=f'https://{batch.name}.{batch.location}.batch.azure.com')


class AzureWorkflow(api.Workflow):

    name = 'azure'

    credentials: DefaultAzureCredential

    @classmethod
    def init(cls):
        import logging
        if logging.root.level > logging.DEBUG:
            for name in az_loggers:
                logging.getLogger(name).setLevel(logging.ERROR)

    def __enter__(self):
        self.ready = False

        credential = get_credential()
        if not credential:
            return self

        subscription = get_subscription(credential)
        if not subscription:
            return self
        util.log.info(f"Using Azure subscription '{subscription.display_name}'")

        batch_account = get_batch_account(credential, subscription)
        if not batch_account:
            return self
        util.log.info(f"Using batch account '{batch_account.name}'")

        self.blob_client = get_attached_storage(credential, subscription.subscription_id, batch_account)
        self.batch_client = get_batch_client(credential, subscription.subscription_id, batch_account)

        self.ready = True
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def pipeline(self):
        return None
