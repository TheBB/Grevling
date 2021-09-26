from operator import attrgetter
import re
import string

import inquirer

from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from azure.batch.models import VirtualMachineConfiguration
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import DefaultAzureCredential
from azure.mgmt.batch import BatchManagementClient
from azure.mgmt.compute import ComputeManagementClient
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


def vm_size_disp(size):
    cpus = next(c for c in size.capabilities if c.name == 'vCPUs').value
    memory = next(c for c in size.capabilities if c.name == 'MemoryGB').value
    return f'{size.name} ({cpus} vCPUs, {memory} GB)'


def get_one(choices, prompt, error, namer=(lambda x: x), special=None):
    answers = [namer(c) for c in choices]
    if special:
        answers.append(special)

    if len(answers) == 0:
        util.log.error(error)
        return None
    if len(answers) == 1:
        answer = answers[0]
    else:
        question = inquirer.List('x', message=prompt, choices=answers)
        answer = inquirer.prompt([question])
        if not answer:
            return None
        answer = answer['x']
    if answer == special:
        return special
    return next(c for c in choices if namer(c) == answer)


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

    util.log.info(f"Using storage account '{acct_name}'")
    return BlobServiceClient(account_url='https://{acct_name}.blob.core.windows.net', credential=key)


def get_batch_client(cred, sub, batch):
    try:
        res_grp = re.match(r'.*/resourceGroups/(?P<resgrp>[^/]*)', batch.id).groupdict()['resgrp']
    except:
        util.log.error(f"Batch account {batch.name} does not appear to have an associated resource group, or its ID was not understood")
        return None

    client = BatchManagementClient(cred, sub)
    key = client.batch_account.get_keys(res_grp, batch.name).primary
    scred = SharedKeyCredentials(batch.name, key)
    return BatchServiceClient(scred, batch_url=f'https://{batch.name}.{batch.location}.batch.azure.com')


def get_pool_settings(cred, sub, acct, batch):
    util.log.info("Creating a new batch pool")
    client = ComputeManagementClient(cred, sub)
    images = list(batch.account.list_supported_images())

    publishers = sorted(set(i.image_reference.publisher for i in images))
    publisher = get_one(publishers, "Select a VM image publisher", "Unable to find a suitable VM image publisher")
    if not publisher:
        return None
    images = [i for i in images if i.image_reference.publisher == publisher]

    offers = sorted(set(i.image_reference.offer for i in images))
    offer = get_one(offers, "Select a VM image offer", "Unable to find a suitable VM image offer")
    if not offer:
        return None
    images = [i for i in images if i.image_reference.offer == offer]

    skus = sorted(set(i.image_reference.sku for i in images))
    sku = get_one(skus, "Select an SKU", "Unable to find a suitable SKU")
    if not sku:
        return None
    images = [i for i in images if i.image_reference.sku == sku]

    versions = sorted(set(i.image_reference.version for i in images))
    version = get_one(versions, "Select a version", "Unable to find a suitable version")
    if not version:
        return None
    images = [i for i in images if i.image_reference.version == version]
    image = images[0]

    families = [f for f in acct.dedicated_core_quota_per_vm_family if f.core_quota > 0]
    family = get_one(families, "Select a VM family", "Unable to find a VM family with nonzero quota", attrgetter('name'))
    if not family:
        return None

    sizes = [
        s for s in client.resource_skus.list(filter=f"location eq '{acct.location}'")
        if s.resource_type == 'virtualMachines' and s.family == family.name
    ]
    size = get_one(sizes, "Select a VM size", "Unable to find a suitable VM size", vm_size_disp)
    if not size:
        return None

    util.log.info(f"Using VM size {vm_size_disp(size)}\n")

    question = inquirer.Text(
        'number', f"Select number of nodes (1-{family.core_quota})",
        validate=lambda _,x: x.isnumeric() and 1 <= int(x) <= family.core_quota
    )
    try:
        nnodes = int(inquirer.prompt([question])['number'])
    except:
        return None

    return {
        'publisher': image.image_reference.publisher,
        'offer': image.image_reference.offer,
        'sku': image.image_reference.sku,
        'version': image.image_reference.version,
        'sku_id': image.node_agent_sku_id,
        'size': size.name,
        'nnodes': nnodes,
    }


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

        pool = get_pool_settings(credential, subscription.subscription_id, batch_account, self.batch_client)
        print(pool)

        self.ready = True
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def pipeline(self):
        return None
