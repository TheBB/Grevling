from contextlib import contextmanager
from io import IOBase, BytesIO
from operator import attrgetter
from pathlib import Path
import re

from typing import Iterable, ContextManager, Union

import inquirer

from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
from azure.batch.models import (
    VirtualMachineConfiguration, PoolAddParameter, ImageReference, JobAddParameter,
    PoolInformation, TaskAddParameter, ResourceFile, TaskState, BatchErrorException
)
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import DefaultAzureCredential
from azure.mgmt.batch import BatchManagementClient
from azure.mgmt.batch.models import BatchAccount
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import SubscriptionClient
from azure.mgmt.resource.subscriptions.models import Subscription
from azure.mgmt.storage import StorageManagementClient
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobPrefix

from .. import api, util
from . import PrepareInstance, DownloadResults, Pipeline, PipeSegment, PipeFilter


az_loggers = [
    'azure.core.pipeline.policies.http_logging_policy',
    'azure.identity._credentials.chained',
    'azure.identity._credentials.environment',
    'azure.identity._credentials.imds',
    'azure.identity._credentials.managed_identity',
    'azure.identity._internal.decorators',
    'azure.identity._internal.get_token_mixin',
    'azure.identity._internal.linux_vscode_adapter',
    'azure.identity._credentials.default',
    'msrest.async_paging',
    'msrest.service_client',
    'msrest.universal_http',
    'urllib3.connectionpool',
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
        return None, None

    client = StorageManagementClient(cred, sub.subscription_id)

    try:
        keys = client.storage_accounts.list_keys(res_grp, acct_name)
        assert keys.keys
        key = keys.keys[0].value
    except:
        util.log.error(f"Could not retrieve access keys to associated storage account")
        return None, None

    util.log.info(f"Using storage account '{acct_name}'")
    client = BlobServiceClient(account_url=f'https://{acct_name}.blob.core.windows.net', credential=key)
    return client.get_container_client('grevling')


def get_batch_client(cred, sub, batch):
    try:
        res_grp = re.match(r'.*/resourceGroups/(?P<resgrp>[^/]*)', batch.id).groupdict()['resgrp']
    except:
        util.log.error(f"Batch account {batch.name} does not appear to have an associated resource group, or its ID was not understood")
        return None

    client = BatchManagementClient(cred, sub.subscription_id)
    key = client.batch_account.get_keys(res_grp, batch.name).primary
    scred = SharedKeyCredentials(batch.name, key)
    return BatchServiceClient(scred, batch_url=f'https://{batch.name}.{batch.location}.batch.azure.com')


def get_pool_settings(cred, sub, acct, batch):
    util.log.info("Creating a new batch pool")
    client = ComputeManagementClient(cred, sub.subscription_id)
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


class AzureStorageWorkspace(api.Workspace):

    client: ContainerClient
    root: Path

    def __init__(self, client: ContainerClient, root: Path, name: str = ''):
        self.client = client
        self.root = root
        self.name = name

    def __str__(self):
        return f'{self.client.container_name}/{self.root}'

    def destroy(self):
        assert False

    def open_file(self, path: api.PathStr, mode: str = 'w') -> ContextManager[IOBase]:
        assert False

    def write_file(self, path: api.PathStr, source: Union[str, bytes, IOBase, Path]):
        self.client.upload_blob(str(self.root / path), source, overwrite=True)

    def read_file(self, path: api.PathStr) -> ContextManager[IOBase]:
        assert False

    def files(self) -> Iterable[Path]:
        for f in self.client.list_blobs(name_starts_with=str(self.root)):
            yield Path(f['name']).relative_to(self.root)

    def exists(self, path: api.PathStr) -> bool:
        assert False

    def subspace(self, path: str, name: str = '') -> 'AzureStorageWorkspace':
        name = name or str(path)
        return AzureStorageWorkspace(self.client, self.root / path, name=f'{self.name}/{name}')

    def top_name(self) -> str:
        assert False


class AzureStorageWorkspaceCollection(api.WorkspaceCollection):

    client: ContainerClient

    def __init__(self, client: ContainerClient, name: str = ''):
        self.client = client
        self.name = name

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def workspace_names(self) -> Iterable[str]:
        for blob in self.client.walk_blobs():
            if isinstance(blob, BlobPrefix):
                yield blob.name

    def open_workspace(self, path: str, name: str = '') -> AzureStorageWorkspace:
        return AzureStorageWorkspace(self.client, Path(path), name)


class AzureBatchWorkspace(api.Workspace):

    client: BatchServiceClient
    task_id: str
    root: Path

    def __init__(self, client: ContainerClient, task_id: str, root: Path, name: str = ''):
        self.client = client
        self.task_id = task_id
        self.root = root
        self.name = name

    def __str__(self):
        return f'/{self.root}'

    def destroy(self):
        assert False

    def open_file(self, path: api.PathStr, mode: str = 'w') -> ContextManager[IOBase]:
        assert False

    def write_file(self, path: api.PathStr, source: Union[str, bytes, IOBase, Path]):
        assert False

    # TODO: Must be possible not to download the whole file at once here
    @contextmanager
    def read_file(self, path: api.PathStr) -> ContextManager[IOBase]:
        path = self.root / path
        stream = self.client.file.get_from_task('grevling', self.task_id, str(path))
        output = BytesIO()
        for data in stream:
            output.write(data)
        yield BytesIO(output.getvalue())

    def files(self) -> Iterable[Path]:
        for file in self.client.file.list_from_task('grevling', self.task_id, recursive=True):
            if file.is_directory:
                continue
            pfile = Path(file.name)
            if self.root not in pfile.parents:
                continue
            yield pfile.relative_to(self.root)

    def exists(self, path: api.PathStr) -> bool:
        path = str(self.root / path)
        try:
            self.client.file.get_properties_from_task('grevling', self.task_id, path)
        except BatchErrorException:
            return False
        return True

    def subspace(self, path: str, name: str = '') -> 'AzureStorageWorkspace':
        name = name or str(path)
        return AzureBatchWorkspace(self.client, self.task_id, self.root / path, name=f'{self.name}/name')

    def top_name(self) -> str:
        assert False


class AzureBatchWorkspaceCollection(api.WorkspaceCollection):

    client: BatchServiceClient

    def __init__(self, client: ContainerClient, name: str = ''):
        self.client = client
        self.name = name

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def workspace_names(self) -> Iterable[str]:
        for task in self.client.task.list(job_id='grevling'):
            yield task.id

    def open_workspace(self, path: str, name: str = '') -> AzureBatchWorkspace:
        root = Path('wd') / path
        return AzureBatchWorkspace(self.client, task_id=path, root=root, name=name)


class RunInstance(PipeSegment):

    def __init__(self, workflow: 'AzureWorkflow', workspaces):
        self.workflow = workflow
        self.workspaces = workspaces

    @util.with_context('I {instance.index}')
    @util.with_context('Run')
    async def apply(self, instance):
        with instance.bind_remote(self.workspaces):
            instance.upload_script()
        util.log.info(f'Started {instance.logdir}')
        self.workflow.batch_client.task.add('grevling', TaskAddParameter(
            id=instance.logdir,
            command_line=f"/bin/bash -c 'cd {instance.logdir} && bash .grevling/grevling.sh'",
            resource_files=[ResourceFile(
                auto_storage_container_name='grevling',
                blob_prefix=instance.logdir,
            )],
        ))
        return instance


class PollInstances(PipeFilter):

    def __init__(self, workflow: 'AzureWorkflow'):
        self.workflow = workflow

    async def apply(self, instance):
        task = self.workflow.batch_client.task.get('grevling', instance.logdir)
        if task is None:
            return None
        if task.state == TaskState.completed:
            instance.status = api.Status.Finished
        return task.state == TaskState.completed


class AzureWorkflow(api.Workflow):

    name = 'azure'

    credential: DefaultAzureCredential
    subscription: Subscription

    batch_account: BatchAccount

    batch_client: BatchServiceClient
    container_client: ContainerClient

    out_workspaces: AzureStorageWorkspaceCollection
    in_workspaces: AzureBatchWorkspaceCollection

    pool_started: bool
    job_started: bool

    @classmethod
    def init(cls):
        import logging
        for name in az_loggers:
            logging.getLogger(name).setLevel(logging.ERROR)

    def __init__(self, **_):
        pass

    def init_storage(self) -> bool:
        credential = get_credential()
        if not credential:
            return False
        self.credential = credential

        subscription = get_subscription(credential)
        if not subscription:
            return False
        util.log.info(f"Using Azure subscription '{subscription.display_name}'")
        self.subscription = subscription

        batch_account = get_batch_account(self.credential, self.subscription)
        if not batch_account:
            return False
        util.log.info(f"Using batch account '{batch_account.name}'")
        self.batch_account = batch_account

        container_client = get_attached_storage(self.credential, self.subscription, batch_account)
        if container_client is None:
            return False
        self.container_client = container_client
        self.out_workspaces = AzureStorageWorkspaceCollection(container_client)

        batch_client = get_batch_client(self.credential, self.subscription, self.batch_account)
        if not batch_client:
            return False
        self.batch_client = batch_client
        self.in_workspaces = AzureBatchWorkspaceCollection(batch_client)

        return True

    def init_pool(self) -> bool:
        pool_cfg = get_pool_settings(self.credential, self.subscription, self.batch_account, self.batch_client)
        if not pool_cfg:
            return False

        pool = PoolAddParameter(
            id='grevling',
            virtual_machine_configuration=VirtualMachineConfiguration(
                image_reference=ImageReference(
                    publisher=pool_cfg['publisher'],
                    offer=pool_cfg['offer'],
                    sku=pool_cfg['sku'],
                    version=pool_cfg['version'],
                ),
                node_agent_sku_id=pool_cfg['sku_id'],
            ),
            vm_size=pool_cfg['size'],
            target_dedicated_nodes=pool_cfg['nnodes'],
        )
        self.batch_client.pool.add(pool)
        self.pool_started = True

        job = JobAddParameter(id='grevling', pool_info=PoolInformation(pool_id='grevling'))
        self.batch_client.job.add(job)
        self.job_started = True

    def __enter__(self):
        self.pool_started = False
        self.job_started = False
        self.ready = False

        if not self.init_storage():
            return self

        self.ready = True
        return self

    def __exit__(self, *args, **kwargs):
        # if self.job_started:
            # self.batch_client.job.delete('grevling')
        # if self.pool_started:
            # self.batch_client.pool.delete('grevling')
        pass

    def pipeline(self):
        self.init_pool()
        return Pipeline(
            PrepareInstance(self.out_workspaces),
            RunInstance(self, self.out_workspaces),
            PollInstances(self),
            DownloadResults(self.in_workspaces),
        )
