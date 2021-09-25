import inquirer

from azure.core.exceptions import ClientAuthenticationError
from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import SubscriptionClient

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
        self.credentials = DefaultAzureCredential()

        try:
            self.credentials.get_token('https://management.azure.com/.default')
        except ClientAuthenticationError:
            util.log.error("Unable to authenticate with Azure")
            util.log.error("Please verify that you have enabled an authentication method as described:")
            util.log.error("https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential")
            return self

        subscriptions = SubscriptionClient(self.credentials)
        subs = list(subscriptions.subscriptions.list())

        if len(subs) == 0:
            util.log.error("Successfully authenticated with Azure, but no subscriptions were found")
            return self
        elif len(subs) == 1:
            sub = subs[0]
        else:
            question = inquirer.List(
                'sub', message="Found multiple Azure subscriptions",
                choices=[s.display_name for s in subs]
            )
            answer = inquirer.prompt([question])['sub']
            sub = next(s for s in subs if s.display_name == answer)

        util.log.info(f"Using Azure subscription {sub.display_name}")

        self.ready = True
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def pipeline(self):
        return None
