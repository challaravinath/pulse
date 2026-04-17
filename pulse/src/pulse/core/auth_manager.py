"""Auth Manager - All 3 auth methods working"""
from dotenv import load_dotenv
load_dotenv()

import os
import logging
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.identity import AzureCliCredential, DefaultAzureCredential, ClientSecretCredential
from .config_loader import AuthMethod  # Import instead of redefining!

logger = logging.getLogger(__name__)

class AuthenticationError(Exception):
    pass

class KustoAuthManager:
    @staticmethod
    def create_client(cluster_url, auth_method, client_id=None, client_secret=None, tenant_id=None):
        try:
            if auth_method == AuthMethod.AZURE_CLI:
                # Use with_azure_token_credential so the live credential object
                # handles token refresh automatically — avoids ~1hr disconnect.
                # with_az_cli_authentication uses a static token snapshot.
                credential = AzureCliCredential()
                credential.get_token("https://kusto.kusto.windows.net/.default")
                kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
                    cluster_url, credential
                )
                return KustoClient(kcsb)
            
            elif auth_method == AuthMethod.MANAGED_IDENTITY:
                credential = DefaultAzureCredential()
                credential.get_token("https://kusto.kusto.windows.net/.default")
                kcsb = KustoConnectionStringBuilder.with_azure_token_credential(cluster_url, credential)
                return KustoClient(kcsb)
            
            elif auth_method == AuthMethod.SERVICE_PRINCIPAL:
                if not all([client_id, client_secret, tenant_id]):
                    raise AuthenticationError("Service Principal requires client_id, client_secret, tenant_id")
                credential = ClientSecretCredential(tenant_id, client_id, client_secret)
                credential.get_token("https://kusto.kusto.windows.net/.default")
                kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
                    cluster_url, client_id, client_secret, tenant_id
                )
                return KustoClient(kcsb)
            
            else:
                raise AuthenticationError(f"Unknown auth method: {auth_method}")
        
        except AuthenticationError:
            raise
        except Exception as e:
            raise AuthenticationError(f"Auth failed: {str(e)}")
