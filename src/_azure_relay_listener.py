import json
import time
from typing import Any, Optional, Tuple, cast
from urllib.parse import urlparse

from azure.core.utils import \
    parse_connection_string as core_parse_connection_string
from websocket import create_connection

from utils import create_listener_url, generate_sas_token

import logging
LOG = logging.getLogger(__name__)


class HybridConnectionListener():
    def __init__(self, fully_qualified_name: str, entity_path: str, *, sas_token:str, **kwargs: Any) -> None:
        self.listener_url = create_listener_url(fully_qualified_name, entity_path, sas_token)

    @classmethod
    def from_connection_string(cls, conn_str: str, **kwargs):
        host, policy, key, entity, token, token_expiry = cls._parse_conn_str(conn_str, **kwargs)
        token = generate_sas_token(host, entity, policy, key)
        return cls(host, entity, sas_token = token, **kwargs)
    
    def _open(self):
        try:
            self.control_conn = create_connection(self.listener_url)
            LOG.debug(f"Connected to control websocket {self.listener_url}")
        except Exception as e:
            raise Exception(f"Failed to connect to {self.listener_url}") from e

    
    def receive(self, on_message_received, **kwargs):
        self._open()
        request = self.control_conn.recv()
        command = json.loads(request)

        if 'accept' in command:
            LOG.debug(f"Received accept from control websocket")
            request = command['accept']
            try:
                rendezvous_conn = create_connection(request['address'])
                LOG.debug(f"Connected to rendezvous websocket {request['address']}")
            except Exception as e:
                raise Exception(f"Failed to connect to {request['address']}") from e
            
            while True:
                LOG.debug(f"Waiting for data from rendezvous websocket")
                data = rendezvous_conn.recv()

                if data:
                    LOG.debug(f"Received data from rendezvous websocket")
                    print(data.decode('utf-8'))

                response = {
                    'requestId': request['id'],
                    'statusCode': '200',
                    'responseHeaders': {
                        'content-type': 'text/html',
                    }
                }

                response_str = json.dumps({'response':response})

                rendezvous_conn.send(response_str)
                LOG.debug(f"Sent response ack to rendezvous websocket")
        

    @staticmethod
    def _parse_conn_str(conn_str: str, **kwargs: Any) -> Tuple[str, Optional[str], Optional[str], str, Optional[str], Optional[int]]:
        endpoint = None
        shared_access_key_name = None
        shared_access_key = None
        entity_path = None
        shared_access_signature = None
        shared_access_signature_expiry = None
        check_case = kwargs.pop("check_case", False)
        conn_settings = core_parse_connection_string(
            conn_str, case_sensitive_keys=check_case
        )
        if check_case:
            shared_access_key = conn_settings.get("SharedAccessKey")
            shared_access_key_name = conn_settings.get("SharedAccessKeyName")
            endpoint = conn_settings.get("Endpoint")
            entity_path = conn_settings.get("EntityPath")
            # non case sensitive check when parsing connection string for internal use
            for key, value in conn_settings.items():
                # only sas check is non case sensitive for both conn str properties and internal use
                if key.lower() == "sharedaccesssignature":
                    shared_access_signature = value

        if not check_case:
            endpoint = conn_settings.get("endpoint") or conn_settings.get("hostname")
            if endpoint:
                endpoint = endpoint.rstrip("/")
            shared_access_key_name = conn_settings.get("sharedaccesskeyname")
            shared_access_key = conn_settings.get("sharedaccesskey")
            entity_path = conn_settings.get("entitypath")
            shared_access_signature = conn_settings.get("sharedaccesssignature")

        if shared_access_signature:
            try:
                # Expiry can be stored in the "se=<timestamp>" clause of the token. ('&'-separated key-value pairs)
                shared_access_signature_expiry = int(
                    shared_access_signature.split("se=")[1].split("&")[0]  # type: ignore
                )
            except (
                IndexError,
                TypeError,
                ValueError,
            ):  # Fallback since technically expiry is optional.
                # An arbitrary, absurdly large number, since you can't renew.
                shared_access_signature_expiry = int(time.time() * 2)

        entity = entity_path

        # check that endpoint is valid
        if not endpoint:
            raise ValueError("Connection string is either blank or malformed.")
        parsed = urlparse(endpoint)
        if not parsed.netloc:
            raise ValueError("Invalid Endpoint on the Connection String.")
        host = cast(str, parsed.netloc.strip())

        if any([shared_access_key, shared_access_key_name]) and not all(
            [shared_access_key, shared_access_key_name]
        ):
            raise ValueError(
                "Invalid connection string. Should be in the format: "
                "Endpoint=sb://<FQDN>/;SharedAccessKeyName=<KeyName>;SharedAccessKey=<KeyValue>"
            )
        # Only connection string parser should check that only one of sas and shared access
        # key exists. For backwards compatibility, client construction should not have this check.
        if check_case and shared_access_signature and shared_access_key:
            raise ValueError(
                "Only one of the SharedAccessKey or SharedAccessSignature must be present."
            )
        if not shared_access_signature and not shared_access_key:
            raise ValueError(
                "At least one of the SharedAccessKey or SharedAccessSignature must be present."
            )

        return (
            host,
            str(shared_access_key_name) if shared_access_key_name else None,
            str(shared_access_key) if shared_access_key else None,
            entity,
            str(shared_access_signature) if shared_access_signature else None,
            shared_access_signature_expiry,
        )