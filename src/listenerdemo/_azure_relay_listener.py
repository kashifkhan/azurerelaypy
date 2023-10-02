from datetime import datetime, timezone
import json
import threading
import time
from typing import Any, MutableMapping, Optional, Tuple, cast, Callable
from urllib.parse import urlparse
from azure.core.pipeline import PipelineResponse

from azure.core.utils import \
    parse_connection_string as core_parse_connection_string
from websocket import create_connection

from .utils import create_listener_url, generate_sas_token

from azure.core.polling.base_polling import LongRunningOperation, OperationFailed
from azure.core.polling import PollingMethod
JSON = MutableMapping[str, Any]

import logging
LOG = logging.getLogger(__name__)


class RelayPollingStrategy(LongRunningOperation):
    def can_poll(self, pipeline_response: PipelineResponse) -> bool:
        return True
    
    def get_polling_url(self) -> str:
        raise NotImplementedError("The polling strategy does not need to extract a polling URL.")
    
    def set_initial_status(self, pipeline_response: str) -> str:
        response = pipeline_response
        if response == "200":
            return "InProgress"
        raise OperationFailed("Operation failed or cancelled.")
    
    def get_status(self, response: JSON) -> str:
        if response is None:
            return "InProgress"
        return "Succeeded"
    
    def get_final_get_url(self, pipeline_response: PipelineResponse) -> Optional[str]:
        return None
    

class RelayPollingMethod(PollingMethod):
    def __init__(self, fully_qualified_name: str, entity_path: str, sas_token:str, **kwargs: Any) -> None:
        self.listener_url = create_listener_url(fully_qualified_name, entity_path, sas_token)
        self.ping_interval = 20
        self.ping_thread = None
        self.send_ping = True
        self.stop_event = threading.Event()
    
    def _send_ping(self):
        while self.send_ping:
            try:
                self.control_conn.ping()
                LOG.debug(f"Sent ping to control websocket {self.listener_url}")
            except Exception as e:
                self.control_conn.close()
                raise Exception(f"Failed to send ping to {self.listener_url}") from e
            LOG.debug("Sleeping")
            self.stop_event.wait(self.ping_interval)
        #LOG.debug("send ping is now false")

    def initialize(self, client: Any, initial_response: Any, deserialization_callback: Callable) -> None:
        try:
            self.control_conn = create_connection(self.listener_url)
            self.rendezvous_conn = None
            LOG.debug(f"Connected to control websocket {self.listener_url}")
            self.ping_thread = threading.Thread(target=self._send_ping, daemon=True)
            self.ping_thread.start()
        except Exception as e:
            raise Exception(f"Failed to connect to {self.listener_url}") from e
        
        self._initial_response = initial_response
        self._deserialization_callback = deserialization_callback
        self._resource = None
        self._finished = False

        self._operation = RelayPollingStrategy()

        self.status = self._operation.set_initial_status("200")
    
    def status(self) -> str:
        return self.status
    
    def finished(self) -> bool:
        #shut down everything here
        return True if self.status == "Succeeded" else False
    
    def resource(self) -> JSON:
        return self._deserialization_callback(self._resource)
    
    def run(self, event) -> None:
        while not self.finished():
            self.update_status(event)
            
    def update_status(self, event_thread) -> None:
        from_rendezvous = False
        response = self.control_conn.recv()

        #response2 = self.control_conn.recv()
            
        request = json.loads(response)["request"]
        if request:
            LOG.debug(f"Received request from control websocket")
            if 'method' not in request: # This means we have to rendezvous to get the rest
                LOG.debug(f"connecting to rendezvous websocket")
                self.rendezvous_conn = create_connection(request['address'])
                rendezvous_resp = self.rendezvous_conn.recv()
                command = json.loads(rendezvous_resp)
                request = command['request']
                from_rendezvous = True
            
            if request['body']:
                event = self.control_conn.recv() if not from_rendezvous else self.rendezvous_conn.recv()
                #print(event.decode('utf-8'))
                self._resource = event
            response = {
                'requestId': request['id'],
                'body': True,
                'statusCode': '200',
                'responseHeaders': {'content-type': 'text/html'},
            }
            if not from_rendezvous: #TODO if response is over 64kb we need to use rendezvous
                self.control_conn.send(json.dumps({'response':response}))
                LOG.debug(f"Sent response to control websocket")
            else:
                self.rendezvous_conn.send(json.dumps({'response':response}))
            if self.rendezvous_conn:
                self.rendezvous_conn.close()
                self.rendezvous_conn = None
                from_rendezvous = False
            
        self.status = "Succeeded"
        self.send_ping = False
        self.stop_event.set()
        LOG.debug(f"Closing ping")
        self.ping_thread.join()
        LOG.debug(f"closed ping")
        self.control_conn.close()
        LOG.debug(f"Closed control websocket {self.listener_url}")
        LOG.debug("Setting event")
        event_thread.set()
        



class HybridConnectionListener():
    def __init__(self, fully_qualified_name: str, entity_path: str, *, sas_token:str, **kwargs: Any) -> None:
        self.listener_url = create_listener_url(fully_qualified_name, entity_path, sas_token)
        self.ping_interval = 20
        self.ping_thread = None
        self.send_ping = True
        self.notification_lock = threading.Lock()
        self.notification = {}


    @classmethod
    def from_connection_string(cls, conn_str: str, **kwargs):
        host, policy, key, entity, token, token_expiry = cls._parse_conn_str(conn_str, **kwargs)
        token = generate_sas_token(host, entity, policy, key)
        return cls(host, entity, sas_token = token, **kwargs)
    
    def _send_ping(self):
        while self.send_ping:
            try:
                self.control_conn.ping()
                LOG.debug(f"Sent ping to control websocket {self.listener_url}")
            except Exception as e:
                self.reconnect()
                self.control_conn.ping()
                raise Exception(f"Failed to send ping to {self.listener_url}") from e
            time.sleep(self.ping_interval)

    def close(self):
        self.send_ping = continue_ping
        self.control_conn.close()
        self.ping_thread.join()
        LOG.debug(f"Closed control websocket {self.listener_url}")

    def reconnect(self):
        try:
            self.control_conn.close()
            self.control_conn = create_connection(self.listener_url)
            LOG.debug(f"reconnected to control websocket {self.listener_url}")
        except Exception as e:
            raise Exception(f"failed to reconnect to {self.listener_url}") from e

    def open(self):
        try:
            self.control_conn = create_connection(self.listener_url, timeout=600)
            LOG.debug(f"Connected to control websocket {self.listener_url}")
            LOG.debug("Listening for notifications")
            self.notification_thread = threading.Thread(target=self._receive, daemon=True)
            self.notification_thread.start()
            self.ping_thread = threading.Thread(target=self._send_ping, daemon=True)
            self.ping_thread.start()
        except Exception as e:
            raise Exception(f"Failed to connect to {self.listener_url}") from e

    
    def _receive(self, **kwargs):
        #self._open()

        keep_running = True
        from_rendezvous = False
        rendezvous_conn = None

        while keep_running:
            LOG.debug("Waiting for request from control websocket")
            try:
                response = self.control_conn.recv()
            except Exception as e:
                LOG.debug(f"Failed to receive request {e}")
                self.reconnect()
                LOG.debug("Waiting for request from control websocket after reconnect")
                response = self.control_conn.recv()
            
            request = json.loads(response)["request"]

            if request:
                LOG.debug(f"Received request from control websocket at {datetime.now(timezone.utc)}")
                if 'method' not in request: # This means we have to rendezvous to get the rest
                    LOG.debug(f"connecting to rendezvous websocket")
                    rendezvous_conn = create_connection(request['address'])
                    rendezvous_resp = rendezvous_conn.recv()
                    command = json.loads(rendezvous_resp)
                    request = command['request']
                    from_rendezvous = True
                
                if request['body']:
                    LOG.debug(f"Received body from {request['id']} at {datetime.now(timezone.utc)}, waiting on event")
                    try:
                        event = self.control_conn.recv() if not from_rendezvous else rendezvous_conn.recv()
                    except Exception as e:
                        LOG.debug(f"Failed to receive event {request['id']} {e}")
                        self.reconnect()
                        event = self.control_conn.recv() if not from_rendezvous else rendezvous_conn.recv()

                    LOG.debug(f"Received event for {request['id']} event: {request}")
                    json_event = json.loads(event)
                    print(event)
                    wait_event = None
                    if 'operationalInfo' in json_event[0]['data']:
                        id = f"{json_event[0]['data']['operationalInfo']['operationalStatus']['contexts'][0].split(',')[1].split(';')[0].split('/')[1]}/{json_event[0]['data']['operationalInfo']['operationalStatus']['contexts'][0].split(',')[1].split(';')[0].split('/')[2]}"
                        LOG.debug(f"Received an event for {id} at {datetime.utcnow()}}")
                        status = json_event[0]['data']['operationalInfo']['operationalStatus']['status']
                        if status == 'Created':
                            with self.notification_lock:
                                LOG.debug(f"storing notification for {id} status: {status} at {datetime.now(timezone.utc)}")
                                if id in self.notification:
                                    wait_event = self.notification[id]
                                self.notification[id] = json_event
                                if wait_event:
                                    wait_event.set()
                        else:
                            LOG.debug(f"Received an event for {id} but the status was {status} at {datetime.now(timezone.utc)}")

                response = {
                    'requestId': request['id'],
                    'body': True,
                    'statusCode': '200',
                    'responseHeaders': {'content-type': 'text/html'},
                }
                if not from_rendezvous: #TODO if response is over 64kb we need to use rendezvous
                    self.control_conn.send(json.dumps({'response':response}))
                else:
                    rendezvous_conn.send(json.dumps({'response':response}))

                #with self.notification_lock:


                if rendezvous_conn:
                    rendezvous_conn.close()
                    rendezvous_conn = None
                    from_rendezvous = False

    def wait(self, id, timeout=None):

        with self.notification_lock:
            if id in self.notification:
                LOG.debug(f"notitication was already received for {id}")
                return self.notification[id]
            else:
                LOG.debug(f"notification was not received yet {id} at {datetime.now(timezone.utc)}")
                wait_event = threading.Event()
                self.notification[id] = wait_event
            
        LOG.debug(f"waiting for notification {id} at {datetime.now(timezone.utc)}")
        wait_event.wait(timeout)
        with self.notification_lock:
            if isinstance(self.notification[id], threading.Event):
                return None
            return self.notification[id]

        

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