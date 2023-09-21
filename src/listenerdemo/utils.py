from base64 import b64encode
from hashlib import sha256
from hmac import HMAC
import time
from collections import namedtuple
from urllib.parse import urlencode, quote_plus, quote

AccessToken = namedtuple('AccessToken', ['token', 'expiry'])

def generate_sas_token(audience: str, entity: str, policy: str, key: str, expiry:int = None):
    if not expiry:
        expiry = int(time.time()) + 3600  # Default to 1 hour.

    encoded_uri = quote_plus(f'http://{audience}/{entity}')
    encoded_policy = quote_plus(policy).encode("utf-8")
    encoded_key = key.encode("utf-8")

    ttl = int(expiry)
    sign_key = '%s\n%d' % (encoded_uri, ttl)
    signature = b64encode(HMAC(encoded_key, sign_key.encode('utf-8'), sha256).digest())
    result = {
        'sr': f'http://{audience}/{entity}',
        'sig': signature,
        'se': str(ttl)
    }
    if policy:
        result['skn'] = encoded_policy
    return 'SharedAccessSignature ' + urlencode(result)


def create_listener_url(namespace: str, entity: str, sas_token: str) -> str:
    url = f"wss://{namespace}/$hc/{entity}?sb-hc-action=listen"

    if sas_token:
        url += '&sb-hc-token=' + quote(sas_token)
    
    return url