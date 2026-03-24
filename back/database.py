import os
import base64
import snowflake.connector
from snowflake.connector import DictCursor
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from config import *

# État global pour les erreurs Snowflake
_state = {"last_snowflake_error": None}

def set_last_error(err):
    _state["last_snowflake_error"] = str(err) if err else None

def get_last_error():
    return _state["last_snowflake_error"]

def get_required_env_vars():
    return ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_ROLE", 
            "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_PRIVATE_KEY_B64"]

def get_missing_required_env_vars():
    return [name for name in get_required_env_vars() if not os.getenv(name)]

def is_snowflake_configured():
    return len(get_missing_required_env_vars()) == 0

def load_private_key_der_bytes():
    key_b64 = os.getenv("SNOWFLAKE_PRIVATE_KEY_B64", "").strip()
    passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").strip()
    if not key_b64:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY_B64 is missing")

    private_key_pem = base64.b64decode(key_b64)
    private_key = serialization.load_pem_private_key(
        private_key_pem,
        password=passphrase.encode() if passphrase else None,
        backend=default_backend(),
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

def quote_ident(identifier):
    return '"' + str(identifier).replace('"', '""') + '"'

def fq_table(schema_name, table_name):
    database_name = (os.getenv("SNOWFLAKE_DATABASE") or "").strip()
    schema_part = quote_ident(schema_name)
    table_part = quote_ident(table_name)
    if database_name:
        return f"{quote_ident(database_name)}.{schema_part}.{table_part}"
    return f"{schema_part}.{table_part}"

def get_connection(query_tag="aura_backend"):
    if not is_snowflake_configured():
        raise Exception("Snowflake is not fully configured.")
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=SNOWFLAKE_SCHEMA_PUBLIC,
        private_key=load_private_key_der_bytes(),
        login_timeout=SNOWFLAKE_LOGIN_TIMEOUT_SECONDS,
        network_timeout=SNOWFLAKE_NETWORK_TIMEOUT_SECONDS,
        socket_timeout=SNOWFLAKE_SOCKET_TIMEOUT_SECONDS,
        ocsp_fail_open=True,
        session_parameters={"QUERY_TAG": query_tag},
    )