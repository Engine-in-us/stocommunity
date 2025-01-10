from airflow.hooks.base import BaseHook

def get_connections(conn_id):
    conn = BaseHook.get_connection(conn_id)
    return {
        "host": conn.host,
        "schema": conn.schema,
        "login": conn.login,
        "password": conn.password,
        "port": conn.port,
        "extra": conn.extra_dejson,
    }
