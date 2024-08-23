import time
from typing import Union

import psycopg2  # type: ignore



from db import migrations
from helpers.logger import LogLevel, log, error_log  # type: ignore
from psycopg2.pool import ThreadedConnectionPool
from server.setup import start
def connect_multi_threaded(
    dbconfig: DBConfig,
    min_conn_pool: int = 1,
    max_conn_pool: int = 30,
    back_off: int = 20,
) -> Union[psycopg2.connect, None]:
    attempt, max_attempts, exponential = 0, 5, 2
    if dbconfig.err:
        log(
            f"db.adapter.connect: unable to connect to database: {dbconfig.err}",
            LogLevel.ERROR,
        )
        return []
    while attempt < max_attempts:
        try:
            connection_pool = ThreadedConnectionPool(
                min_conn_pool,
                max_conn_pool,
                dbname=dbconfig.db_name,
                user=dbconfig.user,
                password=dbconfig.passw,
                host=dbconfig.db_host_address[0],
                port=dbconfig.port,
                connect_timeout=20,
            )
            log("connection pool established", LogLevel.DEBUG)
            return connection_pool
        except psycopg2.OperationalError as exception:
            log(
                f"connect: failed create connection pool {dbconfig.db_name} in {dbconfig.db_host_address}:{dbconfig.port}: {exception}",
                LogLevel.WARNING,
            )
            attempt += 1
            log(f"retrying in {back_off} s: {attempt}/{max_attempts}", LogLevel.WARNING)
            time.sleep(back_off)
            back_off *= exponential
            log("retrying", LogLevel.WARNING)
    log(
        f"connect: failed create connection pool {dbconfig.db_name} in {dbconfig.db_host_address}:{dbconfig.port}: after {max_attempts} attempts",
        LogLevel.ERROR,
    )
    log("max number of retries reached", LogLevel.INFO)
    return []