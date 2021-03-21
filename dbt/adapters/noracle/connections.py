import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Optional, Tuple

import cx_Oracle
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import Connection
from dbt.logger import GLOBAL_LOGGER as logger

import dbt


@dataclass
class NoracleCredentials(Credentials):
    host: str
    username: str
    password: str
    port: int = 1521

    @property
    def type(self):
        return "noracle"

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        return ("host", "username", "port", "database", "schema")


class NoracleConnectionManager(SQLConnectionManager):
    TYPE = "noracle"

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials

        try:
            handle = cx_Oracle.connect(
                credentials.username,
                credentials.password,
                f"{credentials.host}/{credentials.database}",
                encoding="UTF-8",
            )
        except cx_Oracle.Error as exc:
            logger.error(f"Failed to connect: {exc}")

        connection.state = "open"
        connection.handle = handle

        return connection

    @classmethod
    def get_response(cls, cursor):
        return "OK"

    def cancel(self, connection):
        raise RuntimeError("Not fully implemented yet!")
        # tid = connection.handle.transaction_id()
        # sql = "select cancel_transaction({})".format(tid)
        # logger.debug("Cancelling query '{}' ({})".format(connection_name, pid))
        # _, cursor = self.add_query(sql, "master")
        # res = cursor.fetchone()
        # logger.debug("Canceled query '{}': {}".format(connection_name, res))

    @contextmanager
    def exception_handler(self, sql: str):
        try:
            yield
        except cx_Oracle.DatabaseError as exc:
            self.release()

            logger.debug(f"Oracle error: {str(exc)}")
            raise dbt.exceptions.DatabaseException(str(exc))
        except Exception as exc:
            logger.debug(f"Error running SQL: {sql}")
            logger.debug("Rolling back transaction.")
            self.release()
            raise dbt.exceptions.RuntimeException(str(exc))

    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()

        logger.debug(f"Using {self.TYPE} connection '{connection.name}'")

        with self.exception_handler(sql):
            if abridge_sql_log:
                log_sql = "{}...".format(sql[:512])
            else:
                log_sql = sql

            logger.debug(
                "On {connection_name}: {sql}",
                connection_name=connection.name,
                sql=log_sql,
            )
            pre = time.time()

            cursor = connection.handle.cursor()

            if bindings is not None:
                logger.info("Bindings are not implemented for Oracle!")
                logger.info(f"bindings: {bindings}")

            cursor.execute(sql)
            logger.debug(
                "SQL status: {status} in {elapsed:0.2f} seconds",
                status=self.get_response(cursor),
                elapsed=(time.time() - pre),
            )

            return connection, cursor
