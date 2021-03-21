from dataclasses import dataclass
from contextlib import contextmanager

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger
import cx_Oracle



import debugpy


# 5678 is the default attach port in the VS Code debug configurations. Unless a host and port are specified, host defaults to 127.0.0.1
debugpy.listen(5678)
print("Waiting for debugger attach")
#debugpy.wait_for_client()


@dataclass
class NoracleCredentials(Credentials):
    host: str
    username: str
    password: str
    port: int = 1521

    @property
    def type(self):
        return 'noracle'

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        return ('host', 'username', 'port', 'database', "schema")


class NoracleConnectionManager(SQLConnectionManager):
    TYPE = 'noracle'

    @classmethod
    def open(cls, connection):

        raise RuntimeError("Not fully implemented yet!")
    
    @classmethod
    def get_response(cls, cursor):
        raise RuntimeError("Not fully implemented yet!")
        #return cursor.status_message

    def cancel(self, connection):
        raise RuntimeError("Not fully implemented yet!")
        tid = connection.handle.transaction_id()
        sql = 'select cancel_transaction({})'.format(tid)
        logger.debug("Cancelling query '{}' ({})".format(connection_name, pid))
        _, cursor = self.add_query(sql, 'master')
        res = cursor.fetchone()
        logger.debug("Canceled query '{}': {}".format(connection_name, res))

    @contextmanager
    def exception_handler(self, sql: str):        
        try:
            yield
        except cx_Oracle.DatabaseError as exc:
            self.release()

            logger.debug(f'myadapter error: {str(exc)}')
            raise dbt.exceptions.DatabaseException(str(exc))
        except Exception as exc:
            logger.debug(f"Error running SQL: {sql}")
            logger.debug("Rolling back transaction.")
            self.release()
            raise dbt.exceptions.RuntimeException(str(exc))
