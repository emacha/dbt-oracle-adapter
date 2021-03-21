from dataclasses import dataclass
from contextlib import contextmanager

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.logger import GLOBAL_LOGGER as logger


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
        raise RuntimeError("Not fully implemented yet!")
        try:
            yield
        except myadapter_library.DatabaseError as exc:
            self.release(connection_name)

            logger.debug('myadapter error: {}'.format(str(e)))
            raise dbt.exceptions.DatabaseException(str(exc))
        except Exception as exc:
            logger.debug("Error running SQL: {}".format(sql))
            logger.debug("Rolling back transaction.")
            self.release(connection_name)
            raise dbt.exceptions.RuntimeException(str(exc))
