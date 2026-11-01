"""Two real MySQL drivers, preserving SQLSTATE for contract assertions.

PyMySQL does not expose SQLSTATE on its exception, so its packet decoder
is observed without changing exception classification. Each test process
installs this once; thread-local capture prevents cross-session mixing.
"""
import threading
import pymysql
import mysql.connector

_capture = threading.local()
_original = pymysql.err.raise_mysql_exception
_original_eof = pymysql.connections.MySQLResult._check_packet_is_eof


def _error(data):
    _capture.sqlstate = data[4:9].decode('ascii') if len(data) >= 9 and data[3:4] == b'#' else None
    _original(data)


pymysql.err.raise_mysql_exception = _error


def _eof(result, packet):
    # PyMySQL 1.1.2 reads EOF warnings/has_next but does not copy its status
    # into connection.server_status. Observe the wire without changing it.
    if packet.is_eof_packet():
        _capture.status = int.from_bytes(packet.get_all_data()[3:5], 'little')
    return _original_eof(result, packet)


pymysql.connections.MySQLResult._check_packet_is_eof = _eof


class Client:
    def __init__(self, endpoint, mode, database=None, timeout=10):
        host, port = endpoint.rsplit(':', 1)
        self.mode = mode
        opts = dict(host=host, port=int(port), user='sb', password='', database=database,
                    autocommit=True, charset='utf8mb4')
        if mode == 'pymysql':
            self.conn = pymysql.connect(**opts, connect_timeout=5, read_timeout=timeout, write_timeout=5)
        else:
            self.conn = mysql.connector.connect(**opts, use_pure=True, connection_timeout=5,
                                                read_timeout=timeout, write_timeout=5,
                                                collation='utf8mb4_bin')

    def query(self, sql, params=None):
        _capture.sqlstate = None
        _capture.status = None
        cur = (self.conn.cursor() if self.mode == 'pymysql'
               else self.conn.cursor(prepared=self.mode == 'prepared'))
        try:
            cur.execute(sql, params)
            result = [list(row) for row in cur.fetchall()] if cur.description else cur.rowcount
            if self.mode == 'pymysql':
                self.status = self.conn.server_status if _capture.status is None else _capture.status
            return result
        finally:
            try:
                cur.close()
            except (pymysql.Error, mysql.connector.Error):
                pass

    def error(self, sql, params=None):
        try:
            self.query(sql, params)
        except pymysql.Error as e:
            return [e.args[0], getattr(_capture, 'sqlstate', None)]
        except mysql.connector.Error as e:
            return [e.errno, e.sqlstate]
        raise AssertionError('expected an error: ' + sql)

    def active(self):
        # ERR packets have no status bits: refresh them with a successful
        # constant SELECT before asserting the server's transaction state.
        self.query('SELECT 1')
        return (bool(self.status & 1) if self.mode == 'pymysql'
                else self.conn.in_transaction)

    def close(self):
        try:
            self.conn.close()
        except (pymysql.Error, mysql.connector.Error):
            pass
