
import mysql.connector

from config import MySQLDBConfig

class MySQLConn(object):
    """Stores the connection to mysql."""
    def __init__(self, db, user, password, host, port):
        self.db = db
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def connect(self):
        connection = mysql.connector.connect(
            host=self.host,
            database=self.db,
            user=self.user,
            password=self.password,
            port = self.port)
        connection.autocommit = True
        return connection

def update_status(sql):

    conn = MySQLConn(MySQLDBConfig().prd_schema,
                     MySQLDBConfig().prd_user,
                     MySQLDBConfig().prd_pass,
                     MySQLDBConfig().prd_host,
                     MySQLDBConfig().prd_port).connect()
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.close()
