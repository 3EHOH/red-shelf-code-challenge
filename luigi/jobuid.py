# -*- coding: utf-8 -*-
"""
You can run this example like this:
    .. code:: console
            $ python jobuid.py
"""
from utils import MySQLConn

from config import MySQLDBConfig

def get_next_jobuid():
    """ returns the next JOBUID """

    row = (None,)

    conn = MySQLConn(MySQLDBConfig().prd_schema,
                     MySQLDBConfig().prd_user,
                     MySQLDBConfig().prd_pass
                     MySQLDBConfig().prd_host,
                     MySQLDBConfig().prd_port).connect()
    cur = conn.cursor()
    sql = "select max(uid)+1 as max_uid from processJob;"
    cur.execute(sql)
    row = cur.fetchone()
    if row[0] is not None:
        return row[0]
    else:
        return 1
    cur.close()
    conn.close()


if __name__ == "__main__":
    print(get_next_jobuid())
