# -*- coding: utf-8 -*-
"""
You can run this example like this:
    .. code:: console
            $ python jobuid.py
"""
import mysql.connector

from config import MySQLDBConfig

def get_next_jobuid():
    """ returns the next JOBUID """

    sql = "select max(uid)+1 as max_uid from processJob;"
    db = mysql.connector.connect(host=MySQLDBConfig().prd_host,
                                 user=MySQLDBConfig().prd_user,
                                 passwd=MySQLDBConfig().prd_pass,
                                 db=MySQLDBConfig().prd_schema)    
    cur = db.cursor()
    cur.execute(sql)
    row = cur.fetchone()
    db.close()
    if row[0] is not None:
        return row[0]
    else:
        return 1


if __name__ == "__main__":
    print(get_next_jobuid())
