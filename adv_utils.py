import urllib.parse
import psycopg2
import psycopg2.extras

class UserKeywordsDatabase(object):
  def __init__(self, db_url, table_name):
    self.url = urllib.parse.urlparse(db_url)
    self.table_name = table_name

  def getKeywords(self,user_id):
    con = None
    try:
      rows = None
      con = self.getConnection()
      with con:
        with con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
          cur.execute("SELECT keyword, count FROM "+self.table_name+" WHERE user_id=(%s);",[user_id])
          rows = cur.fetchall()
      if rows is None:
        return rows
      else:
        return [dict(row) for row in rows]
    finally:
      if con is not None:
        con.close()

  def getKeyword(self,user_id,keyword):
    con = None
    try:
      row = None
      con = self.getConnection()
      with con:
        with con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
          cur.execute("SELECT keyword, count FROM "+self.table_name+" WHERE user_id=(%s) AND keyword=(%s);",
                      [user_id,keyword])
          row = cur.fetchone()
      if row is None:
        return row
      else:
        return dict(row)
    finally:
      if con is not None:
        con.close()

  def insertKeyword(self,user_id,keyword):
    con = None
    try:
      con = self.getConnection()
      with con:
        with con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
          cur.execute("INSERT INTO "+self.table_name+"(user_id,keyword) VALUES(%s,%s);",[user_id,keyword])
    finally:
      if con is not None:
        con.close()

  def updateKeywordCount(self,user_id, keyword, count):
    con = None
    try:
      rowcount = None
      con = self.getConnection()
      with con:
        with con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
          cur.execute("UPDATE "+self.table_name+" SET count=%s WHERE user_id=(%s) AND keyword=(%s);",
                      [count, user_id, keyword])
          rowcount = cur.rowcount
      return rowcount
    finally:
      if con is not None:
        con.close()

  def deleteKeywords(self,user_id):
    con = None
    try:
      con = self.getConnection()
      with con:
        with con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
          cur.execute("DELETE FROM "+self.table_name+" WHERE user_id=(%s);",[user_id])
    finally:
      if con is not None:
        con.close()

  def getConnection(self):
    return psycopg2.connect(database=self.url.path[1:], user=self.url.username,
                            password=self.url.password,host=self.url.hostname,
                            port=self.url.port
           )
