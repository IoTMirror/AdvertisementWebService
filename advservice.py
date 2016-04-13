import flask
import json
import os
import psycopg2
import psycopg2.extras
import urllib.parse


app = flask.Flask(__name__)

class AdvDatabase:
  def __init__(self, db_url, table_prefix=""):
    self.url = urllib.parse.urlparse(db_url)
    self.table_prefix=table_prefix
    
  def getUserHashtags(self,userID):
    url = self.url
    con = psycopg2.connect(database=self.url.path[1:], user=self.url.username,
			  password=self.url.password,host=self.url.hostname,
			  port=self.url.port
	  )
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT keyword, count FROM twitter_hashtags WHERE user_id=(%s);",[userID])
    rows = cur.fetchall()
    cur.close()
    con.close()
    return [dict(row) for row in rows]
  
  def getUserHashtag(self,userID,keyword):
    url = self.url
    con = psycopg2.connect(database=self.url.path[1:], user=self.url.username,
			  password=self.url.password,host=self.url.hostname,
			  port=self.url.port
	  )
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT keyword, count FROM twitter_hashtags WHERE user_id=(%s) AND keyword=(%s);",[userID,keyword])
    row = cur.fetchone()
    cur.close()
    con.close()
    if row is None:
      return None
    return dict(row)
  
  def insertUserHashtag(self,userID,keyword):
    url = self.url
    con = psycopg2.connect(database=self.url.path[1:], user=self.url.username,
			  password=self.url.password,host=self.url.hostname,
			  port=self.url.port
	  )
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("INSERT INTO twitter_hashtags(user_id,keyword) VALUES(%s,%s);",[userID,keyword])
    con.commit()
    cur.close()
    con.close()
    
  def updateUserHashtagCount(self,hashtag):
    url = self.url
    con = psycopg2.connect(database=self.url.path[1:], user=self.url.username,
			  password=self.url.password,host=self.url.hostname,
			  port=self.url.port
	  )
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("UPDATE twitter_hashtags SET count=%s WHERE user_id=(%s) AND keyword=(%s);",[hashtag["count"],hashtag["user_id"],hashtag["keyword"]])
    rowcount = cur.rowcount
    con.commit()
    cur.close()
    con.close()
    return rowcount
  
  def deleteUserHashtags(self,userID):
    url = self.url
    con = psycopg2.connect(database=self.url.path[1:], user=self.url.username,
			  password=self.url.password,host=self.url.hostname,
			  port=self.url.port
	  )
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("DELETE FROM twitter_hashtags WHERE user_id=(%s);",[userID])
    con.commit()
    cur.close()
    con.close()
    
  def deleteUserHashtag(self,userID, keyword):
    url = self.url
    con = psycopg2.connect(database=self.url.path[1:], user=self.url.username,
			  password=self.url.password,host=self.url.hostname,
			  port=self.url.port
	  )
    cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("DELETE FROM twitter_hashtags WHERE user_id=(%s) AND keyword=(%s);",[userID,keyword])
    con.commit()
    cur.close()
    con.close()

@app.route('/users/<userID>/tweeter/hashtags', methods=['GET'])
def getUserHashtags(userID):
  hashtags = AdvDatabase(os.environ['DATABASE_URL']).getUserHashtags(userID)
  return json.dumps(hashtags)

@app.route('/users/<userID>/tweeter/hashtags/<keyword>', methods=['GET'])
def getUserHashtag(userID,keyword):
  hashtag = AdvDatabase(os.environ['DATABASE_URL']).getUserHashtag(userID,keyword)
  if hashtag is None:
    return ('',404)
  return json.dumps(hashtag)

@app.route('/users/<userID>/tweeter/hashtags/<keyword>', methods=['PUT'])
def putUserHashtag(userID,keyword):
  db = AdvDatabase(os.environ['DATABASE_URL'])
  try:
    db.insertUserHashtag(userID,keyword)
    return ('',201,{"location":flask.url_for("getUserHashtag",userID=userID, keyword=keyword)})
  except psycopg2.IntegrityError:
    hashtag = db.getUserHashtag(userID,keyword)
    hashtag["count"]+=1;
    hashtag["user_id"]=userID
    if hashtag is None:
      return ('',404)
    rowcount = db.updateUserHashtagCount(hashtag)
    if(rowcount==0):
      return ('',404)
    return ('',204)
    

@app.route('/users/<userID>/tweeter/hashtags', methods=['DELETE'])
def deleteUserHashtags(userID):
  AdvDatabase(os.environ['DATABASE_URL']).deleteUserHashtags(userID)
  return ('',204)

@app.route('/users/<userID>/tweeter/hashtags/<keyword>', methods=['DELETE'])
def deleteUserHashtag(userID,keyword):
  try:
    AdvDatabase(os.environ['DATABASE_URL']).deleteUserHashtag(userID,keyword)
    return ('',204)
  except psycopg2.IntegrityError:
    return ('',409)

if __name__ == '__main__':
  port = int(os.environ.get('PORT',5000))
  app.run(host='0.0.0.0',port=port)