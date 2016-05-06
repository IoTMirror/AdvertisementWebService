import flask
import json
import os
import psycopg2
from adv_utils import UserKeywordsDatabase

app = flask.Flask(__name__)
tkdb = UserKeywordsDatabase(os.environ['DATABASE_URL'], "adv_twitter_hashtags")

@app.route('/users/<userID>/twitter/hashtags', methods=['GET'])
def getUserHashtags(userID):
  hashtags = tkdb.getKeywords(userID)
  return json.dumps(hashtags)

@app.route('/users/<userID>/twitter/hashtags/<keyword>', methods=['PUT'])
def putUserHashtag(userID,keyword):
  try:
    tkdb.insertKeyword(userID,keyword)
    return ('',201)
  except psycopg2.IntegrityError:
    hashtag = tkdb.getKeyword(userID,keyword)
    if hashtag is None:
      return ('',404)
    rowcount = tkdb.updateKeywordCount(userID,hashtag["keyword"],hashtag["count"]+1)
    if(rowcount==0):
      return ('',404)
    return ('',204)

@app.route('/users/<userID>/twitter/hashtags', methods=['DELETE'])
def deleteUserHashtags(userID):
  tkdb.deleteKeywords(userID)
  return ('',204)

if __name__ == '__main__':
  port = int(os.environ.get('PORT',5000))
  app.run(host='0.0.0.0',port=port)
