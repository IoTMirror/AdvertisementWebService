import flask
import json
import os
import psycopg2
from adv_utils import UserKeywordsDatabase

app = flask.Flask(__name__)
tkdb = UserKeywordsDatabase(os.environ['DATABASE_URL'], "adv_twitter_hashtags")
gtkdb = UserKeywordsDatabase(os.environ['DATABASE_URL'], "adv_google_gmail")

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

@app.route('/users/<user_id>/google/gmail/keywords', methods=['GET'])
def getGmailKeywords(user_id):
  keywords = gtkdb.getKeywords(user_id)
  return json.dumps(keywords)

@app.route('/users/<user_id>/google/gmail/subjects', methods=['POST'])
def putGmailTitle(user_id):
  content = flask.request.get_json(silent = True)
  if content is None:
    return ('',400)
  if "subject" not in content:
    return ('', 400)
  keywords = content["subject"].split()
  result = {}
  result["keywords_updated"] = 0
  for key in keywords:
    try:
      gtkdb.insertKeyword(user_id,key.lower())
      result["keywords_updated"] += 1
    except psycopg2.IntegrityError:
      keyword = gtkdb.getKeyword(user_id,key.lower())
      if keyword is None:
        continue
      rowcount = gtkdb.updateKeywordCount(user_id,keyword["keyword"],keyword["count"]+1)
      result["keywords_updated"] += rowcount
  return json.dumps(result)

@app.route('/users/<user_id>/google/gmail/keywords', methods=['DELETE'])
def deleteGmailKeywords(user_id):
  gtkdb.deleteKeywords(user_id)
  return ('',204)

if __name__ == '__main__':
  port = int(os.environ.get('PORT',5000))
  app.run(host='0.0.0.0',port=port)
