import mongoengine as mongo

class Tweet(mongo.Document):
    tweetid = mongo.fields.IntField(required = True)
    userid = mongo.fields.IntField(required = True)
    text = mongo.fields.StringField(required = True, max_length = 200)
    geo = mongo.fields.ListField(mongo.fields.FloatField(), required = True)

#class User(mongo.Document):
#    userid = mongo.fields.IntField(required = True)
#    name = mongo.fields.StringField(required = True)
#    tags = mongo.fields.ListField(mongo.fields.StringField())

mongo.connect('twitter2')

