import mongoengine as mongo


class AbstractTweet(mongo.Document):
    tweetid = mongo.fields.IntField(required=True)
    userid = mongo.fields.IntField(required=True)
    text = mongo.fields.StringField(required=True, max_length=200)
    location = mongo.fields.BaseField(required=True)
    geohash = mongo.fields.StringField(required=True, max_length=32)
    in_reply_to_id = mongo.fields.IntField(required=False),
    username = mongo.fields.StringField(required=True, max_length=32),
    screen_name = mongo.fields.StringField(required=True, max_length=32),
    description = mongo.fields.StringField(required=False, max_length=256)

    meta = {
        'allow_inheritance': True,
        'abstract': True
    }

class EnglishTweet(AbstractTweet):
    pass


class GenericTweet(AbstractTweet):
    lang = mongo.fields.StringField(required=False, max_length=16)


#class User(mongo.Document):
#    userid = mongo.fields.IntField(required = True)
#    name = mongo.fields.StringField(required = True)
#    tags = mongo.fields.ListField(mongo.fields.StringField())

def dbConnect(dbAddress):
    mongo.connect(dbAddress.split('/')[-1], host=dbAddress)

def genericSize():
    return GenericTweet.objects.count()