import json
import re
import traceback
from types import StringType

class TagParser():
    
    def __init__(self):
        #set of tags mapped to a list of categories
        self.__tag_dict={}
        #a key in __geo_set contains tag and coordinates separated by commas
        self.__geo_set=set()
        self.__tags_filename=None
        self.__tweets_filename=None
        self.__output_filename=None
        
    def set_tag_filename(self,name):
        if type(name) is StringType:
            self.__tags_filename=name
    
    def set_input_filename(self,name):
        if type(name) is StringType:
            self.__tweets_filename=name
    
    def set_output_filename(self, name):
        if type(name) is StringType:
            self.__output_filename=name
    
    def get_tag_filename(self, ):
        return self.__tags_filename
    
    def get_input_filename(self, ):
        return self.__tweets_filname
    
    def get_output_fileaname(self):
        return self.__get_output_filename
    
    
    def read_tags(self):
        tags_file=open(self.__tags_filename,"r")
        for line in tags_file.readlines():
            tag_with_categories=line.rstrip("\n").split(",")
            self.__tag_dict[tag_with_categories[0]]=tag_with_categories[1:]
        print self.__tag_dict
            
    def process_tags(self):
        tweets_file=open(self.__tweets_filename,"r")
        output_file=open(self.__output_filename,"w")
        for line in tweets_file:
            tweet_object=json.loads(line)
            #print tweet_object
            for cat in self.__tag_dict.keys():
                prog_regular=re.compile(cat)
                #prog_at=re.compile(u"@"+cat)
                #prog_hash=re.compile(u"#"+cat)
                text_result_regular=prog_regular.match(tweet_object[u'text'])
                #text_result_at=prog_at.match(tweet_object[u'text'])
                #text_result_hash=prog_hash.match(tweet_object[u'text'])
                if u'description' in tweet_object:
                    description_result_regular=prog_regular.match(tweet_object[u'description'])
                    #description_result_at=prog_at.match(tweet_object[u'description'])
                    #description_result_hash=prog_hash.match(tweet_object[u'description'])
                else:
                    description_result_regular=None
                    #description_result_at=None
                    #description_result_hash=None
                if text_result_regular!=None or description_result_regular!=None:#\
                                        #or text_result_at!=None or text_result_hash!=None or desceiption_result_at!=None or description_result_hash!=None:
                    hsh=cat+','+','.join([ i.__str__() for i in tweet_object[u'geo']])
                    print_text = tweet_object[u'text']+'\n'+tweet_object[u'description']
                    print hsh
                    if not (hsh in self.__geo_set):   
                        self.__geo_set.add(hsh)
                        output_file.write(hsh+','+','.join(self.__tag_dict[cat])+'\n')
                        
                    
if __name__ == '__main__':
    t=TagParser()
    t.set_tag_filename("./muzycy")
    t.set_input_filename("../twitter.english_tweet.json")
    t.set_output_filename("./output.csv")
    t.read_tags()
    t.process_tags()