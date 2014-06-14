Świetna biblioteka do Twitter API: *Twython* - https://twython.readthedocs.org/en/latest/index.html
Opis użycia Streaming API w Twythonie: https://twython.readthedocs.org/en/latest/usage/streaming_api.html

Instalacja Twythona:

    $ sudo apt-get install pip
    $ sudo pip install twython

Mongoengine:

    $ sudo apt-get install python-mongoengine
    
###Wyświetlanie tweetów

#####Potrzebujemy:

 - MongoDB 2.4.9 ( jest prawdopodobieństwo, że na innych wersjach nie zadziała)
    http://downloads.mongodb.org/linux/mongodb-linux-x86_64-2.4.9.tgz
 - Elasticsearch 1.0.0
    https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.0.0.tar.gz

Możemy je odpalać prosto z katalogów, które wypakujemy.

Załóżmy, że chcemy wyświetlać tweety, które znajdują się w bazie lokalnej 'twitter' i kolekcji 'generic_tweet'.

#####Konfiguracja Mongo:

1. Odpalalamy mongo jako replikę

        $ sudo ./mongod --replSet "rs0"
2. Łączymy się z bazą

        $ ./mongo
3. Potem w terminalu 

        $ use twitter
        $ rs.initiate()
        

#####Konfiguracja Elasticsearch:

1. Bedąc w głównym katalogu pobieramy potrzebne wtyczki:

        $ ./bin/plugin -install elasticsearch/elasticsearch-mapper-attachments/1.9.0
        $ ./bin/plugin --install com.github.richardwilly98.elasticsearch/elasticsearch-river-mongodb/2.0.0
        $ ./bin/plugin --url https://github.com/triforkams/geohash-facet/releases/download/geohash-facet-0.0.14/geohash-facet-0.0.14.jar --install geohash-facet
2. Uruchamiamy Elasticsearch

        $ ./bin/elasticsearch
3. Teraz konfiguracja: 

        $ curl -XPUT 'localhost:9200/twitter' -d '{
            "mappings": {
              "generic_tweet" : {
                "properties" : {
                  "_cls" : {
                    "type" : "string"
                  },
                  "_types" : {
                    "type" : "string"
                  },
                  "description" : {
                    "type" : "string"
                  },
                  "geo" : {
                    "type" : "double"
                  },
                  "geohash" : {
                    "type" : "string"
                  },
                  "location" : {
                    "type" : "geo_point"
                  },
                  "text" : {
                    "type" : "string"
                  },
                  "tweetid" : {
                    "type" : "long"
                  },
                  "userid" : {
                    "type" : "long"
                  }
                }
              }
            }
          }'
        $ curl -XPUT 'localhost:9200/_river/twitter/_meta' -d '{ 
            "type": "mongodb", 
            "mongodb": { 
                "db": "twitter", 
                "collection": "generic_tweet"
            }, 
            "index": {
                "name": "twitter", 
                "type": "generic_tweet" 
            }
        }'
4. Po wykonaniu powyższych komend utworzyliśmy indeks o nazwie 'twitter' z elementami o typie 'generic_tweet', które pobieramy z lokalnej bazy 'twitter' i kolekcji 'generic_tweet'.
5. Restatrujemy Elasticrearch. Aby sprawdzić czy wszystko działa wpisujemy w przegladarke:

        $ http://localhost:9200/twitter/_search?search_type=count&pretty=1
6. Jeśli otrzymaliśmy JSONa z odpowiedzią, gdzie klucz "total" jest rózny od zera to prawdopodobnie wszystko jest ok.
    

    
    
