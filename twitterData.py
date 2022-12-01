import os, time, gzip, json, sqlite3
import pandas as pd
from IPython.display import display
from os import path

###############################################################################

#  twitterData.py               Author: Jordan Richard 
#
#   Multi-function spaghetti code to handle parsing of compressed twitter json
#
#   USAGE: python3 twitterData.py

###############################################################################

# Reads compressed json objects from a gzip file. Returns each object and an index.
class ChunkReader:

    def __init__(self, cnkdir):
        self.__cnkdir = cnkdir


    def get_cnkdir(self):
        return self.__cnkdir


    def get_filename(self, cnk):
        cnkdir = self.get_cnkdir()
        return f'{cnkdir}/a{cnk:08d}.cnk.gz'

    # Returns each tweet obj and its index
    def get_records(self, cnk):
        fn = self.get_filename(cnk)
        if not os.path.exists(fn):
            return
        with gzip.open(fn, 'rb') as f:  # Opens file as bytes obj
            res_bytes = f.read()

        lst = res_bytes.splitlines()    # Splits bytes into seperate json objs
        res_bytes = None                # to save intermediate memory
        
        for line in lst:                # Reads list line by line
            record = eval(line)
            if record == [] or record is None:
                return
            ndx = record[0]
            yield ndx, json.loads(record[1])


# Inserts a tuple into the sqlite database. Slow approach - need to alter to transaction
def insertTuple(tupleToInsert):
    conn = sqlite3.connect("twittDB.db")
    c = conn.cursor()
    
    query = ("INSERT INTO tweets ("
        "created_at,"
        "id_str,"
        "source,"
        "tweet_text,"
        "truncated,"
        "name,"
        "screen_name,"
        "location,url,"
        "description,"
        "verified,"
        "followers_count,"
        "friends_count,"
        "favourite_count,"
        "statuses_count,"
        "user_created_at,"
        "quote_count,"
        "reply_count,"
        "retweet_count,"
        "favourites_count,"
        "lang) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);")

    c.execute(query,tupleToInsert)
    conn.commit()
    conn.close()


# Initializes The database
def createDB():
    
    print("Starting up the DB...")
    conn = sqlite3.connect("twittDB.db")
    
    stmt = ("CREATE TABLE tweets("
        "created_at VARCHAR,"
        "id_str VARCHAR,"
        "tweet_text TEXT,"
        "source VARCHAR,"
        "truncated BOOL,"
        "name VARCHAR,"
        "screen_name VARCHAR,"
        "location VARCHAR,"
        "url VARCHAR,"
        "description TEXT,"
        "verified BOOL,"
        "followers_count int,"
        "friends_count int,"
        "favourite_count int,"
        "statuses_count int,"
        "user_created_at VARCHAR,"
        #"place VARCHAR,"       #Causing some issues
        "quote_count int,"
        "reply_count int,"
        "retweet_count int,"
        "favourites_count int,"
        "lang VARCHAR);")
    
    c = conn.cursor()
    print(c.execute(stmt))

    conn.close()
    print("Database created.\n")



# Generates a list of JSON objects from compressed input
def getTweets():
        
    cr = ChunkReader('j228')
    iter = cr.get_records(228000)
    
    startTime = time.time()
    print("Getting the tweets from our dataset...")
    for i, (ndx, obj) in zip(range(228000),iter):    
    
        # Filters out tweets without text
        if 'text' in obj:
            
            # List of values we are interested in from each tweet object, acts as a DB row for insert
            tweetValues = (
                obj['created_at'],
                obj['id_str'],
                obj['text'],
                obj['source'],
                obj['truncated'],
                obj['user']['name'],
                obj['user']['screen_name'],
                obj['user']['location'],
                obj['user']['url'],
                obj['user']['description'],
                obj['user']['verified'],
                obj['user']['followers_count'],
                obj['user']['friends_count'],
                obj['user']['favourites_count'],
                obj['user']['statuses_count'],
                obj['user']['created_at'],
                #obj['place'],                      # Type issue with sqlite -- not text or varchar?
                obj['quote_count'],
                obj[('reply_count')],
                obj['retweet_count'],
                obj['favorite_count'],
                obj['lang']
                )

            #print(ndx,tweetValues) # Shows the json objects as they are retrieved

            insertTuple(tweetValues)                   

    endTime = time.time()
    elapsedTime = endTime - startTime
    print("Processed ", ndx, " tweets in", elapsedTime, " seconds.\n")
    



if __name__ == '__main__':
    
    # Deletes and re-creates database if one already exists
    if path.exists('twittDB.db'):
        print("Database file already exists -- Reinitializing.")
        os.remove('twittDB.db')
        
    createDB()
    getTweets()
    
