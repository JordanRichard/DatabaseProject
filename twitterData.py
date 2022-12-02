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
def insertTweetTuple(tupleToInsert):
    conn = sqlite3.connect("twittDB.db")
    c = conn.cursor()
    
    tweetsQuery =  ''' 
                        INSERT INTO tweets (
                            id_str,
                            created_at,
                            source,
                            tweet_text,
                            truncated,
                            quote_count,
                            reply_count,
                            retweet_count,
                            favourites_count,
                            lang) VALUES(?,?,?,?,?,?,?,?,?,?);
                    '''

    c.execute(tweetsQuery,tupleToInsert)
    conn.commit()
    conn.close()


# Initializes The database
def createDB():
    
    print("Starting up the DB...")
    conn = sqlite3.connect("twittDB.db")
    
    createTweetsStmt =  '''
                CREATE TABLE tweets(
                    id_str VARCHAR PRIMARY KEY NOT NULL,
                    created_at VARCHAR,
                    tweet_text TEXT,
                    source VARCHAR,
                    truncated BOOL,
                    quote_count int,
                    reply_count int,
                    retweet_count int,
                    favourites_count int,
                    lang VARCHAR);
            ''' 
                #place VARCHAR,       Causing some issues
    
    

    createUserStmt =  '''    
                        CREATE TABLE user(
                            id_str VARCHAR PRIMARY KEY NOT NULL,
                            name VARCHAR,
                            screen_name VARCHAR,
                            location VARCHAR,
                            url VARCHAR,
                            description TEXT,
                            verified BOOL,
                            followers_count INT,
                            friends_count INT,
                            favourites_count INT,
                            statuses_count INT,
                            created_at VARCHAR
                            );
                      '''

    c = conn.cursor()
    print(c.execute(createTweetsStmt))
    c.execute(createUserStmt)

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
                obj['id_str'],
                obj['created_at'],
                obj['text'],
                obj['source'],
                obj['truncated'],
                obj['quote_count'],
                obj[('reply_count')],
                obj['retweet_count'],
                obj['favorite_count'],
                obj['lang']
                )
                #obj['place'],                      # Type issue with sqlite -- not text or varchar?
                
            #print(ndx,tweetValues) # Shows the json objects as they are retrieved

            insertTweetTuple(tweetValues)                   

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
    
