import os, time, gzip, json, sqlite3
import pandas as pd
from os import path

###############################################################################

#  twitterData.py               Author: Jordan Richard 
#
#   Multi-function spaghetti code to handle parsing of compressed twitter json
#
#   USAGE: python3 twitterData.py

###############################################################################

# Reads compressed json objects from a gzip file. Returns each object and index
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


# Inserts tuple into sqlite database. Slow approach - need transaction
def insertTweetTuple(tweetTuple,userTuple,placeTuple,entitiesTuple):
    conn = sqlite3.connect("twittDB.db")
    c = conn.cursor()
    
    tweetsQuery =  ''' 
                        INSERT INTO tweets (
                            id_str,
                            created_at,
                            tweet_text,
                            source,
                            truncated,
                            lang) VALUES(?,?,?,?,?,?);
                    '''

    usersQuery =  '''
                        INSERT INTO user (
                            id_str,
                            name,
                            screen_name,
                            location,
                            url,
                            description,
                            verified,
                            followers_count,
                            friends_count,
                            favourites_count,
                            statuses_count,
                            acct_created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?);
                  '''

    placeQuery =  '''
                        INSERT INTO place (
                            id_str,
                            place_type,
                            name,
                            full_name,
                            country_code,
                            country) VALUES (?,?,?,?,?,?);
                  '''

    hashtagsQuery =  '''
                        INSERT INTO entities (
                            id_str,
                            hashtags) VALUES (?,?);                            
                     '''

    #Write values to DB
    c.execute(tweetsQuery,tweetTuple)
    c.execute(usersQuery,userTuple)
    c.execute(placeQuery,placeTuple)
    c.execute(hashtagsQuery,entitiesTuple)

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
                    lang VARCHAR);
                        ''' 
                
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
                            acct_created_at VARCHAR
                            );
                      '''

    createPlaceStmt =  '''
                        CREATE TABLE place(
                            id_str VARCHAR PRIMARY KEY NOT NULL,
                            place_type VARCHAR,
                            name VARCHAR,
                            full_name VARCHAR,
                            country_code VARCHAR,
                            country VARCHAR
                            );
                       '''

    createEntitiesStmt =  '''
                            CREATE TABLE entities(
                                id_str PRIMARY KEY NOT NULL,
                                    hashtags VARCHAR
                                );
                          '''

    c = conn.cursor()
    print(c.execute(createTweetsStmt))
    c.execute(createUserStmt)
    c.execute(createPlaceStmt)
    c.execute(createEntitiesStmt)

    conn.close()
    print("Database created.\n")



# Generates a list of JSON objects from compressed input
def getTweets():
        
    cr = ChunkReader('j228')
    iter = cr.get_records(228000)
    count = 0
    startTime = time.time()
    print("Getting the tweets from our dataset...")
    for i, (ndx, obj) in zip(range(228000),iter):    
        placeValues = ()
        # Filters out tweets without text
        if 'text' in obj:
            
            # List of values to save from each tweet object, acts as DB row 
            tweetValues = (
                obj['id_str'],
                obj['created_at'],
                obj['text'],
                obj['source'],
                obj['truncated'],
                obj['lang']
                )

            #List for user table
            userValues = (
                obj['id_str'],
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
                obj['user']['created_at']
                )

            # List for geo place data if present
            if obj['place'] != None:
                placeValues = (
                    obj['id_str'],
                    obj['place']['place_type'],
                    obj['place']['name'],
                    obj['place']['full_name'],
                    obj['place']['country_code'],
                    obj['place']['country']
                    )
            #Return null place if not included by usr    
            else:                   
                placeValues = (
                    obj['id_str'],
                    None,
                    None,
                    None,
                    None,
                    None
                )

            # Converts list of hashtag objects from each tweet into a string 
            # containing a summary list
            hashstring = None                     #String we are creating
            if obj['entities']['hashtags'] != None:
                hashCounter = 0;                    #Counter tracking hash indx
                

                for _ in obj['entities']['hashtags']:
                    hashtagList = obj['entities']['hashtags']
                    
                    #Finds endpoint of hashtag text -- dirty fix
                    endpt = str(hashtagList[hashCounter]).index('\', ') 
                    
                    #Concatenates current hashtag to list of tags for this tweet
                    newstring = (str(hashtagList[hashCounter])[10:endpt])
                    
                    #Filter out ugly extra comma on first iteration
                    if hashCounter == 0:
                        hashstring = newstring
                    else:
                        hashstring = hashstring + "," + newstring

                    hashCounter += 1                       
            entitiesValues = (obj['id_str'],hashstring)

            #Write given tuples into according tables
            insertTweetTuple(tweetValues,userValues,placeValues,entitiesValues)     


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
    
