import os, gzip, json, sqlite3
import pandas as pd
from IPython.display import display

###############################################################################

#  twitterData.py               Author: Jordan Richard 
#
#   Multi-function spaghetti code to handle parsing of compressed twitter json
#
#   USAGE: python3 twitterData.py

###############################################################################

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


# TODO: Normalize and Insert pandas Dataframe into DB 
def jsonToSQL(dataframe):
    print("Initializing the database...")
    


# TODO: Converts a JSON object to a pandas dataframe object
def jsonToDataFrame(tweetList):
    #Converts list of tweet dicts into pandas dataframe
    tweetDataFrame = pd.DataFrame(tweetList)
    #tweetDataFrame['created_at'] = pd.to_datetime(tweetList['created_at'])
    #Cleaning data example, changing to date type

    #display(tweetDataFrame) #Displays dataframe using external library
    print("Dataframe created. Details below:")
    print(tweetDataFrame.head(),"\n")




def insertDB(tupleToInsert):
    conn = sqlite3.connect("twittDB.db")
    c = conn.cursor()
    c.execute("INSERT INTO testtable VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",tupleToInsert)
    conn.close()


# Initializes The database
def createDB():
    
    print("Starting up the DB...")
    conn = sqlite3.connect("twittDB.db")
    
    stmt = ("CREATE TABLE testtable("
        "created_at VARCHAR,"
        "id_str VARCHAR,"
        "source VARCHAR,"
        "tweet_text TEXT,"
        "truncated BOOL,"
        "name VARCHAR,"
        "screen_name VARCHAR,"
        "location VARCHAR,"
        "url VARCHAR,"
        "description TEXT"
        "verified BOOL,"
        "followers_count int,"
        "friends_count int,"
        "favourite_count int,"
        "statuses_count int,"
        "user_created_at VARCHAR,"
        "place TEXT,"
        "quote_count int,"
        "reply_count int,"
        "retweet_count int,"
        "favourites_count int,"
        "lang VARCHAR);")
    
    c = conn.cursor()
    print(c.execute(stmt))

    #c.execute("INSERT INTO testtable VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",tuple)

    # If using pandas approach - data needs to be flattened? 
    #tweetDataFrame.to_sql('tweets', conn, if_exists='replace', index = False)
    
    print("Do some operations on database here...")

    """
        TODO: Insert Values into DB
    """

    conn.close()
    print("Database closed... all done.")



# Generates a list of JSON objects from compressed input
def getTweets():
        
    cr = ChunkReader('j228')
    iter = cr.get_records(228000)
    
    # Add each tweet json to a list of tweets
    #tweets = [] # Creates a list of tweets
    x = 0
    print('[')
    for i, (ndx, obj) in zip(range(228000),iter):    
        #tweets.append(obj)
        if 'text' in obj:
            #print(ndx, obj['text'], "\n")
            
            # More convenient dict of json values to be inserted 
            tweetValues = (obj['created_at'],
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
                obj['place'],
                obj['quote_count'],
                obj[('reply_count')],
                obj['retweet_count'],
                obj['favorite_count'],
                obj['lang']
                )

            print(ndx,tweetValues)

            insertDB(tweetValues)


    print("Parsed ", ndx, " tweets.\n")
    print(']')
    



if __name__ == '__main__':
    createDB()
    getTweets()
    
    #newDF = jsonToDataFrame(tweetList)
    #jsonToSQL(tweetList)
