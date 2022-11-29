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
    conn = sqlite3.connect("newDB")
    
    c = conn.cursor()
    
    #tweetDataFrame.to_sql('tweets', conn, if_exists='replace', index = False)
    
    print("Do some operations on database here...")

    """
    Prints each tweet in our list of tweets
    ind = 1
    for tweetObj in dataframe:
        print("TWEET ", ind,"\n",tweetObj,"\n")
        ind += 1
    """

    conn.close()
    print("Database closed... all done.")


# TODO: Converts a JSON object to a pandas dataframe object
def jsonToDataFrame(tweetList):
    #Converts list of tweet dicts into pandas dataframe
    tweetDataFrame = pd.DataFrame(tweetList)
    #tweetDataFrame['created_at'] = pd.to_datetime(tweetList['created_at'])
    #Cleaning data example, changing to date type

    #display(tweetDataFrame) #Displays dataframe using external library
    print("Dataframe created. Details below:")
    print(tweetDataFrame.info(),"\n")



# Generates a list of JSON objects from compressed input
def getTweets():
        
    cr = ChunkReader('j228')
    iter = cr.get_records(228000)
    
    print('[')

    #Add each tweet json to a list of tweets
    tweets = [] #Creates a list of tweets
    for i, (ndx, obj) in zip(range(228000),iter):    
        tweets.append(obj)
    
    print("Parsed ", len(tweets), " tweets.\n")
    print(']')
    
    return tweets



if __name__ == '__main__':

    tweetList = getTweets()
    newDF = jsonToDataFrame(tweetList)
    jsonToSQL(tweetList)
