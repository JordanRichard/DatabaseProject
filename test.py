import os, gzip, json
class ChunkReader:
    def __init__(self, cnkdir):
        self.__cnkdir = cnkdir

    def get_cnkdir(self):
        return self.__cnkdir

    def get_filename(self, cnk):
        cnkdir = self.get_cnkdir()
        return f'{cnkdir}/a{cnk:08d}.cnk.gz'

    def get_records(self, cnk):
        fn = self.get_filename(cnk)
        if not os.path.exists(fn):
            return
        with gzip.open(fn, 'rb') as f:  # Opens file as bytes obj
            res_bytes = f.read()

        lst = res_bytes.splitlines()    # Splits bytes into list obj
        res_bytes = None                # to save intermediate memory
        
        for line in lst:                # Reads list line by line
            record = eval(line)
            if record == [] or record is None:
                return
            ndx = record[0]
            yield ndx, json.loads(record[1])

if __name__ == '__main__':
    
    def test():
        cr = ChunkReader('j228')
        iter = cr.get_records(228000)
        print('[')
        for i, (ndx, obj) in zip(range(250),iter):    #Range is the number of tweet objects to return
            if 'text' in obj:
                if obj["lang"] == "en":               #Filter out only english-tagged tweets
                    print("Object " ,i, ": @",obj['user']['name'],":",obj['entities'],",")
        print(']')
    
    test()
#Test