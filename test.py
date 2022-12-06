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
        with gzip.open(fn, 'rb') as f:
            res_bytes = f.read()

        lst = res_bytes.splitlines()
        res_bytes = None # to save intermediate memory
        for line in lst:
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
        for i, (ndx, obj) in zip(range(20),iter):
            if 'text' in obj:
                print(obj,",")
        print(']')
    test()
