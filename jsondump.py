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
        start = 228000
        lst = []
        for cnk in range(start,start + 1):
            iter = cr.get_records(cnk)
            fn = f'a{cnk}.json'
            for ndx,obj in cr.get_records(cnk):
                lst.append(obj)
        with open(fn,'w') as f:
            json.dump(lst,f,indent=2)
    test()
