# -*- coding : utf8 -*-

class DataGrid:
    def __init__(self):
        pass
    def __del__(self):
        pass
    def __iter__(self):
        return self.next()
    def next(self):
        raise StopIteration

class FileReader(DataGrid):
    def __init__(self, files, fields, separator):
        self.files, self.fields, self.separator = files, fields, separator
    def next(self):
        for f in self.files:
            fi = open(f)
            for l in fi:
                yield dict(zip(self.fields, l.rstrip().split(self.separator)))
            fi.close()
        raise StopIteration


for i in FileReader(['testdata', 'testdata1'], ['id', 'age', 'rate', 'name'], ','): print i
class Reverse:
    """Iterator for looping over a sequence backwards."""
    def __init__(self, data):
        self.data = data
        self.index = len(data)
    def __iter__(self):
        return self
    def next(self):
        if self.index == 0:
            raise StopIteration
        self.index = self.index - 1
        return self.data[self.index]
#for i in Reverse([1,2,3]): print i