#!/usr/bin/env python
# -*- coding : utsf8 -*-
DEFAULT_HOST, DEFAULT_PORT = None, 37100
import sys, json, random, traceback
sys.path.append('./gen-py')
from DataAccess import QueryProcessorService
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from hbase import Hbase
from hbase.ttypes import *
def Main(host, port): TServer.TSimpleServer(QueryProcessorService.Processor(Worker()), TSocket.TServerSocket(None if host == None or host == '' else str(host), int(port)), TTransport.TBufferedTransportFactory(), TBinaryProtocol.TBinaryProtocolFactory()).serve()
class Record:
    def __init__(self, d): self.d = dict(d)
    def v(self, k): return self.d[k] if k in self.d else None
    def __del__(self): del self.d
    def __str__(self): return str(self.d)
    def JSON(self): return self.d
class DataGrid:
    def __init__(self): pass
    def __iter__(self): return self.next()
    def next(self): raise StopIteration
class BasicDataGrid(DataGrid):
    def __init__(self, data): self.data = data
    def __iter__(self): return iter(self.data)
class TestSourceData(DataGrid):
    def next(self):
        yield Record({'start' : '234247', 'rate': 0.588, 'rate000': 0.47658, 'channel': 102, 'area': 1, 'date': 20111120, 'length': 102, 'market': 0.0})
        yield Record({'start' : '234243', 'rate': 0.952, 'rate000': 0.32376, 'channel': 102, 'area': 3, 'date': 20111117, 'length': 68, 'market': 0.26448})
        yield Record({'start' : '001200', 'rate': 0.758, 'rate000': 1.9469, 'channel': 108, 'area': 5, 'date': 20111117, 'length': 178, 'market': 0.97197068})
        yield Record({'start' : '001200', 'rate': 0.867, 'rate000': 0.29484, 'channel': 110, 'area': 1, 'date': 20111120, 'length': 73, 'market': 0.8012829})
        yield Record({'start' : '001200', 'rate': 0.948, 'rate000': 0.1884, 'channel': 110, 'area': 3, 'date': 20111120, 'length': 83, 'market': 0.63755})
        yield Record({'start' : '001200', 'rate': 0.347, 'rate000': 4.6448, 'channel': 107, 'area': 3, 'date': 20111120, 'length': 40, 'market': 0.311521})
        yield Record({'start' : '345345', 'rate': 0.00531, 'rate000': 0.14311, 'channel': 106, 'area': 1, 'date': 20111119, 'length': 110, 'market': 0.76})
        yield Record({'start' : '001200', 'rate': 0.654, 'rate000': 0.60679, 'channel': 108, 'area': 1, 'date': 20111119, 'length': 21, 'market': 0.59139456})
        yield Record({'start' : '001200', 'rate': 0.827, 'rate000': 3.4546, 'channel': 106, 'area': 1, 'date': 20111119, 'length': 30, 'market': 0.69695})
        yield Record({'start' : '001200', 'rate': 0.0517, 'rate000': 0.62187, 'channel': 102, 'area': 5, 'date': 20111121, 'length': 147, 'market': 0.56})
        yield Record({'start' : '756735', 'rate': 0.715, 'rate000': 6.099, 'channel': 110, 'area': 3, 'date': 20111118, 'length': 119, 'market': 0.02340277})
        yield Record({'start' : '001200', 'rate': 0.73, 'rate000': 1.0779, 'channel': 102, 'area': 1, 'date': 20111119, 'length': 82, 'market': 0.86546136})
        yield Record({'start' : '001200', 'rate': 0.835, 'rate000': 1.5658, 'channel': 107, 'area': 3, 'date': 20111117, 'length': 200, 'market': 0.1655})
        yield Record({'start' : '234247', 'rate': 0.431, 'rate000': 1.6849, 'channel': 106, 'area': 5, 'date': 20111118, 'length': 24, 'market': 0.894782})
        yield Record({'start' : '001200', 'rate': 0.564, 'rate000': 6.428, 'channel': 102, 'area': 3, 'date': 20111120, 'length': 33, 'market': 0.8022249})
        yield Record({'start' : '001200', 'rate': 0.0606, 'rate000': 7.8873, 'channel': 106, 'area': 3, 'date': 20111121, 'length': 42, 'market': 0.2595468})
        yield Record({'start' : '001200', 'rate': 0.523, 'rate000': 8.9123, 'channel': 106, 'area': 1, 'date': 20111121, 'length': 156, 'market': 0.6883453})
        yield Record({'start' : '001200', 'rate': 0.765, 'rate000': 0.044051, 'channel': 106, 'area': 3, 'date': 20111118, 'length': 11, 'market': 0.43724})
        yield Record({'start' : '001200', 'rate': 0.0238, 'rate000': 3.8409, 'channel': 110, 'area': 5, 'date': 20111120, 'length': 46, 'market': 0.73827705})
        yield Record({'start' : '001200', 'rate': 0.562, 'rate000': 0.82476, 'channel': 102, 'area': 1, 'date': 20111118, 'length': 74, 'market': 0.1932})
        yield Record({'start' : '001200', 'rate': 0.447, 'rate000': 4.7711, 'channel': 107, 'area': 1, 'date': 20111120, 'length': 188, 'market': 0.215904694})
        yield Record({'start' : '001200', 'rate': 0.997, 'rate000': 0.22331, 'channel': 107, 'area': 3, 'date': 20111121, 'length': 159, 'market': 0.582725})
        yield Record({'start' : '001200', 'rate': 0.147, 'rate000': 6.5493, 'channel': 102, 'area': 5, 'date': 20111117, 'length': 84, 'market': 0.54071497})
        yield Record({'start' : '001200', 'rate': 0.226, 'rate000': 1.0678, 'channel': 110, 'area': 1, 'date': 20111117, 'length': 96, 'market': 0.62115933})
        yield Record({'start' : '001200', 'rate': 0.173, 'rate000': 4.2589, 'channel': 108, 'area': 1, 'date': 20111117, 'length': 109, 'market': 0.77639889})
        yield Record({'start' : '001200', 'rate': 0.388, 'rate000': 4.735, 'channel': 106, 'area': 1, 'date': 20111121, 'length': 134, 'market': 0.60228487})
        yield Record({'start' : '001200', 'rate': 0.869, 'rate000': 3.4194, 'channel': 108, 'area': 1, 'date': 20111120, 'length': 178, 'market': 0.1776002})
        yield Record({'start' : '001200', 'rate': 0.284, 'rate000': 1.3945, 'channel': 107, 'area': 1, 'date': 20111119, 'length': 88, 'market': 0.9283678})
        yield Record({'start' : '001200', 'rate': 0.279, 'rate000': 2.4186, 'channel': 106, 'area': 1, 'date': 20111118, 'length': 89, 'market': 0.1726923})
        yield Record({'start' : '234247', 'rate': 0.669, 'rate000': 1.3355, 'channel': 106, 'area': 5, 'date': 20111117, 'length': 23, 'market': 0.6547277})
        yield Record({'start' : '001200', 'rate': 0.544, 'rate000': 5.8991, 'channel': 107, 'area': 1, 'date': 20111120, 'length': 200, 'market': 0.4079244})
        yield Record({'start' : '001200', 'rate': 0.414, 'rate000': 7.8017, 'channel': 108, 'area': 5, 'date': 20111118, 'length': 142, 'market': 0.2624028})
        yield Record({'start' : '001200', 'rate': 0.421, 'rate000': 0.918, 'channel': 102, 'area': 5, 'date': 20111118, 'length': 28, 'market': 0.51881})
        yield Record({'start' : '001200', 'rate': 0.794, 'rate000': 2.0751, 'channel': 107, 'area': 3, 'date': 20111120, 'length': 40, 'market': 0.16973})
        yield Record({'start' : '001200', 'rate': 0.639, 'rate000': 7.9218, 'channel': 107, 'area': 1, 'date': 20111120, 'length': 64, 'market': 0.61352})
        yield Record({'start' : '001200', 'rate': 0.311, 'rate000': 3.6826, 'channel': 106, 'area': 5, 'date': 20111119, 'length': 103, 'market': 0.85763})
        yield Record({'start' : '001200', 'rate': 0.153, 'rate000': 5.598, 'channel': 107, 'area': 1, 'date': 20111121, 'length': 33, 'market': 0.45737})
        yield Record({'start' : '123123', 'rate': 0.628, 'rate000': 0.1818, 'channel': 108, 'area': 5, 'date': 20111118, 'length': 75, 'market': 0.23009})
        yield Record({'start' : '001200', 'rate': 0.247, 'rate000': 2.6869, 'channel': 106, 'area': 1, 'date': 20111120, 'length': 137, 'market': 0.97654})
        yield Record({'start' : '001200', 'rate': 0.86, 'rate000': 1.5401, 'channel': 106, 'area': 5, 'date': 20111121, 'length': 81, 'market': 0.5404565971})
        yield Record({'start' : '001200', 'rate': 0.931, 'rate000': 0.9819, 'channel': 110, 'area': 5, 'date': 20111120, 'length': 196, 'market': 0.760582})
        yield Record({'start' : '001200', 'rate': 0.0114, 'rate000': 1.4758, 'channel': 102, 'area': 5, 'date': 20111119, 'length': 85, 'market': 0.565402})
        yield Record({'start' : '001200', 'rate': 0.611, 'rate000': 1.8474, 'channel': 106, 'area': 5, 'date': 20111117, 'length': 110, 'market': 0.756885})
        yield Record({'start' : '001200', 'rate': 0.997, 'rate000': 1.9782, 'channel': 107, 'area': 3, 'date': 20111120, 'length': 35, 'market': 0.944514})
        yield Record({'start' : '001200', 'rate': 0.155, 'rate000': 4.8891, 'channel': 106, 'area': 3, 'date': 20111119, 'length': 182, 'market': 0.704212})
        yield Record({'start' : '001200', 'rate': 0.993, 'rate000': 8.6424, 'channel': 110, 'area': 5, 'date': 20111121, 'length': 94, 'market': 0.988477})
        yield Record({'start' : '001200', 'rate': 0.256, 'rate000': 4.0653, 'channel': 102, 'area': 5, 'date': 20111119, 'length': 122, 'market': 0.587296})
        yield Record({'start' : '234247', 'rate': 0.715, 'rate000': 2.0196, 'channel': 110, 'area': 5, 'date': 20111121, 'length': 32, 'market': 0.619468})
        yield Record({'start' : '001200', 'rate': 0.907, 'rate000': 1.8547, 'channel': 107, 'area': 3, 'date': 20111118, 'length': 43, 'market': 0.208099})
        yield Record({'start' : '001200', 'rate': 0.842, 'rate000': 4.3236, 'channel': 106, 'area': 3, 'date': 20111119, 'length': 95, 'market': 0.833692})
        raise StopIteration
class Unpackage(DataGrid):
    def __init__(self, Groupby, separator, data): self.f, self.sep, self.data = list(Groupby), separator, data
    def __del__(self): del self.f, self.sep, self.data
    def next(self):
        r = {}
        for k in self.data:
            r.clear()
            for i in self.data[k]: r[i] = self.data[k][i].GetResult()
            yield Record(r)
        raise StopIteration
class Boolean:
    def bool(self, record): return True
class Operator:
    def __init__(self): self.data = None
    def emit(self, record): pass
    def GetValue(self, record):
        self.emit(record)
        return self.GetResult()
    def GetResult(self): return self.data
class Field(Operator):
    def __init__(self, field): self.field, self.data = field, None
    def emit(self, record): self.data = record.v(self.field)
    def Clone(self): return Field(self.field)
class SUM(Operator):
    def __init__(self, node): self.node, self.data = Field(node) if isinstance(node, str) else node, 0
    def emit(self, record):
        self.node.emit(record)
        self.data += float(self.node.GetResult())
    def Clone(self): return SUM(self.node.Clone())
class COUNT(Operator):
    def __init__(self, node): self.node, self.data = Field(node) if isinstance(node, str) else node, 0
    def emit(self, record):
        self.node.emit(record)
        self.data += 1
    def Clone(self): return COUNT(self.node.Clone())
class AVG(Operator):
    def __init__(self, node): self.node, self.sum, self.count = Field(node) if isinstance(node, str) else node, 0, 0
    def emit(self, record):
        self.node.emit(record)
        self.sum, self.count = self.sum + float(self.node.GetValue(record)), self.count + 1
    def GetResult(self): return self.sum/self.count
    def Clone(self): return AVG(self.node.Clone())
class MUL(Operator):
    def __init__(self, node1, node2): self.nodes, self.data = [Field(node1) if isinstance(node1, str) else node1, Field(node2) if isinstance(node2, str) else node2], 0
    def emit(self, record):
        for i in self.nodes: i.emit(record)
    def GetResult(self): return float(self.nodes[0].GetResult()) * float(self.nodes[1].GetResult())
    def Clone(self): return MUL(self.nodes[0].Clone(), self.nodes[1].Clone())
class DIV(Operator):
    def __init__(self, node1, node2): self.nodes, self.data = [Field(node1) if isinstance(node1, str) else node1, Field(node2) if isinstance(node2, str) else node2], 0
    def emit(self, record):
        for i in self.nodes: i.emit(record)
    def GetResult(self): return float(self.nodes[0].GetResult()) / float(self.nodes[1].GetResult())
    def Clone(self): return DIV(self.nodes[0].Clone(), self.nodes[1].Clone())
class ADD(Operator):
    def __init__(self, node1, node2): self.nodes, self.data = [Field(node1) if isinstance(node1, str) else node1, Field(node2) if isinstance(node2, str) else node2], 0
    def emit(self, record):
        for i in self.nodes: i.emit(record)
    def GetResult(self): return float(self.nodes[0].GetResult()) + float(self.nodes[1].GetResult())
    def Clone(self): return ADD(self.nodes[0].Clone(), self.nodes[1].Clone())
class PLU(Operator):
    def __init__(self, node1, node2): self.nodes, self.data = [Field(node1) if isinstance(node1, str) else node1, Field(node2) if isinstance(node2, str) else node2], 0
    def emit(self, record):
        for i in self.nodes: i.emit(record)
    def GetResult(self): return float(self.nodes[0].GetResult()) - float(self.nodes[1].GetResult())
    def Clone(self): return PLU(self.nodes[0].Clone(), self.nodes[1].Clone())
class Worker:
    def GROUP(self, Select = {}, From = TestSourceData(), Where = Boolean(), Groupby = [], Having = Boolean(), Sortby = [], Skip = 0, Limit = 10):
        data = {}
        if Groupby != None:
            db = []
            for record in From:
                if not Where.bool(record): continue
                key = ','.join([str(record.v(i)) for i in Groupby])
                if key not in data:
                    data[key] = {}
                    for s in Select: data[key][s] = Field(Select[s]) if isinstance(Select[s], str) else Select[s].Clone()
                for k in data[key]: data[key][k].emit(record)
            for result in Unpackage(Groupby, ",", data):
                if not Having.bool(result): continue
                yield result
        else:
            r, S, re = {}, {}, None
            for s in Select: S[s] = Field(Select[s]) if isinstance(Select[s], str) else Select[s].Clone()
            db = []
            for record in From:
                if not Where.bool(record): continue
                r.clear()
                for s in S: r[s] = S[s].GetValue(record)
                re = Record(r)
                if not Having.bool(re): continue
                yield re
        raise StopIteration
    def JOIN(self, Select = {}, Left = TestSourceData(), Right = TestSourceData(), On = Boolean(), Where = Boolean(), Sortby = [], Skip = 0, Limit = 10): raise StopInteration
    def _parse(self, input):
        # Read parameters from input, here are some test cases.
        Select = {
            'AREA' : 'area',
            'CHANNEL' : 'channel',
            'date' : 'date',
            'COUNT' : COUNT('area'),
            'SUM' : SUM('length'),
            'AVG' : AVG('start'),
            'RATE' : DIV(SUM(MUL('rate', 'length')), SUM('length')),
            'RATE000' : DIV(SUM(MUL('rate000', 'length')), SUM('length')),
            'MARKET' : DIV(SUM(MUL('market', 'length')), SUM('length')),
        }
        From = TestSourceData()
        Groupby = [ 'area', 'channel', 'date', ]
        Sortby, Skip, Limit = [], 0, 10 # should be implent
        Where, Having = Boolean(), Boolean() # not implement
        c = self.GROUP(Select, From, Where, Groupby, Having, Sortby, Skip, Limit)
        return list(c)
    def Process(self, input):
        try: return json.dumps([i.d for i in list(self._parse(json.loads(input)))])
        except Exception, e:
            print traceback.format_exc()
            return json.dumps({'ReturnCode':100, 'ReturnMessage':str(e)})
if __name__ == '__main__': Main(DEFAULT_HOST if len(sys.argv) <= 1 else sys.argv[1], DEFAULT_PORT if len(sys.argv) <= 2 else sys.argv[2])
