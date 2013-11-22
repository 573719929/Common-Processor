#!/usr/bin/env python
#-*- coding:utf-8 -*-
DEFAULT_HOST, DEFAULT_PORT = None, 7729
import sys, json, random, traceback
sys.path.append('./gen-py')
from DataAccess import QueryProcessorService
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from hbase import Hbase
from hbase.ttypes import *
def Main(host, port): TServer.TSimpleServer(QueryProcessorService.Processor(Worker()), TSocket.TServerSocket(None if host == None or host == '' else str(host), int(port)), TTransport.TBufferedTransportFactory(), TBinaryProtocol.TBinaryProtocolFactory()).serve()

def ConditionToFilter(condition):
    ef = []
    for field in condition:
        if field == 'timeslot': ef.append(OR(AND(GT('CF', 'start', condition['timeslot'][0]), LT('CF', 'start', condition['timeslot'][1])), AND(GT('CF', 'end', condition['timeslot'][0]), LT('CF', 'end', condition['timeslot'][1]))))
        elif field == 'dayslot': ef.append(AND(GT('CF', 'day', condition['dayslot'][0]), LT('CF', 'day', condition['dayslot'][1])))
        elif field == 'subject': pass
        elif isinstance(condition[field], list) or isinstance(condition[field], tuple): ef.append(OR(*[EQ('CF', field, i) for i in condition[field]]))
        else: ef.append(EQ('CF', field, str(condition[field])))
    return None if len(ef) == 0 else AND(*ef)
class Record(dict):
    def v(self, k): return self[k] if k in self else None
class DataGrid:
    def __init__(self): pass
    def __iter__(self): return self.next()
    def next(self): raise StopIteration
class BasicDataGrid(DataGrid):
    def __init__(self, data): self.data = data
    def __iter__(self): return iter(self.data)
class BOOL:
    def __init__(self): pass
    def __del__(self): pass
    def __str__(self): return None
class AND(BOOL):
    def __init__(self, *args): self.args = args
    def __str__(self): return '('+' AND '.join([str(i) for i in self.args])+')'
class OR(BOOL):
    def __init__(self, *args): self.args = args
    def __str__(self): return '('+' OR '.join([str(i) for i in self.args])+')'
class GT(BOOL):
    def __init__(self, cf, qf, value): self.cf, self.qf, self.value = cf, qf, value
    def __str__(self): return "SingleColumnValueFilter ('%s', '%s', >, 'binary:%s')"%(self.cf, self.qf, self.value)
class LT(BOOL):
    def __init__(self, cf, qf, value): self.cf, self.qf, self.value = cf, qf, value
    def __str__(self): return "SingleColumnValueFilter ('%s', '%s', <, 'binary:%s')"%(self.cf, self.qf, self.value)
class GTE(BOOL):
    def __init__(self, cf, qf, value): self.cf, self.qf, self.value = cf, qf, value
    def __str__(self): return "SingleColumnValueFilter ('%s', '%s', >=, 'binary:%s')"%(self.cf, self.qf, self.value)
class LTE(BOOL):
    def __init__(self, cf, qf, value): self.cf, self.qf, self.value = cf, qf, value
    def __str__(self): return "SingleColumnValueFilter ('%s', '%s', <=, 'binary:%s')"%(self.cf, self.qf, self.value)
class EQ(BOOL):
    def __init__(self, cf, qf, value): self.cf, self.qf, self.value = cf, qf, value
    def __str__(self): return "SingleColumnValueFilter ('%s', '%s', =, 'binary:%s')"%(self.cf, self.qf, self.value)
class NEQ(BOOL):
    def __init__(self, cf, qf, value): self.cf, self.qf, self.value = cf, qf, value
    def __str__(self): return "SingleColumnValueFilter ('%s', '%s', !=, 'binary:%s')"%(self.cf, self.qf, self.value)
class IN(BOOL):
    def __init__(self, a, b, c): self.a, self.b, self.c = a, b, c
    def __str__(self): return '('+' OR '.join([str(EQ(self.a, self.b, i)) for i in self.args])+')'
class HBaseReader(DataGrid):
    def __init__(self, Host, Port, Table, RowKeyPattern, Filter): self.Host, self.Port, self.Table, self.RowKeyPattern, self.Filter = str(Host), int(Port), Table, RowKeyPattern, Filter
    def next(self):
        transport = TTransport.TBufferedTransport(TSocket.TSocket(self.Host, self.Port))
        client = Hbase.Client(TBinaryProtocol.TBinaryProtocol(transport))
        transport.open()
        scan = TScan(filterString = str(self.Filter) if self.Filter != None else None)
        id = client.scannerOpenWithScan(self.Table, scan, None)
        result = client.scannerGet(id)
        while result:
            yield Record(dict([(q, result[0].columns[q].value) for q in result[0].columns]))
            result = client.scannerGet(id)
        raise StopIteration
def FLOAT(f):
    try: return float(f)
    except: return 0.0
class Unpackage(DataGrid):
    def __init__(self, Groupby, separator, data): self.f, self.sep, self.data = list(Groupby), separator, data
    def __del__(self): del self.f, self.sep, self.data
    def next(self):
        for k in self.data: yield Record(dict([(i, self.data[k][i].GetResult()) for i in self.data[k]]))
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
        self.data += FLOAT(self.node.GetResult())
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
        self.sum, self.count = self.sum + FLOAT(self.node.GetValue(record)), self.count + 1
    def GetResult(self): return self.sum/self.count
    def Clone(self): return AVG(self.node.Clone())
class MUL(Operator):
    def __init__(self, node1, node2): self.nodes, self.data = [Field(node1) if isinstance(node1, str) else node1, Field(node2) if isinstance(node2, str) else node2], 0
    def emit(self, record): map(lambda x: x.emit(record), self.nodes)
    def GetResult(self): return FLOAT(self.nodes[0].GetResult()) * FLOAT(self.nodes[1].GetResult())
    def Clone(self): return MUL(self.nodes[0].Clone(), self.nodes[1].Clone())
class BIGGER(Operator):
    def __init__(self, node1, node2): self.node1, self.node2 = Field(node1) if isinstance(node1, str) else node1, Field(node2) if isinstance(node2, str) else node2
    def emit(self, record):
        self.node1.emit(record)
        self.node2.emit(record)
    def GetResult(self):
        l, r = FLOAT(self.node1.GetResult()), FLOAT(self.node2.GetResult())
        return l if l > r else r
    def Clone(self): return BIGGER(self.node1.Clone(), self.node2.Clone())
class SMALLER(Operator):
    def __init__(self, node1, node2): self.node1, self.node2 = Field(node1) if isinstance(node1, str) else node1, Field(node2) if isinstance(node2, str) else node2
    def emit(self, record):
        self.node1.emit(record)
        self.node2.emit(record)
    def GetResult(self):
        l, r = FLOAT(self.node1.GetResult()), FLOAT(self.node2.GetResult())
        return l if l < r else r
    def Clone(self): return SMALLER(self.node1.Clone(), self.node2.Clone())
class NUMBER(Operator):
    def __init__(self, value): self.value = FLOAT(value)
    def GetResult(self): return self.value
    def Clone(self): return NUMBER(self.value)
class DIV(Operator):
    def __init__(self, node1, node2): self.nodes, self.data = [Field(node1) if isinstance(node1, str) else node1, Field(node2) if isinstance(node2, str) else node2], 0
    def emit(self, record): map(lambda x: x.emit(record), self.nodes)
    def GetResult(self): return FLOAT(self.nodes[0].GetResult()) / FLOAT(self.nodes[1].GetResult())
    def Clone(self): return DIV(self.nodes[0].Clone(), self.nodes[1].Clone())
class Worker:
    def __init__(self):
        self.function = {
            'DayReport' : lambda x: self.DayReport(x),
            'AreaReport' : lambda x: self.AreaReport(x),
            'RenqunReport' : lambda x: self.RenqunReport(x),
            'Detail' : lambda x: self.Detail(x),
            'Summary' : lambda x: self.Summary(x),
        }
    def GROUP(self, Select = {}, From = None, Where = Boolean(), Groupby = [], Having = Boolean(), Sortby = [], Skip = 0, Limit = 10):
        data = {}
        if From != None and Groupby != None:
            for record in From:
                if not Where.bool(record): continue
                key = ','.join([str(record.v(i)) for i in Groupby])
                if key not in data: data[key] = dict([(s, Field(Select[s]) if isinstance(Select[s], str) else Select[s].Clone()) for s in Select])
                for k in data[key]: data[key][k].emit(record)
            for result in Unpackage(Groupby, ",", data):
                if Having.bool(result): yield result
        elif From != None:
            S = dict([(s, Field(Select[s]) if isinstance(Select[s], str) else Select[s].Clone()) for s in Select])
            for record in From:
                if not Where.bool(record): continue
                re = Record(dict([(s, S[s].GetValue(record)) for s in S]))
                if Having.bool(re): yield re
        raise StopIteration
    def DayReport(self, parameters):
        From = HBaseReader(Host = "localhost", Port = 9090, Table = 'dc.CITY.COL.PLAY.ALL', RowKeyPattern = None, Filter = ConditionToFilter(parameters))
        Select = {
            'DATE' : 'CF:date',
            'COUNT' : COUNT('CF:area'),
            'SUM' : SUM('CF:length'),
            'RATE' : DIV(SUM(MUL('CF:rate', 'CF:length')), SUM('CF:length')),
            'RATE000' : DIV(SUM(MUL('CF:rate000', 'CF:length')), SUM('CF:length')),
            'MARKET' : DIV(SUM(MUL('CF:market', 'CF:length')), SUM('CF:length')),
        }
        Groupby =  ['CF:date',]
        Sortby, Skip, Limit, Where, Having =[], 0, 10, Boolean(), Boolean()
        return list(self.GROUP(Select, From, Where, Groupby, Having, Sortby, Skip, Limit))
    def Detail(self, paramters):
        From = HBaseReader(Host = "localhost", Port = 9090, Table = 'dc.CITY.COL.PLAY.ALL', RowKeyPattern = None, Filter = ConditionToFilter(paramters))
        Select = {
            'RQ' : 'CF:rq',
            'AREA' : 'CF:area',
            'NAME' : 'CF:name',
            'PAGE' : 'CF:page',
            'TITLE' : 'CF:title',
            'CHANNEL' : 'CF:channel',
            'DATE' : 'CF:date',
            'WEEKDAY' : 'CF:weekday',
            'START' : 'CF:start',
            'LENGTH' : 'CF:length',
            'END' : 'CF:end',
            'TOPIC' : 'CF:topic',
            'REPLAY' : 'CF:replay',
            'RATE' : 'CF:rate',
            'RATE000' : 'CF:rate000',
            'MARKET' : 'CF:market',
            'SERIALNO' : 'CF:serialno',
        }
        Groupby =  None
        Sortby, Skip, Limit, Where, Having =[], 0, 10, Boolean(), Boolean()
        return list(self.GROUP(Select, From, Where, Groupby, Having, Sortby, Skip, Limit))
    def Summary(self, paramters):
        From = HBaseReader(Host = "localhost", Port = 9090, Table = 'dc.CITY.COL.PLAY.ALL', RowKeyPattern = None, Filter = ConditionToFilter(paramters))
        Select = {
            'RQ' : 'CF:rq',
            'AREA' : 'CF:area',
            'NAME' : 'CF:name',
            'PAGE' : 'CF:page',
            'TITLE' : 'CF:title',
            'CHANNEL' : 'CF:channel',
            'DATE' : 'CF:date',
            'WEEKDAY' : 'CF:weekday',
            'START' : 'CF:start',
            'LENGTH' : 'CF:length',
            'END' : 'CF:end',
            'TOPIC' : 'CF:topic',
            'REPLAY' : 'CF:replay',
            'SERIALNO' : 'CF:serialno',
        }
        Groupby =  None
        Sortby, Skip, Limit, Where, Having =[], 0, 10, Boolean(), Boolean()
        return list(self.GROUP(Select, From, Where, Groupby, Having, Sortby, Skip, Limit))
    def AreaReport(self, paramters):
        From = HBaseReader(Host = "localhost", Port = 9090, Table = 'dc.CITY.COL.PLAY.ALL', RowKeyPattern = None, Filter = ConditionToFilter(paramters))
        Select = {
            'AREA' : 'CF:area',
            'COUNT' : COUNT('CF:area'),
            'SUM' : SUM('CF:length'),
            'RATE' : DIV(SUM(MUL('CF:rate', 'CF:length')), SUM('CF:length')),
            'RATE000' : DIV(SUM(MUL('CF:rate000', 'CF:length')), SUM('CF:length')),
            'MARKET' : DIV(SUM(MUL('CF:market', 'CF:length')), SUM('CF:length')),
        }
        Groupby =  [ 'CF:area',]
        Sortby, Skip, Limit, Where, Having =[], 0, 10, Boolean(), Boolean()
        return list(self.GROUP(Select, From, Where, Groupby, Having, Sortby, Skip, Limit))
    def RenqunReport(self, paramters):
        From = HBaseReader(Host = "localhost", Port = 9090, Table = 'dc.CITY.COL.PLAY.ALL', RowKeyPattern = None, Filter = ConditionToFilter(paramters))
        Select = {
            'RQ' : 'CF:rq',
            'COUNT' : COUNT('CF:area'),
            'SUM' : SUM('CF:length'),
            'RATE' : DIV(SUM(MUL('CF:rate', 'CF:length')), SUM('CF:length')),
            'RATE000' : DIV(SUM(MUL('CF:rate000', 'CF:length')), SUM('CF:length')),
            'MARKET' : DIV(SUM(MUL('CF:market', 'CF:length')), SUM('CF:length')),
        }
        Groupby =  ['CF:rq',]
        Sortby, Skip, Limit, Where, Having =[], 0, 10, Boolean(), Boolean()
        return list(self.GROUP(Select, From, Where, Groupby, Having, Sortby, Skip, Limit))
    def Process(self, input):
        try:
            input = json.loads(input)
            type, version, parameters = input['type'], input['version'], input['parameters']
            f = self.function[type]
            try:
                output = f(dict([(i, input['parameters'][i]) for i in input['parameters'] if i[0] != '_']))
                # cache, sort, page here
                result = output
                currentPage = 1
                # post-processing complete, output
                currentPage, currentSize, totalSize = currentPage, len(result), len(output)
                return json.dumps({'ReturnCode':0, 'ReturnMessage': 'OK', 'Result' : {'currentPage' : currentPage, 'currentSize' : currentSize, 'totalSize' : totalSize, 'data' : result}})
            except Exception, e:
                print traceback.format_exc()
                return json.dumps({'ReturnCode':200, 'ReturnMessage':str(e)})
        except Exception, e:
            print traceback.format_exc()
            return json.dumps({'ReturnCode':100, 'ReturnMessage':'input format error (%s)'%(str(e))})
if __name__ == '__main__': Main(DEFAULT_HOST if len(sys.argv) <= 1 else sys.argv[1], DEFAULT_PORT if len(sys.argv) <= 2 else sys.argv[2])
