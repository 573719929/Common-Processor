#!/usr/bin/env python
# -*- coding : utsf8 -*-
import sys, json, random
sys.path.append('./gen-py')
from DataAccess import QueryProcessorService
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from hbase import Hbase
from hbase.ttypes import *

def Main(host, port):
    server = TServer.TSimpleServer(QueryProcessorService.Processor(Worker()), TSocket.TServerSocket(None if host == None else str(host), int(port)), TTransport.TBufferedTransportFactory(), TBinaryProtocol.TBinaryProtocolFactory())
    server.serve()
    print "Query Service : Host = %s, Port = %d, Worker = QueryServer" % (host, port)

class Record:
    def __init__(self, d): self.d = dict(d)
    def v(self, k): return self.d[k] if k in self.d else None
    def __del__(self): del self.d
    def __str__(self): return str(self.d)

class DataGrid:
    def __init__(self): pass
    def __iter__(self): return self.next()
    def next(self): raise StopIteration


class TestSourceData(DataGrid):
    def next(self):
        a = '''
        AREA = [1,3,5]
        DATE = [20111117, 20111118, 20111119, 20111120, 20111121]
        CHANNEL = [102,106,107,108,110]
        for i in range(50):
            area, date, channel, length, rate, rate000, market = random.choice(AREA), random.choice(DATE), random.choice(CHANNEL), random.randint(10,200), random.random(), random.random()*random.randint(1,10), random.random()
            yield Record({'area':area, 'date':date, 'channel':channel, 'length':length, 'rate':rate, 'rate000':rate000, 'market':market})
        '''
        yield Record({'rate': 0.58661178387938018, 'rate000': 0.47562600021553658, 'channel': 102, 'area': 1, 'date': 20111120, 'length': 102, 'market': 0.025246491183763764})
        yield Record({'rate': 0.95033357629614212, 'rate000': 0.32299559640749376, 'channel': 102, 'area': 3, 'date': 20111117, 'length': 68, 'market': 0.24741093960806448})
        yield Record({'rate': 0.75023232397635198, 'rate000': 1.9412696618486769, 'channel': 108, 'area': 5, 'date': 20111117, 'length': 178, 'market': 0.9791882584197068})
        yield Record({'rate': 0.86705583820402277, 'rate000': 0.29060617273228484, 'channel': 110, 'area': 1, 'date': 20111120, 'length': 73, 'market': 0.87893370426012829})
        yield Record({'rate': 0.94112435528643368, 'rate000': 0.1801840986933384, 'channel': 110, 'area': 3, 'date': 20111120, 'length': 83, 'market': 0.63740030743734755})
        yield Record({'rate': 0.34276170943840067, 'rate000': 4.6412161746240148, 'channel': 107, 'area': 3, 'date': 20111120, 'length': 40, 'market': 0.32446509703311521})
        yield Record({'rate': 0.0085821973127746531, 'rate000': 0.55499054905014311, 'channel': 106, 'area': 1, 'date': 20111119, 'length': 110, 'market': 0.76167447318111692})
        yield Record({'rate': 0.65619920285749234, 'rate000': 0.60641735144397679, 'channel': 108, 'area': 1, 'date': 20111119, 'length': 21, 'market': 0.59106284147039456})
        yield Record({'rate': 0.82166205177786467, 'rate000': 3.4550246445233546, 'channel': 106, 'area': 1, 'date': 20111119, 'length': 30, 'market': 0.69679800581402895})
        yield Record({'rate': 0.055760635873697617, 'rate000': 0.60976969720622187, 'channel': 102, 'area': 5, 'date': 20111121, 'length': 147, 'market': 0.56988625891973288})
        yield Record({'rate': 0.71312729741540515, 'rate000': 6.096730068010479, 'channel': 110, 'area': 3, 'date': 20111118, 'length': 119, 'market': 0.023407985887756277})
        yield Record({'rate': 0.7333698607377348, 'rate000': 1.0771239208066559, 'channel': 102, 'area': 1, 'date': 20111119, 'length': 82, 'market': 0.86546155289793736})
        yield Record({'rate': 0.83058858609124875, 'rate000': 1.5641814414281658, 'channel': 107, 'area': 3, 'date': 20111117, 'length': 200, 'market': 0.16858657551985523})
        yield Record({'rate': 0.43509025886071861, 'rate000': 1.6871075532632049, 'channel': 106, 'area': 5, 'date': 20111118, 'length': 24, 'market': 0.89473185530229182})
        yield Record({'rate': 0.56116886762143714, 'rate000': 6.422468138331098, 'channel': 102, 'area': 3, 'date': 20111120, 'length': 33, 'market': 0.80226299604835249})
        yield Record({'rate': 0.063862951466028606, 'rate000': 7.8477845819108873, 'channel': 106, 'area': 3, 'date': 20111121, 'length': 42, 'market': 0.25997801983195468})
        yield Record({'rate': 0.52052554761912873, 'rate000': 8.9107070716289023, 'channel': 106, 'area': 1, 'date': 20111121, 'length': 156, 'market': 0.68740703387283453})
        yield Record({'rate': 0.76748043917418285, 'rate000': 0.048826398861544051, 'channel': 106, 'area': 3, 'date': 20111118, 'length': 11, 'market': 0.43724623508741889})
        yield Record({'rate': 0.020915298464311838, 'rate000': 3.8625859705312409, 'channel': 110, 'area': 5, 'date': 20111120, 'length': 46, 'market': 0.73809944030127705})
        yield Record({'rate': 0.56253059195689692, 'rate000': 0.82428824588160476, 'channel': 102, 'area': 1, 'date': 20111118, 'length': 74, 'market': 0.12601670551945932})
        yield Record({'rate': 0.44255993946777927, 'rate000': 4.7717144068718911, 'channel': 107, 'area': 1, 'date': 20111120, 'length': 188, 'market': 0.21527475798904694})
        yield Record({'rate': 0.99708514670672677, 'rate000': 0.22863250424993331, 'channel': 107, 'area': 3, 'date': 20111121, 'length': 159, 'market': 0.58216322887752725})
        yield Record({'rate': 0.14369105440061347, 'rate000': 6.5488570069681993, 'channel': 102, 'area': 5, 'date': 20111117, 'length': 84, 'market': 0.54074483350951497})
        yield Record({'rate': 0.22833644441519096, 'rate000': 1.0628363593300478, 'channel': 110, 'area': 1, 'date': 20111117, 'length': 96, 'market': 0.62110116127545933})
        yield Record({'rate': 0.17900451451945343, 'rate000': 4.2523539572668989, 'channel': 108, 'area': 1, 'date': 20111117, 'length': 109, 'market': 0.77606102073839889})
        yield Record({'rate': 0.38204296158936768, 'rate000': 4.733371576812285, 'channel': 106, 'area': 1, 'date': 20111121, 'length': 134, 'market': 0.60225479131298487})
        yield Record({'rate': 0.86250420946254569, 'rate000': 3.4161828086150994, 'channel': 108, 'area': 1, 'date': 20111120, 'length': 178, 'market': 0.17761285774722002})
        yield Record({'rate': 0.28930135096284004, 'rate000': 1.3927946124181245, 'channel': 107, 'area': 1, 'date': 20111119, 'length': 88, 'market': 0.92836950010537178})
        yield Record({'rate': 0.27834241742537269, 'rate000': 2.4170612928297186, 'channel': 106, 'area': 1, 'date': 20111118, 'length': 89, 'market': 0.17269765179563723})
        yield Record({'rate': 0.66870672918557739, 'rate000': 1.3339466862026155, 'channel': 106, 'area': 5, 'date': 20111117, 'length': 23, 'market': 0.65472045376527377})
        yield Record({'rate': 0.54858940297158254, 'rate000': 5.8986791789943291, 'channel': 107, 'area': 1, 'date': 20111120, 'length': 200, 'market': 0.40792273172381244})
        yield Record({'rate': 0.41667202232306144, 'rate000': 7.8052486953184417, 'channel': 108, 'area': 5, 'date': 20111118, 'length': 142, 'market': 0.26245987302307028})
        yield Record({'rate': 0.42147186158361361, 'rate000': 0.910045980187698, 'channel': 102, 'area': 5, 'date': 20111118, 'length': 28, 'market': 0.51881659733085961})
        yield Record({'rate': 0.79858398799799324, 'rate000': 2.0775937857132551, 'channel': 107, 'area': 3, 'date': 20111120, 'length': 40, 'market': 0.16958780064951373})
        yield Record({'rate': 0.63301740466300049, 'rate000': 7.9282827187166118, 'channel': 107, 'area': 1, 'date': 20111120, 'length': 64, 'market': 0.61354215930171752})
        yield Record({'rate': 0.31757797718385161, 'rate000': 3.6825883774824226, 'channel': 106, 'area': 5, 'date': 20111119, 'length': 103, 'market': 0.85542460663181763})
        yield Record({'rate': 0.15864964655234903, 'rate000': 5.592211475600668, 'channel': 107, 'area': 1, 'date': 20111121, 'length': 33, 'market': 0.45736969179003817})
        yield Record({'rate': 0.62075995601089318, 'rate000': 0.1885156828992518, 'channel': 108, 'area': 5, 'date': 20111118, 'length': 75, 'market': 0.23075274305479909})
        yield Record({'rate': 0.24378574517552087, 'rate000': 2.6838051242839969, 'channel': 106, 'area': 1, 'date': 20111120, 'length': 137, 'market': 0.97701224710961654})
        yield Record({'rate': 0.8668039076528864, 'rate000': 1.5408306891894901, 'channel': 106, 'area': 5, 'date': 20111121, 'length': 81, 'market': 0.5404567459565971})
        yield Record({'rate': 0.93703828236283371, 'rate000': 0.9824043106401219, 'channel': 110, 'area': 5, 'date': 20111120, 'length': 196, 'market': 0.76073043059509482})
        yield Record({'rate': 0.013814804508760714, 'rate000': 1.4172028322981758, 'channel': 102, 'area': 5, 'date': 20111119, 'length': 85, 'market': 0.56296691540107602})
        yield Record({'rate': 0.61143091855522591, 'rate000': 1.8480764884643774, 'channel': 106, 'area': 5, 'date': 20111117, 'length': 110, 'market': 0.75211398685542585})
        yield Record({'rate': 0.99524309579772097, 'rate000': 1.9714529195849382, 'channel': 107, 'area': 3, 'date': 20111120, 'length': 35, 'market': 0.94460097451664904})
        yield Record({'rate': 0.15063557281039175, 'rate000': 4.8833282239841891, 'channel': 106, 'area': 3, 'date': 20111119, 'length': 182, 'market': 0.70142757429862712})
        yield Record({'rate': 0.99208113285485133, 'rate000': 8.6498859941044124, 'channel': 110, 'area': 5, 'date': 20111121, 'length': 94, 'market': 0.98809393247757327})
        yield Record({'rate': 0.25484137186580946, 'rate000': 4.0607426672285953, 'channel': 102, 'area': 5, 'date': 20111119, 'length': 122, 'market': 0.58862772723755596})
        yield Record({'rate': 0.71369795151370685, 'rate000': 2.0194185564652596, 'channel': 110, 'area': 5, 'date': 20111121, 'length': 32, 'market': 0.61923500346890548})
        yield Record({'rate': 0.90874766925001937, 'rate000': 1.8597368907238847, 'channel': 107, 'area': 3, 'date': 20111118, 'length': 43, 'market': 0.20881103509290289})
        yield Record({'rate': 0.84534880251544342, 'rate000': 4.3232843769310136, 'channel': 106, 'area': 3, 'date': 20111119, 'length': 95, 'market': 0.83313534669197042})
        raise StopIteration

class Unpackage(DataGrid):
    def __init__(self, Groupby, separator, data): self.f, self.sep, self.data = list(Groupby), separator, data
    def __del__(self): del self.f, self.sep, self.data
    def next(self):
        r = {}
        for k in self.data:
            r.clear()
            for i in self.data[k]: r[i] = self.data[k][i].GetValue(None)
            yield Record(r)
        raise StopIteration

class Boolean:
    def __init__(self): pass
    def bool(self, record): return True

class Field:
    def __init__(self, field): self.field, self.data = field, None
    def GetValue(self, record):
        if record == None: return self.data
        else: return record.v(self.field)
    def GetResult(self): return self.data
    def Clone(self): return Field(self.field)
    def emit(self, record): self.data = record.v(self.field)
class Operator:
    def __init__(self): pass
class SUM(Operator):
    def __init__(self, node): self.node, self.data = node, 0
    def emit(self, record): self.data += float(self.node.GetValue(record))
    def Clone(self): return SUM(self.node.Clone())
    def GetValue(self, ig = None): return self.GetResult()
    def GetResult(self): return self.data
class AVG(Operator):
    def __init__(self, node): self.node, self.sum, self.count = node, 0, 0
    def emit(self, record):
        self.sum += float(self.node.GetValue(record))
        self.count += 1
    def GetValue(self, ig = None): return self.GetResult()
    def GetResult(self): return self.sum/self.count
    def Clone(self): return AVG(self.node.Clone())
class MUL(Operator):
    def __init__(self, node1, node2): self.node1, self.node2, self.data = node1, node2, 0
    def emit(self, record):
        self.node1.emit(record)
        self.node2.emit(record)
    def GetValue(self, record): return float(self.node1.GetValue(record)) * float(self.node2.GetValue(record))
    def GetResult(self): return self.data
    def Clone(self): return MUL(self.node1.Clone(), self.node2.Clone())
class DIV(Operator):
    def __init__(self, node1, node2): self.node1, self.node2, self.data = node1, node2, 0
    def emit(self, record):
        self.node1.emit(record)
        self.node2.emit(record)
    def GetValue(self, record):
        
        return float(self.node1.GetValue(record)) / float(self.node2.GetValue(record))
    def GetResult(self):
        
        return self.data
    def Clone(self):
        
        return DIV(self.node1.Clone(), self.node2.Clone())
class COUNT(Operator):
    def __init__(self, node): self.node, self.count = node, 0
    def emit(self, record):
        self.count += 1
    def GetValue(self, ig = None): return self.GetResult()
    def GetResult(self): return self.count
    def Clone(self): return COUNT(self.node.Clone())
class Worker:
    def _proc(self, Select, From, Where, Groupby, Having, Sortby, Skip, Limit):
        data = {}
        print '*'*128

        for record in From:
            if not Where.bool(record):
                print '[pass]', record
                continue
            print '<in>', record
            key = ','.join([str(record.v(i)) for i in Groupby])
            if key not in data:
                data[key] = {}
                for k in Select: data[key][k] = Select[k].Clone()
            for k in data[key]:
                
                data[key][k].emit(record)
        print '*'*128
        for result in Unpackage(Groupby, ",", data):
            if not Having.bool(result):
                print '[drop]', result
                continue
            print '<out>', result
        print '*'*128
        print
        return None
    def _parse(self, input):
        # Read parameters from input, here are some test cases.
        
        # proc
        Select = {
            'AREA' : Field('area'),
            #'DATE' : Field('date'),
            #'CHANNEL' : Field('channel'),
            
            'LENGTH' : SUM(Field('length')),
            'COUNT' : COUNT(Field('length')),
            
            'AVG_RATE': DIV(SUM(MUL(Field('rate'), Field('length'))), SUM(Field('length'))),
            'AVG_RATE000': DIV(SUM(MUL(Field('rate000'), Field('length'))), SUM(Field('length'))),
            'AVG_MARKET': DIV(SUM(MUL(Field('market'), Field('length'))), SUM(Field('length'))),
        }
        
        # proc
        From = TestSourceData()
        
        # not implement
        Where = Boolean()
        
        # proc
        Groupby = [ 'area', 'date', 'channel' ]
        Groupby = [ 'area' ]
        
        # not implement
        Having = Boolean()
        
        # not implement
        Sortby = []
        
        # not implement
        Skip = 0
        
        # not implement
        Limit = 10
        
        # test cases end
        return Select, From, Where, Groupby, Having, Sortby, Skip, Limit
    def Process(self, input):
        INPUT = json.loads(input)
        Select, From, Where, Groupby, Having, Sortby, Skip, Limit = self._parse(INPUT)
        OUTPUT = self._proc(Select, From, Where, Groupby, Having, Sortby, Skip, Limit)
        return json.dumps(OUTPUT)

if __name__ == '__main__': Main(None, 37100)
