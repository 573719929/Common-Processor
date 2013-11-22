#!/usr/bin/env python
# -*- coding : utsf8 -*-
DEFAULT_HOST, DEFAULT_PORT = "localhost", 37100
import sys, json
sys.path.append('./gen-py')
from DataAccess import QueryProcessorService
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from hbase import Hbase
from hbase.ttypes import *

 
try:
    transport = TTransport.TBufferedTransport(TSocket.TSocket(DEFAULT_HOST, DEFAULT_PORT))
    client = QueryProcessorService.Client(TBinaryProtocol.TBinaryProtocol(transport))
    
    transport.open()
    for i in json.loads(client.Process(json.dumps({
        'type' : 'abc',
        'version' : '1.0',
        'parameters' : {
            'area' : '20',
            'channel' : ['1000', '2000', '3000'],
            'dayslot' : ['20130913', '20130916'],
            'timeslot' : ['082700', '082799'],
        },
    })))['Result']['data']: print i
    transport.close()
 
except Thrift.TException, ex:
  print "%s" % (ex.message)