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
    print client.Process(json.dumps({'a':1, 'b':2}))
    transport.close()
 
except Thrift.TException, ex:
  print "%s" % (ex.message)