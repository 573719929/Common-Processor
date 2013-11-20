#!/usr/bin/env python
# -*- coding : utsf8 -*-
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
    transport = TTransport.TBufferedTransport(TSocket.TSocket('localhost', 37100))
    client = QueryProcessorService.Client(TBinaryProtocol.TBinaryProtocol(transport))
    
    transport.open()
    print client.Process(json.dumps(""))
    transport.close()
 
except Thrift.TException, ex:
  print "%s" % (ex.message)