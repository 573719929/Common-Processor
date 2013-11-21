#!/usr/bin/env python
# -*- coding : utf8 -*-
class BOOL:
    def __str__(self): return 'a'
class AND(BOOL):
    def __init__(self, *args): self.args = args
    def __str__(self): return '(%s)'%(' AND '.join([str(i) for i in self.args]))
class OR(BOOL):
    def __init__(self, *args): self.args = args
    def __str__(self): return '(%s)'%(' OR '.join([str(i) for i in self.args]))
class GT(BOOL):
    def __init__(self, a, b, c): self.a, self.b, self.c = a, b, c
    def __str__(self): return '(%s:%s > %s)'%(self.a, self.b, self.c)
class LT(BOOL):
    def __init__(self, a, b, c): self.a, self.b, self.c = a, b, c
    def __str__(self): return "(SingleColumnValueFilter ('%s', '%s', <, '%s'))"%(self.a, self.b, self.c)
class EQ(BOOL):
    def __init__(self, a, b, c): self.a, self.b, self.c = a, b, c
    def __str__(self): return '(%s:%s = %s)'%(self.a, self.b, self.c)
class NEQ(BOOL):
    def __init__(self, a, b, c): self.a, self.b, self.c = a, b, c
    def __str__(self): return '(%s:%s != %s)'%(self.a, self.b, self.c)
class IN(BOOL):
    def __init__(self, a, b, c): self.a, self.b, self.c = a, b, c
    def __str__(self): return str(OR(*[EQ(self.a, self.b, str(i)) for i in self.c]))
class BITAND(BOOL):
    def __init__(self, a, b, c): self.a, self.b, self.c = a, b, c
    def __str__(self): return '(%s:%s && %s)'%(self.a, self.b, self.c)
class BITOR(BOOL):
    def __init__(self, a, b, c): self.a, self.b, self.c = a, b, c
    def __str__(self): return "(SingleColumnValueFilter ('%s', '%s', <=, '%s'))"%(self.a, self.b, self.c)
def foobar(parameters):
    ef = []
    if 'area' in parameters:
        ef.append(EQ('CF', 'area', parameters['area']))
    if 'dayslot' in parameters:
        ef.append(AND(GT('CF', 'day', parameters['dayslot'][0]), LT('CF', 'day', parameters['dayslot'][1])))
    if 'timeslot' in parameters:
        ef.append(OR(AND(GT('CF', 'start', parameters['timeslot'][0]), LT('CF', 'start', parameters['timeslot'][1])), AND(GT('CF', 'end', parameters['timeslot'][0]), LT('CF', 'end', parameters['timeslot'][1]))))
    if 'channel' in parameters:
        ef.append(IN('CF', 'channel', parameters['channel']))
    return AND(*ef)
#
parameters = {
    'channel' : ['1', '2', '3'],
    'area' : '123',
    'dayslot' : ['20131120', '20131130'],
    'timeslot' : ['002200', '002400'],
}
print foobar(parameters)
