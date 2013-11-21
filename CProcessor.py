# -*- coding : utf8 -*-
def BOOL:
    def __init__(self): pass
    def __del__(self): pass
    def __str__(self): return None
def AND(BOOL):
    def __init__(self, **args): self.args = args
    def __str__(self): return '(%s)'%(' AND '.join(['(%s)'%(str(i)) for i in self.args]))
def OR(BOOL):
    def __init__(self, **args): self.args = args
    def __str__(self): return '(%s)'%(' OR '.join(['(%s)'%(str(i)) for i in self.args]))
def GT(a, b, c):
    def __str__(self): return '(%s:%s > %s)'%(a, b, c)
def LT(a, b, c):
    def __str__(self): return '(%s:%s < %s)'%(a, b, c)
def EQ(a, b, c):
    def __str__(self): return '(%s:%s = %s)'%(a, b, c)
def NEQ(a, b, c):
    def __str__(self): return '(%s:%s != %s)'%(a, b, c)
def IN(a, b, c):
    def __str__(self): return OR(*[EQ(a, b, i) for i in c])
def foobar(parameters):
    ef = []
    if 'area' in parameters:
        ef.append(EQ('CF', 'area', parameters['area']))
    if 'dayslot' in parameters:
        ef.append(AND(GT('CF', 'day', parameters['dayslot'][0]), LT('CF', 'day', parameters['dayslot'][1])))
    if 'timeslot' in parameters:
        ef.append(OR(AND(GT('CF', 'time', parameters['timeslot'][0]), LT('CF', 'time', parameters['timeslot'][1])), AND(GT('CF', 'time', parameters['timeslot'][0]), LT('CF', 'time', parameters['timeslot'][1]))))
    return AND(*ef)
#
parameters = {
    'area' : '123',
    'dayslot' : ['20131120', '20131130']
    'timeslot' : ['002200', '002400']
}
print foobar(parameters)
