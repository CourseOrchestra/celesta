from g1._g1_orm import aaCursor 
from g2._g2_orm import bCursor

def hello(context, arg):
    print 'Hello, world from Celesta Python procedure.'
    print 'user %s' % context.userId
    print 'Argument passed was "%s".' % arg
    aa = aaCursor(context)
    aa.deleteAll()
    for i in range(1, 12):
        aa.idaa = i 
        aa.idc = i * i
        aa.insert()
    
    b = bCursor(context)
    b.deleteAll()
    b.descr = 'AB'
    b.insert()
    
    while aa.next():
        print "%s : %s" % (aa.idaa , aa.idc)
        
    print 'Python procedure finished.'
    
def testTrigger(rec):
    print 'Test trigger is run with idc = %s' % rec.idc