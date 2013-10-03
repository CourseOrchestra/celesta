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
    print 'PRE B XREC: %s, REC: %s' %  (b.getXRec().asCSVLine(), b.asCSVLine())
    b.insert()
    oldid = b.idb
    
    print 'POST B XREC: %s, REC: %s' %  (b.getXRec().asCSVLine(), b.asCSVLine())
    b.idb = None
    b.descr = 'CD'
    print 'PRE B XREC: %s, REC: %s' %  (b.getXRec().asCSVLine(), b.asCSVLine())
    b.insert()
    print 'POST B XREC: %s, REC: %s' %  (b.getXRec().asCSVLine(), b.asCSVLine())
    
    b.get(oldid)
    b.descr = 'EF'
    print 'PRE UPDATE B XREC: %s, REC: %s' %  (b.getXRec().asCSVLine(), b.asCSVLine())
    b.update()
    print 'POST UPDATE B XREC: %s, REC: %s' %  (b.getXRec().asCSVLine(), b.asCSVLine())
    
    while aa.next():
        print "%s : %s" % (aa.idaa , aa.idc)
        
    print 'Python procedure finished.'
    
def testTrigger(rec):
    print 'Test trigger is run with idc = %s' % rec.idc