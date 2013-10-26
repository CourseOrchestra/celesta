from g1._g1_orm import aaCursor 
from g2._g2_orm import bCursor
from g3._g3_orm import cCursor
import java.io.OutputStreamWriter as OutputStreamWriter
import java.io.InputStreamReader as InputStreamReader
import java.io.BufferedReader as BufferedReader

def hello(context, arg):
    print 'Hello, world from Celesta Python procedure.'
    print 'user %s' % context.userId
    print 'Argument passed was "%s".' % arg
    
    c = cCursor(context)
    c.descr = 'ab'
    c.calcdat()
    os = c.dat.getOutStream()
    osw = OutputStreamWriter(os, 'utf-8')
    try:
        osw.append('hello, blob field!')
    finally:
        osw.close()
    c.insert()
    insertedId = c.idc
    
    print '>>>> id: %d, %d' % (c.idc, c.bbb)    
    c.first()
    c.get(insertedId)
    c.calcdat()
    ins = BufferedReader(InputStreamReader(c.dat.getInStream(), 'utf-8'))
    print ins.readLine()

    os = c.dat.getOutStream()
    osw = OutputStreamWriter(os, 'utf-8')
    try:
        osw.append('and now, blob field has been updated.')
    finally:
        osw.close()
    c.update()

    c.first()
    c.get(insertedId)
    c.calcdat()
    ins = BufferedReader(InputStreamReader(c.dat.getInStream(), 'utf-8'))
    print ins.readLine()
    
    return
    
    aa = aaCursor(context)
    aa.deleteAll()
    for i in range(1, 12):
        aa.idaa = i 
        aa.idc = i * i
        aa.insert()
    
    b = bCursor(context)
    b.deleteAll()
    b.descr = 'AB'
    print 'PRE B XREC: %s, REC: %s' % (b.getXRec().asCSVLine(), b.asCSVLine())
    b.insert()
    oldid = b.idb
    
    print 'POST B XREC: %s, REC: %s' % (b.getXRec().asCSVLine(), b.asCSVLine())
    b.idb = None
    b.descr = 'CD'
    print 'PRE B XREC: %s, REC: %s' % (b.getXRec().asCSVLine(), b.asCSVLine())
    b.insert()
    print 'POST B XREC: %s, REC: %s' % (b.getXRec().asCSVLine(), b.asCSVLine())
    
    b.get(oldid)
    b.descr = 'EF'
#    b.ida = 1
    print 'PRE UPDATE B XREC: %s, REC: %s' % (b.getXRec().asCSVLine(), b.asCSVLine())
    b.update()
    print 'POST UPDATE B XREC: %s, REC: %s' % (b.getXRec().asCSVLine(), b.asCSVLine())

    while aa.next():
        print "%s : %s" % (aa.idaa , aa.idc)
        
        

        
    print 'Python procedure finished.'
    
def testTrigger(rec):
    print 'Test trigger is run with idc = %s' % rec.idc
