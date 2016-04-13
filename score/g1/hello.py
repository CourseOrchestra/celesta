# coding=UTF-8
print "hello unit called"
import initcontext
print initcontext()
    
from g1._g1_orm import aaCursor 
from g1._g1_orm import adressesCursor
from g2._g2_orm import bCursor
from g3._g3_orm import cCursor
from g1._g1_orm import testviewCursor
import java.io.OutputStreamWriter as OutputStreamWriter
import java.io.InputStreamReader as InputStreamReader
import java.io.BufferedReader as BufferedReader
import ru.curs.celesta.CelestaException as CelestaException
import random



def hello(context, arg):
    print 'Hello, world from Celesta Python procedure.'
    print initcontext()
    
    print 'user %s' % context.userId
    print 'Argument passed was "%s".' % arg
    
    c = cCursor(context)
    print 'c.count() = %d' % c.count()
    c.descr = 'ab'
    c.calcdat()
    os = c.dat.getOutStream()
    osw = OutputStreamWriter(os, 'utf-8')
    try:
        osw.append('hello, blob field!')
    finally:
        osw.close()
    c.doublefield = 12 + 0.14  # random.random()
    print c.doublefield
    c.aaa = 'тексттекст' #ТЕКСТ2--должно вставляться
    c.longtext = 'Привет, очень-очень длинное-сверхдлинное русское текстовое поле'
    c.insert()
    insertedId = c.idc
    c.update()
    
    
    print '>>>> id: %d, %d' % (c.idc, c.bbb)    
    c.first()
    c.get(insertedId)
    c.calcdat()
    ins = BufferedReader(InputStreamReader(c.dat.getInStream(), 'utf-8'))
    print '%s ... %s ... %s' % (ins.readLine(), c.aaa, c.longtext)

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
    print '%s ... %s' % (ins.readLine(), c.longtext) 
    
    c.dat = None
    c.update()
    
    # return
    
    aa = aaCursor(context)
    aa.deleteAll()
    for i in range(1, 20):
        aa.idaa = i 
        aa.idc = i * i
        aa.textvalue = 'abc'
        if not aa.tryInsert():
            aa.update()

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

    aa.idaa = 1
    aa.delete()  

    aa.limit(3, 1)
    for aa in aa.iterate():  # while aa.next():
        print "%s : %s" % (aa.idaa , aa.idc)
    print '---'
    
    aa.limit(5, 5)
    for aa in aa.iterate():  # while aa.next():
        print "%s : %s" % (aa.idaa , aa.idc)

    print '---'
    aa.limit(0, 5)
    for aa in aa.iterate():  # while aa.next():
        print "%s : %s" % (aa.idaa , aa.idc)
    print '---'
    aa.limit(3, 0)
    for aa in aa.iterate():  # while aa.next():
        print "%s : %s" % (aa.idaa , aa.idc)
        
    adresses = adressesCursor(context)
    adresses.postalcode = '11111'
    adresses.building = '11'
    adresses.flat = '2'
    adresses.delete()

    
    adresses.init()
    adresses.country = 'Россия'
    adresses.city = 'Москва'
    adresses.street = 'Ленинский пр-т'
    adresses.insert()
    adresses.clear()
    
    adresses.get('11111', '11', '2')
    print adresses.asCSVLine()
    adresses.country = 'Украина'
    adresses.update()
    print adresses.asCSVLine()
    adresses.get('11111', '11', '2')
    print adresses.asCSVLine()    
    
    adresses2 = adressesCursor(context)
    adresses2.copyFieldsFrom(adresses)
    adresses2.city = 'bbbbb'
    adresses2.update()
    
    # comment the line below to check version check failure
    adresses.get('11111', '11', '2')
    adresses.city = 'bbcc'
    adresses.update()
    
    #adresses.setFilter('country', "!null|'ss'")
    #adresses.setRange('building', 1, 5)
    #adresses.first()
    
    testview = testviewCursor(context)
    testview.orderBy("f1")
    testview.limit(2, 2)
    for testview in testview.iterate():
        print testview.asCSVLine()
        print '%s - %s - %s' % (testview.fieldAlias, testview.checksum, testview.f1)
    testview.limit(0, 0)
    
    print('---')
    
    testview.setFilter('fieldAlias', "%'g1'%")
    for testview in testview.iterate():
        print '%s - %s - %s' % (testview.fieldAlias, testview.checksum, testview.f1)
    print testview.count()

  
    c.reset()
    c.setFilter('doublefield', ">12")
    c.setFilter('datefield', "'20150201'..")
    # c.setRange('doublefield', 12.14)
    for c in c.iterate():
        print c.doublefield
    c.setValue('doublefield', 3.14)
    print c.doublefield
    c.orderBy('aaa', 'bbb')
    c.setRange('doublefield')
    c.navigate('=')
    print c.doublefield
    print c.idc
    c.navigate('<')
    print c.idc
    
    print 'USER \t| PID'
    print '----------------'
    for cc in context.celesta.activeContexts:
        print '%s \t| %s' % (cc.userId, cc.getDBPid())
    
    print 'Python procedure finished.'
    
def testTrigger(rec):
    print 'Test trigger is run with idc = %s' % rec.idc
