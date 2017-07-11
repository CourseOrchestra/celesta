# coding=UTF-8


from celestaunit.internal_celesta_unit import CelestaUnit
from mView._mView_orm import table1Cursor, mView1Cursor

from java.lang import Thread, System, String
from java.time import LocalDateTime
from ru.curs.celesta import SessionContext
from ru.curs.celesta import CallContext
from ru.curs.celesta import ConnectionPool

class TestMaterializedView(CelestaUnit):

    def test_mat_view_insert(self):
        tableCursor = table1Cursor(self.context)
        mViewCursor = mView1Cursor(self.context)

        tableCursor.deleteAll()

        tableCursor.numb = 5
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 2
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()
        
        tableCursor.numb = 0
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = -1
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()


        self.assertEqual(1, mViewCursor.count())

        tableCursor.numb = 20
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 11
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        self.assertEqual(2, mViewCursor.count())

        mViewCursor.get("A")
        self.assertEqual(6, mViewCursor.s)
        self.assertEqual(4, mViewCursor.c)

        mViewCursor.get("B")
        self.assertEqual(31, mViewCursor.s)
        self.assertEqual(2, mViewCursor.c)

        mViewCursor.setRange('var', "A")
        self.assertEqual(1, mViewCursor.count())
        mViewCursor.first()
        self.assertEqual(6, mViewCursor.s)
        self.assertEqual(4, mViewCursor.c)

        mViewCursor.setRange('var', "B")
        self.assertEqual(1, mViewCursor.count())
        mViewCursor.first()
        self.assertEqual(31, mViewCursor.s)
        self.assertEqual(2, mViewCursor.c)

    def test_mat_view_update(self):
        tableCursor = table1Cursor(self.context)
        mViewCursor = mView1Cursor(self.context)

        tableCursor.deleteAll()
        self.assertEqual(0, mViewCursor.count())

        tableCursor.numb = 5
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 2
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        mViewCursor.get("A")
        self.assertEqual(7, mViewCursor.s)

        tableCursor.numb = 20
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 11
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.setRange('numb', 2)
        tableCursor.first()
        tableCursor.numb = 4
        tableCursor.update()
        tableCursor.clear()

        tableCursor.setRange('numb', 11)
        tableCursor.first()
        tableCursor.numb = 15
        tableCursor.update()
        tableCursor.clear()

        self.assertEqual(2, mViewCursor.count())

        mViewCursor.get("A")
        self.assertEqual(9, mViewCursor.s)
        self.assertEqual(2, mViewCursor.c)

        mViewCursor.get("B")
        self.assertEqual(35, mViewCursor.s)
        self.assertEqual(2, mViewCursor.c)

    def test_mat_view_delete(self):
        tableCursor = table1Cursor(self.context)
        mViewCursor = mView1Cursor(self.context)

        tableCursor.deleteAll()

        tableCursor.numb = 6
        tableCursor.var = "A"
        tableCursor.insert()
        old_id = tableCursor.id
        tableCursor.clear()
        

        tableCursor.numb = 2
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()
                
               
        mViewCursor.get("A")
        self.assertEqual(8, mViewCursor.s)
        
        tableCursor.get(old_id)
        tableCursor.delete()
        mViewCursor.get("A")
        self.assertEqual(2, mViewCursor.s)
        
        tableCursor.numb = 5
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        mViewCursor.get("A")
        self.assertEqual(7, mViewCursor.s)        

        tableCursor.numb = 20
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 11
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.setRange('numb', 2)
        tableCursor.first()
        tableCursor.delete()
        tableCursor.clear()

        self.assertEqual(2, mViewCursor.count())

        mViewCursor.get("A")
        self.assertEqual(5, mViewCursor.s)
        self.assertEqual(1, mViewCursor.c)

        tableCursor.setRange('numb', 11)
        tableCursor.first()
        tableCursor.delete()
        tableCursor.clear()

        mViewCursor.get("B")
        self.assertEqual(20, mViewCursor.s)
        self.assertEqual(1, mViewCursor.c)

        tableCursor.setRange('var', "A")
        tableCursor.first()
        tableCursor.delete()

        self.assertEqual(1, mViewCursor.count())


    '''
    def testMultiThread(self):
        tableCursor = table1Cursor(self.context)

        tableCursor.deleteAll()

        self.tearDown()

        thread1 = TableWriterThread()
        thread1.setName('TableWriterThread1')
        thread2 = TableWriterThread()
        thread2.setName('TableWriterThread2')
        thread3 = TableWriterThread()
        thread3.setName('TableWriterThread3')
        thread4 = TableWriterThread()
        thread4.setName('TableWriterThread4')
        thread5 = TableWriterThread()
        thread5.setName('TableWriterThread5')

        thread1.start()
        thread2.start()
        thread3.start()
        thread4.start()
        thread5.start()

        thread1.join()
        thread2.join()
        thread3.join()
        thread4.join()
        thread5.join()

        self.setUp()



class TableWriterThread(Thread):

    def run(self):

        end = LocalDateTime.now().plusMinutes(1)

        tick = 1
        while LocalDateTime.now().isBefore(end):
            sessionContext = SessionContext('super', 'debug')
            conn = ConnectionPool.get()
            context = CallContext(conn, sessionContext)
            tableCursor = table1Cursor(context)

            tableCursor.numb = 6
            tableCursor.var = "A"
            tableCursor.insert()

            context.closeCursors()
            ConnectionPool.putBack(conn)

            System.out.println('insert completed: tick ' + String.valueOf(tick) + ' ===>' + self.getName())

            sessionContext = SessionContext('super', 'debug')
            conn = ConnectionPool.get()
            context = CallContext(conn, sessionContext)
            tableCursor = table1Cursor(context)

            tableCursor.deleteAll()

            context.closeCursors()
            ConnectionPool.putBack(conn)
            System.out.println('delete completed: tick ' + String.valueOf(tick) + ' ===>' + self.getName())
            tick = tick + 1
'''
