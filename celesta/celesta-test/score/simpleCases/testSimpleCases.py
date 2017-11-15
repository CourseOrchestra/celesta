# coding=UTF-8

from java.sql import Timestamp
from java.time import LocalDateTime
from ru.curs.celesta.syscursors import LogCursor

from simpleCases._simpleCases_orm import getDateForViewCursor, viewWithGetDateCursor, zeroInsertCursor, duplicateCursor
from ru.curs.celesta.unit import TestClass, CelestaTestCase


def preInsert(logCursor):
    logCursor.tablename = "getDateForView"

def postInsert(logCursor):
    logCursor.sessionid = "1"

def preUpdate(logCursor):
    logCursor.tablename = "zeroInsert"

def postUpdate(logCursor):
    logCursor.sessionid = "2"

isPreDeleteDone = False
isPostDeleteDone = False

def resetFlags():
    global isPreDeleteDone
    global isPostDeleteDone
    isPreDeleteDone = False
    isPostDeleteDone = False

def preDelete(logCursor):
    global isPreDeleteDone
    isPreDeleteDone = True
    logCursor.tablename = "table2"

def postDelete(logCursor):
    global isPostDeleteDone
    isPostDeleteDone = True
    logCursor.sessionid = "2"


@TestClass
class TestSimpleCases(CelestaTestCase):
    def test_getdate_in_view(self):
        tableCursor = getDateForViewCursor(self.context)
        tableCursor.deleteAll()

        viewCursor = viewWithGetDateCursor(self.context)
        self.assertEquals(0, viewCursor.count())

        tableCursor.date = Timestamp.valueOf(LocalDateTime.now().minusDays(1))
        tableCursor.insert()
        self.assertEquals(0, viewCursor.count())

        tableCursor.clear()
        tableCursor.date = Timestamp.valueOf(LocalDateTime.now().plusDays(1))
        tableCursor.insert()
        self.assertEquals(1, viewCursor.count())


    def test_zero_insert(self):
        c = zeroInsertCursor(self.context)
        c.deleteAll()

        c.insert()
        print c.id
        print c.date


    def test_triggers_on_sys_cursors(self):
        resetFlags()
        c = LogCursor(self.context)

        LogCursor.onPreInsert(preInsert)
        LogCursor.onPostInsert(postInsert)
        LogCursor.onPreUpdate(preUpdate)
        LogCursor.onPostUpdate(postUpdate)
        LogCursor.onPreDelete(preDelete)
        LogCursor.onPostDelete(postDelete)

        c.userid = '1'
        c.sessionid = '0'
        c.grainid = 'simpleCases'
        c.tablename = "zeroInsert"
        c.action_type = "I"
        c.insert()

        self.assertEquals("getDateForView", c.tablename)
        self.assertEquals("1", c.sessionid)

        c.update()

        self.assertEquals("zeroInsert", c.tablename)
        self.assertEquals("2", c.sessionid)

        self.assertFalse(isPreDeleteDone)
        self.assertFalse(isPostDeleteDone)

        c.delete()

        self.assertTrue(isPreDeleteDone)
        self.assertTrue(isPostDeleteDone)
    
    def test_try_insert(self):
        c = duplicateCursor(self.context)
        c.deleteAll()
        c.id = 10
        self.assertTrue(c.tryInsert())
        self.assertFalse(c.tryInsert())
        c.id = 12
        self.assertTrue(c.tryInsert())
        
    def test_cursor_caching(self):
        
        zic1 = self.context.create(zeroInsertCursor)
        zic2 = self.context.create(zeroInsertCursor)
        self.assertEquals(zic1, zic2)
        
        zic1.close()
        self.assertTrue(zic2.isClosed())
        
        zic3 = self.context.create(zeroInsertCursor)
        self.assertNotEquals(zic1, zic3)