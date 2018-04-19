# coding=UTF-8

from java.sql import Timestamp
from java.time import LocalDateTime
from java.util.function import Consumer
from ru.curs.celesta.syscursors import LogCursor

from simpleCases._simpleCases_orm import getDateForViewCursor, viewWithGetDateCursor, zeroInsertCursor, duplicateCursor,\
    customSequence, forTriggersCursor
from ru.curs.celesta.unit import TestClass, CelestaTestCase


class consumer(Consumer):
    def __init__(self,fn):
        self.accept=fn

@consumer
def sysPreInsert(logCursor):
    logCursor.tablename = "getDateForView"

@consumer
def sysPostInsert(logCursor):
    logCursor.sessionid = "1"

@consumer
def sysPreUpdate(logCursor):
    logCursor.tablename = "zeroInsert"

@consumer
def sysPostUpdate(logCursor):
    logCursor.sessionid = "2"

isPreDeleteDone = False
isPostDeleteDone = False

def resetFlags():
    global isPreDeleteDone
    global isPostDeleteDone
    isPreDeleteDone = False
    isPostDeleteDone = False

@consumer
def sysPreDelete(logCursor):
    global isPreDeleteDone
    isPreDeleteDone = True
    logCursor.tablename = "table2"

@consumer
def sysPostDelete(logCursor):
    global isPostDeleteDone
    isPostDeleteDone = True
    logCursor.sessionid = "2"


def preInsert(forTriggersCursor):
    s = customSequence(forTriggersCursor.callContext())
    forTriggersCursor.id = s.nextValue()

def postInsert(forTriggersCursor):
    forTriggersCursor.val = 2

def preUpdate(forTriggersCursor):
    forTriggersCursor.val = 3

def postUpdate(forTriggersCursor):
    s = customSequence(forTriggersCursor.callContext())
    forTriggersCursor.id = s.nextValue()

def preDelete(forTriggersCursor):
    global isPreDeleteDone
    isPreDeleteDone = True

def postDelete(forTriggersCursor):
    global isPostDeleteDone
    isPostDeleteDone = True


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

        LogCursor.onPreInsert(self.context.getCelesta(), sysPreInsert)
        LogCursor.onPostInsert(self.context.getCelesta(), sysPostInsert)
        LogCursor.onPreUpdate(self.context.getCelesta(), sysPreUpdate)
        LogCursor.onPostUpdate(self.context.getCelesta(), sysPostUpdate)
        LogCursor.onPreDelete(self.context.getCelesta(), sysPreDelete)
        LogCursor.onPostDelete(self.context.getCelesta(), sysPostDelete)

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

    def test_triggers_on_gen_cursors(self):
        resetFlags()
        c = forTriggersCursor(self.context)

        forTriggersCursor.onPreInsert(self.context.getCelesta(), preInsert)
        forTriggersCursor.onPostInsert(self.context.getCelesta(), postInsert)
        forTriggersCursor.onPreUpdate(self.context.getCelesta(), preUpdate)
        forTriggersCursor.onPostUpdate(self.context.getCelesta(), postUpdate)
        forTriggersCursor.onPreDelete(self.context.getCelesta(), preDelete)
        forTriggersCursor.onPostDelete(self.context.getCelesta(), postDelete)

        c.insert()

        self.assertEquals(1, c.id)
        self.assertEquals(2, c.val)

        c.update()

        self.assertEquals(2L, c.id)
        self.assertEquals(3, c.val)

        self.assertFalse(isPreDeleteDone)
        self.assertFalse(isPostDeleteDone)

        c.id = 1

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