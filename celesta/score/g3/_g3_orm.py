# coding=UTF-8
# Source grain parameters: version=1.0, len=434, crc32=B1DC9BE9; compiler=11.
"""
THIS MODULE IS BEING CREATED AUTOMATICALLY EVERY TIME CELESTA STARTS.
DO NOT MODIFY IT AS YOUR CHANGES WILL BE LOST.
"""
import ru.curs.celesta.dbutils.Cursor as Cursor
import ru.curs.celesta.dbutils.ViewCursor as ViewCursor
import ru.curs.celesta.dbutils.ReadOnlyTableCursor as ReadOnlyTableCursor
import ru.curs.celesta.dbutils.MaterializedViewCursor as MaterializedViewCursor
from java.lang import Object
from jarray import array
from java.util import Calendar, GregorianCalendar
from java.sql import Timestamp
import datetime

def _to_timestamp(d):
    if isinstance(d, datetime.datetime):
        calendar = GregorianCalendar()
        calendar.set(d.year, d.month - 1, d.day, d.hour, d.minute, d.second)
        ts = Timestamp(calendar.getTimeInMillis())
        ts.setNanos(d.microsecond * 1000)
        return ts
    else:
        return d

class cCursor(Cursor):
    onPreDelete  = []
    onPostDelete = []
    onPreInsert  = []
    onPostInsert = []
    onPreUpdate  = []
    onPostUpdate = []
    def __init__(self, context):
        Cursor.__init__(self, context)
        self.idc = None
        self.descr = None
        self.idb = None
        self.aaa = None
        self.bbb = None
        self.dat = None
        self.longtext = None
        self.test = None
        self.doublefield = None
        self.datefield = None
        self.context = context
    def _grainName(self):
        return 'g3'
    def _tableName(self):
        return 'c'
    def _parseResult(self, rs):
        self.idc = rs.getInt('idc')
        if rs.wasNull():
            self.idc = None
        self.descr = rs.getString('descr')
        if rs.wasNull():
            self.descr = None
        self.idb = rs.getInt('idb')
        if rs.wasNull():
            self.idb = None
        self.aaa = rs.getString('aaa')
        if rs.wasNull():
            self.aaa = None
        self.bbb = rs.getInt('bbb')
        if rs.wasNull():
            self.bbb = None
        self.dat = None
        self.longtext = rs.getString('longtext')
        if rs.wasNull():
            self.longtext = None
        self.test = rs.getInt('test')
        if rs.wasNull():
            self.test = None
        self.doublefield = rs.getDouble('doublefield')
        if rs.wasNull():
            self.doublefield = None
        self.datefield = rs.getTimestamp('datefield')
        if rs.wasNull():
            self.datefield = None
        self.recversion = rs.getInt('recversion')
    def _setFieldValue(self, name, value):
        setattr(self, name, value)
    def _clearBuffer(self, withKeys):
        if withKeys:
            self.idc = None
        self.descr = None
        self.idb = None
        self.aaa = None
        self.bbb = None
        self.dat = None
        self.longtext = None
        self.test = None
        self.doublefield = None
        self.datefield = None
    def _currentKeyValues(self):
        return array([None if self.idc == None else int(self.idc)], Object)
    def _currentValues(self):
        return array([None if self.idc == None else int(self.idc), None if self.descr == None else unicode(self.descr), None if self.idb == None else int(self.idb), None if self.aaa == None else unicode(self.aaa), None if self.bbb == None else int(self.bbb), self.dat, None if self.longtext == None else unicode(self.longtext), None if self.test == None else int(self.test), None if self.doublefield == None else float(self.doublefield), _to_timestamp(self.datefield)], Object)
    def calcdat(self):
        self.dat = self.calcBlob('dat')
        self.getXRec().dat = self.dat.clone()
    def _setAutoIncrement(self, val):
        self.idc = val
    def _preDelete(self):
        for f in cCursor.onPreDelete:
            f(self)
    def _postDelete(self):
        for f in cCursor.onPostDelete:
            f(self)
    def _preInsert(self):
        for f in cCursor.onPreInsert:
            f(self)
    def _postInsert(self):
        for f in cCursor.onPostInsert:
            f(self)
    def _preUpdate(self):
        for f in cCursor.onPreUpdate:
            f(self)
    def _postUpdate(self):
        for f in cCursor.onPostUpdate:
            f(self)
    def _getBufferCopy(self, context):
        result = cCursor(context)
        result.copyFieldsFrom(self)
        return result
    def copyFieldsFrom(self, c):
        self.idc = c.idc
        self.descr = c.descr
        self.idb = c.idb
        self.aaa = c.aaa
        self.bbb = c.bbb
        self.dat = c.dat
        self.longtext = c.longtext
        self.test = c.test
        self.doublefield = c.doublefield
        self.datefield = c.datefield
        self.recversion = c.recversion
    def iterate(self):
        if self.tryFindSet():
            while True:
                yield self
                if not self.nextInSet():
                    break

