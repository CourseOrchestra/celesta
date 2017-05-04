# coding=UTF-8
# Source grain parameters: version=1.0, len=157, crc32=A4FB0F9C; compiler=9.
"""
THIS MODULE IS BEING CREATED AUTOMATICALLY EVERY TIME CELESTA STARTS.
DO NOT MODIFY IT AS YOUR CHANGES WILL BE LOST.
"""
import ru.curs.celesta.dbutils.Cursor as Cursor
import ru.curs.celesta.dbutils.ViewCursor as ViewCursor
import ru.curs.celesta.dbutils.ReadOnlyTableCursor as ReadOnlyTableCursor
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

class bCursor(Cursor):
    onPreDelete  = []
    onPostDelete = []
    onPreInsert  = []
    onPostInsert = []
    onPreUpdate  = []
    onPostUpdate = []
    def __init__(self, context):
        Cursor.__init__(self, context)
        self.idb = None
        self.descr = None
        self.ida = None
        self.context = context
    def _grainName(self):
        return 'g2'
    def _tableName(self):
        return 'b'
    def _parseResult(self, rs):
        self.idb = rs.getInt('idb')
        if rs.wasNull():
            self.idb = None
        self.descr = rs.getString('descr')
        if rs.wasNull():
            self.descr = None
        self.ida = rs.getInt('ida')
        if rs.wasNull():
            self.ida = None
        self.recversion = rs.getInt('recversion')
    def _setFieldValue(self, name, value):
        setattr(self, name, value)
    def _clearBuffer(self, withKeys):
        if withKeys:
            self.idb = None
        self.descr = None
        self.ida = None
    def _currentKeyValues(self):
        return array([None if self.idb == None else int(self.idb)], Object)
    def _currentValues(self):
        return array([None if self.idb == None else int(self.idb), None if self.descr == None else unicode(self.descr), None if self.ida == None else int(self.ida)], Object)
    def _setAutoIncrement(self, val):
        self.idb = val
    def _preDelete(self):
        for f in bCursor.onPreDelete:
            f(self)
    def _postDelete(self):
        for f in bCursor.onPostDelete:
            f(self)
    def _preInsert(self):
        for f in bCursor.onPreInsert:
            f(self)
    def _postInsert(self):
        for f in bCursor.onPostInsert:
            f(self)
    def _preUpdate(self):
        for f in bCursor.onPreUpdate:
            f(self)
    def _postUpdate(self):
        for f in bCursor.onPostUpdate:
            f(self)
    def _getBufferCopy(self, context):
        result = bCursor(context)
        result.copyFieldsFrom(self)
        return result
    def copyFieldsFrom(self, c):
        self.idb = c.idb
        self.descr = c.descr
        self.ida = c.ida
        self.recversion = c.recversion
    def iterate(self):
        if self.tryFindSet():
            while True:
                yield self
                if not self.nextInSet():
                    break

