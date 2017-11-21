# coding=UTF-8
# Source grain parameters: version=1.0, len=1325, crc32=EC09B567; compiler=13.
"""
THIS MODULE IS BEING CREATED AND UPDATED AUTOMATICALLY.
DO NOT MODIFY IT AS YOUR CHANGES WILL BE LOST.
"""
import ru.curs.celesta.dbutils.Cursor as Cursor
import ru.curs.celesta.dbutils.ViewCursor as ViewCursor
import ru.curs.celesta.dbutils.ReadOnlyTableCursor as ReadOnlyTableCursor
import ru.curs.celesta.dbutils.MaterializedViewCursor as MaterializedViewCursor
import ru.curs.celesta.dbutils.ParameterizedViewCursor as ParameterizedViewCursor
from java.lang import Object
from jarray import array
from java.util import Calendar, GregorianCalendar, HashSet, HashMap
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

class aaCursor(Cursor):
    onPreDelete  = []
    onPostDelete = []
    onPreInsert  = []
    onPostInsert = []
    onPreUpdate  = []
    onPostUpdate = []
    def __init__(self, context, fields = []):
        if fields:
            Cursor.__init__(self, context, HashSet(fields))
        else:
            Cursor.__init__(self, context)
        self.idaa = None
        self.idc = None
        self.textvalue = None
        self.realvalue = None
        self.context = context
    def _grainName(self):
        return 'g1'
    def _tableName(self):
        return 'aa'
    def _parseResult(self, rs):
        if self.inRec('idaa'):
            self.idaa = rs.getInt('idaa')
            if rs.wasNull():
                self.idaa = None
        if self.inRec('idc'):
            self.idc = rs.getInt('idc')
            if rs.wasNull():
                self.idc = None
        if self.inRec('textvalue'):
            self.textvalue = rs.getString('textvalue')
            if rs.wasNull():
                self.textvalue = None
        if self.inRec('realvalue'):
            self.realvalue = rs.getDouble('realvalue')
            if rs.wasNull():
                self.realvalue = None
        self.recversion = rs.getInt('recversion')
    def _setFieldValue(self, name, value):
        setattr(self, name, value)
    def _clearBuffer(self, withKeys):
        if withKeys:
            self.idaa = None
        self.idc = None
        self.textvalue = None
        self.realvalue = None
    def _currentKeyValues(self):
        return array([None if self.idaa == None else int(self.idaa)], Object)
    def _currentValues(self):
        return array([None if self.idaa == None else int(self.idaa), None if self.idc == None else int(self.idc), None if self.textvalue == None else unicode(self.textvalue), None if self.realvalue == None else float(self.realvalue)], Object)
    def _setAutoIncrement(self, val):
        pass
    def _preDelete(self):
        for f in aaCursor.onPreDelete:
            f(self)
    def _postDelete(self):
        for f in aaCursor.onPostDelete:
            f(self)
    def _preInsert(self):
        for f in aaCursor.onPreInsert:
            f(self)
    def _postInsert(self):
        for f in aaCursor.onPostInsert:
            f(self)
    def _preUpdate(self):
        for f in aaCursor.onPreUpdate:
            f(self)
    def _postUpdate(self):
        for f in aaCursor.onPostUpdate:
            f(self)
    def _getBufferCopy(self, context, fields=None):
        result = aaCursor(context, fields)
        result.copyFieldsFrom(self)
        return result
    def copyFieldsFrom(self, c):
        self.idaa = c.idaa
        self.idc = c.idc
        self.textvalue = c.textvalue
        self.realvalue = c.realvalue
        self.recversion = c.recversion
    def iterate(self):
        if self.tryFindSet():
            while True:
                yield self
                if not self.nextInSet():
                    break

class aCursor(Cursor):
    onPreDelete  = []
    onPostDelete = []
    onPreInsert  = []
    onPostInsert = []
    onPreUpdate  = []
    onPostUpdate = []
    def __init__(self, context, fields = []):
        if fields:
            Cursor.__init__(self, context, HashSet(fields))
        else:
            Cursor.__init__(self, context)
        self.ida = None
        self.descr = None
        self.parent = None
        self.fff = None
        self.context = context
    def _grainName(self):
        return 'g1'
    def _tableName(self):
        return 'a'
    def _parseResult(self, rs):
        if self.inRec('ida'):
            self.ida = rs.getInt('ida')
            if rs.wasNull():
                self.ida = None
        if self.inRec('descr'):
            self.descr = rs.getString('descr')
            if rs.wasNull():
                self.descr = None
        if self.inRec('parent'):
            self.parent = rs.getInt('parent')
            if rs.wasNull():
                self.parent = None
        if self.inRec('fff'):
            self.fff = rs.getInt('fff')
            if rs.wasNull():
                self.fff = None
        self.recversion = rs.getInt('recversion')
    def _setFieldValue(self, name, value):
        setattr(self, name, value)
    def _clearBuffer(self, withKeys):
        if withKeys:
            self.ida = None
        self.descr = None
        self.parent = None
        self.fff = None
    def _currentKeyValues(self):
        return array([None if self.ida == None else int(self.ida)], Object)
    def _currentValues(self):
        return array([None if self.ida == None else int(self.ida), None if self.descr == None else unicode(self.descr), None if self.parent == None else int(self.parent), None if self.fff == None else int(self.fff)], Object)
    def _setAutoIncrement(self, val):
        self.ida = val
    def _preDelete(self):
        for f in aCursor.onPreDelete:
            f(self)
    def _postDelete(self):
        for f in aCursor.onPostDelete:
            f(self)
    def _preInsert(self):
        for f in aCursor.onPreInsert:
            f(self)
    def _postInsert(self):
        for f in aCursor.onPostInsert:
            f(self)
    def _preUpdate(self):
        for f in aCursor.onPreUpdate:
            f(self)
    def _postUpdate(self):
        for f in aCursor.onPostUpdate:
            f(self)
    def _getBufferCopy(self, context, fields=None):
        result = aCursor(context, fields)
        result.copyFieldsFrom(self)
        return result
    def copyFieldsFrom(self, c):
        self.ida = c.ida
        self.descr = c.descr
        self.parent = c.parent
        self.fff = c.fff
        self.recversion = c.recversion
    def iterate(self):
        if self.tryFindSet():
            while True:
                yield self
                if not self.nextInSet():
                    break

class adressesCursor(Cursor):
    onPreDelete  = []
    onPostDelete = []
    onPreInsert  = []
    onPostInsert = []
    onPreUpdate  = []
    onPostUpdate = []
    def __init__(self, context, fields = []):
        if fields:
            Cursor.__init__(self, context, HashSet(fields))
        else:
            Cursor.__init__(self, context)
        self.postalcode = None
        self.country = None
        self.city = None
        self.street = None
        self.building = None
        self.flat = None
        self.context = context
    def _grainName(self):
        return 'g1'
    def _tableName(self):
        return 'adresses'
    def _parseResult(self, rs):
        if self.inRec('postalcode'):
            self.postalcode = rs.getString('postalcode')
            if rs.wasNull():
                self.postalcode = None
        if self.inRec('country'):
            self.country = rs.getString('country')
            if rs.wasNull():
                self.country = None
        if self.inRec('city'):
            self.city = rs.getString('city')
            if rs.wasNull():
                self.city = None
        if self.inRec('street'):
            self.street = rs.getString('street')
            if rs.wasNull():
                self.street = None
        if self.inRec('building'):
            self.building = rs.getString('building')
            if rs.wasNull():
                self.building = None
        if self.inRec('flat'):
            self.flat = rs.getString('flat')
            if rs.wasNull():
                self.flat = None
        self.recversion = rs.getInt('recversion')
    def _setFieldValue(self, name, value):
        setattr(self, name, value)
    def _clearBuffer(self, withKeys):
        if withKeys:
            self.postalcode = None
            self.building = None
            self.flat = None
        self.country = None
        self.city = None
        self.street = None
    def _currentKeyValues(self):
        return array([None if self.postalcode == None else unicode(self.postalcode), None if self.building == None else unicode(self.building), None if self.flat == None else unicode(self.flat)], Object)
    def _currentValues(self):
        return array([None if self.postalcode == None else unicode(self.postalcode), None if self.country == None else unicode(self.country), None if self.city == None else unicode(self.city), None if self.street == None else unicode(self.street), None if self.building == None else unicode(self.building), None if self.flat == None else unicode(self.flat)], Object)
    def _setAutoIncrement(self, val):
        pass
    def _preDelete(self):
        for f in adressesCursor.onPreDelete:
            f(self)
    def _postDelete(self):
        for f in adressesCursor.onPostDelete:
            f(self)
    def _preInsert(self):
        for f in adressesCursor.onPreInsert:
            f(self)
    def _postInsert(self):
        for f in adressesCursor.onPostInsert:
            f(self)
    def _preUpdate(self):
        for f in adressesCursor.onPreUpdate:
            f(self)
    def _postUpdate(self):
        for f in adressesCursor.onPostUpdate:
            f(self)
    def _getBufferCopy(self, context, fields=None):
        result = adressesCursor(context, fields)
        result.copyFieldsFrom(self)
        return result
    def copyFieldsFrom(self, c):
        self.postalcode = c.postalcode
        self.country = c.country
        self.city = c.city
        self.street = c.street
        self.building = c.building
        self.flat = c.flat
        self.recversion = c.recversion
    def iterate(self):
        if self.tryFindSet():
            while True:
                yield self
                if not self.nextInSet():
                    break

class testviewCursor(ViewCursor):
    def __init__(self, context, fields = []):
        if fields:
            ViewCursor.__init__(self, context, HashSet(fields))
        else:
            ViewCursor.__init__(self, context)
        self.fieldAlias = None
        self.tablename = None
        self.checksum = None
        self.f1 = None
        self.context = context
    def _grainName(self):
        return 'g1'
    def _tableName(self):
        return 'testview'
    def _parseResult(self, rs):
        if self.inRec('fieldAlias'):
            self.fieldAlias = rs.getString('fieldAlias')
            if rs.wasNull():
                self.fieldAlias = None
        if self.inRec('tablename'):
            self.tablename = rs.getString('tablename')
            if rs.wasNull():
                self.tablename = None
        if self.inRec('checksum'):
            self.checksum = rs.getString('checksum')
            if rs.wasNull():
                self.checksum = None
        if self.inRec('f1'):
            self.f1 = rs.getString('f1')
            if rs.wasNull():
                self.f1 = None
    def _setFieldValue(self, name, value):
        setattr(self, name, value)
    def _clearBuffer(self, withKeys):
        self.fieldAlias = None
        self.tablename = None
        self.checksum = None
        self.f1 = None
    def _currentValues(self):
        return array([None if self.fieldAlias == None else unicode(self.fieldAlias), None if self.tablename == None else unicode(self.tablename), None if self.checksum == None else unicode(self.checksum), None if self.f1 == None else unicode(self.f1)], Object)
    def _getBufferCopy(self, context, fields=None):
        result = testviewCursor(context, fields)
        result.copyFieldsFrom(self)
        return result
    def copyFieldsFrom(self, c):
        self.fieldAlias = c.fieldAlias
        self.tablename = c.tablename
        self.checksum = c.checksum
        self.f1 = c.f1
    def iterate(self):
        if self.tryFindSet():
            while True:
                yield self
                if not self.nextInSet():
                    break

