# coding=UTF-8

from ru.curs.celesta.syscursors import UserrolesCursor

def execute(context):
    c = UserrolesCursor(context)
    c.setFilter("userid", "'super'")
    c.first()
    return c