# coding=UTF-8

from ru.curs.celesta.syscursors import UserRolesCursor

def execute(context):
    c = UserRolesCursor(context)
    c.setFilter("userid", "'super'")
    c.first()
    return c