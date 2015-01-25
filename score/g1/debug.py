#coding=UTF-8

#######################################################
# THIS IS 'SANDBOX' DEBUG MODULE.
# DO NOT COMMIT IT TO SVN
#######################################################
import ru.curs.celesta.Celesta as Celesta
import ru.curs.celesta.ConnectionPool as ConnectionPool
import ru.curs.celesta.CallContext as CallContext
import ru.curs.celesta.SessionContext as SessionContext
import sys

#1. При запуске из Eclipse надо обеспечить, чтобы первый вызов, 
# инициализирующий Celesta, был бы именно getDebugInstane(), а не getInstance()

Celesta.getDebugInstance()
conn = ConnectionPool.get()
sesContext = SessionContext('user2', 'testsession')
context = CallContext(conn, sesContext)

#2. Этот блок должен идти до выражений import, импортирующих отлаживаемые гранулы!!
sys.modules['initcontext'] = lambda: context


import hello


try:
    hello.hello(context, 'blah-blah')
except:
    conn.rollback()
    raise
finally:
    #ВСЕГДА ЗАКРЫВАЕМ КУРСОРЫ!!
    context.closeCursors()
    #ВСЕГДА ВОЗВРАЩАЕМ СОЕДИНЕНИЕ В ПУЛ!!
    ConnectionPool.putBack(conn)

