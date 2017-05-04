#coding=utf-8

from ru.curs.celesta import Celesta
from ru.curs.celesta import SessionContext
from ru.curs.celesta import CallContext
from ru.curs.celesta import ConnectionPool
from java.util import Properties

from g1 import hello

props = Properties()

props.setProperty('score.path', 'E:\WorkSpace\Curs\Celesta\score')
props.setProperty('pylib.path', 'C:\jython2.7.0\Lib')

props.setProperty('rdbms.connection.url', 'jdbc:postgresql://localhost:5432/celesta')
props.setProperty('rdbms.connection.username', 'postgres')
props.setProperty('rdbms.connection.password', '123')

Celesta.initialize(props)
Celesta.getDebugInstance()
sc = SessionContext('super', 'debug')
conn = ConnectionPool.get()

try:
    cc = CallContext(conn, sc)
    print 'Hello world!'
    hello.hello(cc, 'foo')

finally:
    ConnectionPool.putBack(conn)

print '!'


