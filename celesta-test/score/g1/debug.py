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

props.setProperty('rdbms.connection.url', 'jdbc:h2:mem:celesta')
#props.setProperty('rdbms.connection.username', 'postgres')
#props.setProperty('rdbms.connection.password', '123')

celesta = Celesta.createInstance(props)
sc = SessionContext('super', 'debug')

try:
    cc = celesta.callContext(sc)
    print 'Hello world!'
    hello.hello(cc, 'foo')

finally:
    cc.close()

print '!'


