#######################################################
# THIS IS 'SANDBOX' DEBUG MODULE.
# DO NOT COMMIT IT TO SVN
#######################################################
import ru.curs.celesta.Celesta as Celesta
import ru.curs.celesta.ConnectionPool as ConnectionPool
import hello


Celesta.initialize()
conn = ConnectionPool.get()
try:
    hello.hello(conn, 'blah-blah')
finally:
    ConnectionPool.putBack(conn)

