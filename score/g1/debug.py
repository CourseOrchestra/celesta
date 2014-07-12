#######################################################
# THIS IS 'SANDBOX' DEBUG MODULE.
# DO NOT COMMIT IT TO SVN
#######################################################
import ru.curs.celesta.Celesta as Celesta
import ru.curs.celesta.ConnectionPool as ConnectionPool
import ru.curs.celesta.CallContext as CallContext
import ru.curs.celesta.SessionContext as SessionContext
import hello


Celesta.initialize()
conn = ConnectionPool.get()
sesContext = SessionContext('user2', 'testsession')
context = CallContext(conn, sesContext)
try:
    hello.hello(context, 'blah-blah')
except:
    conn.rollback()
    raise
finally:
    ConnectionPool.putBack(conn)

