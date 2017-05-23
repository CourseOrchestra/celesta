# coding=UTF-8
from ru.curs.celesta import Celesta
from ru.curs.celesta import SessionContext
from ru.curs.celesta import CallContext
from ru.curs.celesta import ConnectionPool
import testparams
import sys
import importlib
import unittest

props = testparams.CELESTA_PROPERTIES

celesta = Celesta.getDebugInstance(props)
sessionContext = SessionContext('super', 'debug')
conn = ConnectionPool.get()

try:
    callContext = CallContext(conn, sessionContext)
    sys.modules['initcontext'] = lambda: callContext

    try:
        for grain in celesta.getScore().getGrains().values():
            # Когда в testparams заданы модули для инициализации, пропускаем не указанные
            if grain.getName() == "celesta" or \
                    (testparams.INITIALIZING_GRAINS and grain.getName() not in testparams.INITIALIZING_GRAINS):
                continue

            importlib.import_module(grain.getName())

    finally:
        sys.modules.pop('initcontext', None)
        callContext.closeCursors()
finally:
    ConnectionPool.putBack(conn)


class CelestaUnit(unittest.TestCase):
    """Класс-предок для тестирования работы celesta"""

    def setUp(self):
        self.__conn = ConnectionPool.get()
        self.context = CallContext(self.__conn, sessionContext)

    def tearDown(self):
        self.context.closeCursors()
        ConnectionPool.putBack(self.__conn)

    def setReferentialIntegrity(self, integrity):
        sql = "SET REFERENTIAL_INTEGRITY " + str(integrity)
        stmt = self.__conn.createStatement()
        try:
            stmt.execute(sql)
        finally:
            stmt.close()