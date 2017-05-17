# coding=UTF-8

import unittest
import os
from ru.curs.celesta import Celesta
from ru.curs.celesta import SessionContext
from ru.curs.celesta import CallContext
from ru.curs.celesta import ConnectionPool
from java.util import Properties


class InternalCelestaUnit(unittest.TestCase):
    """Класс-предок для тестирования работы celesta"""

    @classmethod
    def setUpClass(cls):
        cls.props = cls.init_props(cls)
        Celesta.initialize(cls.props)
        Celesta.getDebugInstance()
        cls.sessionContext = SessionContext('super', 'debug')

    def setUp(self):
        self.__conn = ConnectionPool.get()
        self.context = CallContext(self.__conn, self.__class__.sessionContext)

    def tearDown(self):
        self.context.closeCursors()
        ConnectionPool.putBack(self.__conn)

    @staticmethod
    def init_props(cls):
        props = Properties()
        props.setProperty('score.path', os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
        props.setProperty('pylib.path', '/')  # Данная настройка необходима только в контексте запуска java приложения
        props.putAll(cls.init_db_props())
        return props

    """Переопределить в потомке, если требуются другие настройки"""
    @staticmethod
    def init_db_props():
        props = Properties()
        props.setProperty('h2.in-memory', 'true')
        return props

    def setReferentialIntegrity(self, integrity):
        sql = "SET REFERENTIAL_INTEGRITY " + str(integrity)
        stmt = self.__conn.createStatement()
        try:
            stmt.execute(sql)
        finally:
            stmt.close()

