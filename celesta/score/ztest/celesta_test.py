# coding=UTF-8
from ztest.internal_celesta_unit import InternalCelestaUnit
from g3._g3_orm import cCursor


class CelestaTest(InternalCelestaUnit):
    """Класс-пример для написания тестов"""

    def test_celesta(self):
        self.setReferentialIntegrity(False)
        c = cCursor(self.context)
        count = c.count()
        c.clear()
        c.insert()
        self.assertEqual(c.count(), count + 1)
