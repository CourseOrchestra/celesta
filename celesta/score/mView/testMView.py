# coding=UTF-8


from celestaunit.internal_celesta_unit import CelestaUnit
from mView._mView_orm import table1Cursor, mView1Cursor

class TestAggregate(CelestaUnit):

    def test_mat_view_insert(self):
        tableCursor = table1Cursor(self.context)
        mViewCursor = mView1Cursor(self.context)

        tableCursor.deleteAll()

        tableCursor.numb = 5
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 2
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        self.assertEqual(1, mViewCursor.count())

        tableCursor.numb = 20
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 11
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        self.assertEqual(2, mViewCursor.count())

        mViewCursor.get("A")
        self.assertEqual(7, mViewCursor.s)

        mViewCursor.get("B")
        self.assertEqual(31, mViewCursor.s)

        mViewCursor.setRange('var', "A")
        self.assertEqual(1, mViewCursor.count())
        mViewCursor.first()
        self.assertEqual(7, mViewCursor.s)

        mViewCursor.setRange('var', "B")
        self.assertEqual(1, mViewCursor.count())
        mViewCursor.first()
        self.assertEqual(31, mViewCursor.s)

    def test_mat_view_update(self):
        tableCursor = table1Cursor(self.context)
        mViewCursor = mView1Cursor(self.context)

        tableCursor.deleteAll()

        tableCursor.numb = 5
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 2
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 20
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 11
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.setRange('numb', 2)
        tableCursor.first()
        tableCursor.numb = 4
        tableCursor.update()
        tableCursor.clear()

        tableCursor.setRange('numb', 11)
        tableCursor.first()
        tableCursor.numb = 15
        tableCursor.update()
        tableCursor.clear()

        self.assertEqual(2, mViewCursor.count())

        mViewCursor.get("A")
        self.assertEqual(9, mViewCursor.s)

        mViewCursor.get("B")
        self.assertEqual(35, mViewCursor.s)

    def test_mat_view_delete(self):
        tableCursor = table1Cursor(self.context)
        mViewCursor = mView1Cursor(self.context)

        tableCursor.deleteAll()

        tableCursor.numb = 5
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 2
        tableCursor.var = "A"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 20
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.numb = 11
        tableCursor.var = "B"
        tableCursor.insert()
        tableCursor.clear()

        tableCursor.setRange('numb', 2)
        tableCursor.first()
        tableCursor.delete()
        tableCursor.clear()

        self.assertEqual(2, mViewCursor.count())

        mViewCursor.get("A")
        self.assertEqual(5, mViewCursor.s)

        tableCursor.setRange('numb', 11)
        tableCursor.first()
        tableCursor.delete()
        tableCursor.clear()

        mViewCursor.get("B")
        self.assertEqual(20, mViewCursor.s)

        tableCursor.setRange('var', "A")
        tableCursor.first()
        tableCursor.delete()

        self.assertEqual(1, mViewCursor.count())