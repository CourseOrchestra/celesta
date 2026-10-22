package ru.curs.celesta.script;

import org.junit.jupiter.api.TestTemplate;
import rename.ChildCursor;
import rename.NoVersionCursor;
import rename.ParentCursor;
import rename.SimpleCursor;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.DBType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRename implements ScriptTest {

    private static void clearAll(CallContext cc) {
        new ChildCursor(cc).deleteAll();
        new ParentCursor(cc).deleteAll();
        new SimpleCursor(cc).deleteAll();
        new NoVersionCursor(cc).deleteAll();
    }

    @TestTemplate
    public void renameChangesPrimaryKeyAndKeepsOtherFields(CallContext cc) {
        clearAll(cc);
        SimpleCursor c = new SimpleCursor(cc);
        c.setId(1).setTitle("one").insert();
        int recversion = c.getRecversion();

        c.rename(2);

        assertEquals(2, c.getId().intValue());
        assertEquals("one", c.getTitle());
        assertEquals(recversion + 1, c.getRecversion());

        SimpleCursor check = new SimpleCursor(cc);
        assertFalse(check.tryGet(1));
        assertTrue(check.tryGet(2));
        assertEquals("one", check.getTitle());
        assertEquals(c.getRecversion(), check.getRecversion());
        assertEquals(1, check.count());

        //the renamed cursor stays in sync with the database
        c.setTitle("two");
        c.update();
        check.get(2);
        assertEquals("two", check.getTitle());
    }

    @TestTemplate
    public void renameUpdatesXRec(CallContext cc) {
        clearAll(cc);
        SimpleCursor c = new SimpleCursor(cc);
        c.setId(1).setTitle("one").insert();

        c.setTitle("modified but not saved");
        c.rename(5);

        SimpleCursor xRec = (SimpleCursor) c.getXRec();
        assertEquals(5, xRec.getId().intValue());
        //rename does not save the changes of non-key fields
        assertEquals("one", xRec.getTitle());
        assertEquals("modified but not saved", c.getTitle());
        assertEquals(c.getRecversion(), xRec.getRecversion());
    }

    @TestTemplate
    public void renameWorksWhenOnlyKeyFieldsAreSet(CallContext cc) {
        clearAll(cc);
        SimpleCursor c = new SimpleCursor(cc);
        c.setId(1).setTitle("one").insert();

        SimpleCursor other = new SimpleCursor(cc);
        other.setId(1);
        //recversion is unknown (zero) here, the same as for delete()
        other.rename(2);

        assertEquals(2, other.getId().intValue());
        assertNull(other.getTitle());
        assertTrue(c.tryGet(2));
        assertEquals("one", c.getTitle());
        assertEquals(other.getRecversion(), c.getRecversion());
    }

    @TestTemplate
    public void renameOfCompositeKeyCascadesToReferencingTables(CallContext cc) {
        clearAll(cc);
        ParentCursor parent = new ParentCursor(cc);
        parent.setCode("a").setNum(1).setTitle("parent").insert();
        ChildCursor child = new ChildCursor(cc);
        child.setId(10).setParentCode("a").setParentNum(1).insert();
        child.clear();
        child.setId(11).setParentCode("a").setParentNum(1).insert();

        parent.rename("b", 2);

        assertEquals("b", parent.getCode());
        assertEquals(2, parent.getNum().intValue());
        assertEquals("parent", parent.getTitle());
        assertFalse(new ParentCursor(cc).tryGet("a", 1));
        assertTrue(new ParentCursor(cc).tryGet("b", 2));

        if (DBType.H2.equals(cc.getDbAdaptor().getType())) {
            // Referential integrity is switched off for H2 in these tests,
            // so there is no foreign key to cascade the change.
            return;
        }
        child.get(10);
        assertEquals("b", child.getParentCode());
        assertEquals(2, child.getParentNum().intValue());
        child.get(11);
        assertEquals("b", child.getParentCode());
        assertEquals(2, child.getParentNum().intValue());
    }

    @TestTemplate
    public void renameToTheSameKeyIsNoOp(CallContext cc) {
        clearAll(cc);
        SimpleCursor c = new SimpleCursor(cc);
        c.setId(1).setTitle("one").insert();
        int recversion = c.getRecversion();

        assertTrue(c.tryRename(1));

        assertEquals(1, c.getId().intValue());
        assertEquals(recversion, c.getRecversion());
        SimpleCursor check = new SimpleCursor(cc);
        check.get(1);
        assertEquals(recversion, check.getRecversion());
    }

    @TestTemplate
    public void renameOfNonExistingRecordFails(CallContext cc) {
        clearAll(cc);
        SimpleCursor c = new SimpleCursor(cc);
        c.setId(1);

        assertFalse(c.tryRename(2));
        assertEquals(1, c.getId().intValue());
        assertThrows(CelestaException.class, () -> c.rename(2));
    }

    @TestTemplate
    public void renameToExistingKeyFails(CallContext cc) {
        clearAll(cc);
        SimpleCursor c = new SimpleCursor(cc);
        c.setId(1).setTitle("one").insert();
        c.clear();
        c.setId(2).setTitle("two").insert();

        c.get(1);
        assertFalse(c.tryRename(2));
        assertEquals(1, c.getId().intValue());
        assertThrows(CelestaException.class, () -> c.rename(2));

        SimpleCursor check = new SimpleCursor(cc);
        check.get(1);
        assertEquals("one", check.getTitle());
        check.get(2);
        assertEquals("two", check.getTitle());
    }

    @TestTemplate
    public void renameWithWrongNumberOfKeyValuesFails(CallContext cc) {
        clearAll(cc);
        ParentCursor parent = new ParentCursor(cc);
        parent.setCode("a").setNum(1).insert();

        assertThrows(CelestaException.class, () -> parent.renameByValuesArray("b"));
        assertThrows(CelestaException.class, () -> parent.tryRenameByValuesArray("b", 2, 3));
        assertTrue(new ParentCursor(cc).tryGet("a", 1));
    }

    @TestTemplate
    public void renameOfOutdatedRecordIsRejected(CallContext cc) {
        clearAll(cc);
        SimpleCursor c1 = new SimpleCursor(cc);
        c1.setId(1).setTitle("one").insert();

        SimpleCursor c2 = new SimpleCursor(cc);
        c2.get(1);
        c2.setTitle("changed by other");
        c2.update();

        CelestaException e = assertThrows(CelestaException.class, () -> c1.rename(2));
        assertTrue(e.getMessage().contains("already modified"), e.getMessage());

        SimpleCursor check = new SimpleCursor(cc);
        assertTrue(check.tryGet(1));
        assertFalse(check.tryGet(2));

        //after re-reading the record, rename succeeds
        c1.get(1);
        c1.rename(2);
        assertTrue(check.tryGet(2));
        assertEquals("changed by other", check.getTitle());
    }

    @TestTemplate
    public void renameOfTableWithoutVersionCheck(CallContext cc) {
        clearAll(cc);
        NoVersionCursor c = new NoVersionCursor(cc);
        c.setId(1).setTitle("one").insert();

        c.rename(2);

        assertEquals(2, c.getId().intValue());
        NoVersionCursor check = new NoVersionCursor(cc);
        assertFalse(check.tryGet(1));
        assertTrue(check.tryGet(2));
        assertEquals("one", check.getTitle());

        c.setId(100);
        assertFalse(c.tryRename(3));
    }
}
