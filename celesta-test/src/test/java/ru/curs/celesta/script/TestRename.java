package ru.curs.celesta.script;

import org.junit.jupiter.api.TestTemplate;
import rename.ChildCursor;
import rename.ChildNoActionCursor;
import rename.ChildSetNullCursor;
import rename.DecimalKeyCursor;
import rename.LoggedCursor;
import rename.NoVersionCursor;
import rename.ParentCursor;
import rename.SimpleCursor;
import rename.TreeCursor;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.DBType;
import ru.curs.celesta.syscursors.LogCursor;
import ru.curs.celesta.syscursors.LogsetupCursor;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

public class TestRename implements ScriptTest {

    private static void clearAll(CallContext cc) {
        new ChildCursor(cc).deleteAll();
        new ChildSetNullCursor(cc).deleteAll();
        new ChildNoActionCursor(cc).deleteAll();
        new ParentCursor(cc).deleteAll();
        new SimpleCursor(cc).deleteAll();
        new NoVersionCursor(cc).deleteAll();
        new DecimalKeyCursor(cc).deleteAll();
        new LoggedCursor(cc).deleteAll();
        new TreeCursor(cc).deleteAll();
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
    public void renameLoadsXRecFromDatabaseWhenItIsEmpty(CallContext cc) {
        clearAll(cc);
        SimpleCursor c = new SimpleCursor(cc);
        c.setId(1).setTitle("one").insert();

        SimpleCursor other = new SimpleCursor(cc);
        other.setId(1);
        //getXRec() on a cursor that has not read anything yields an empty placeholder
        SimpleCursor xRec = (SimpleCursor) other.getXRec();
        assertNull(xRec.getId());
        assertNull(xRec.getTitle());

        other.rename(2);

        assertEquals(2, xRec.getId().intValue());
        assertEquals("one", xRec.getTitle());
        assertEquals(other.getRecversion(), xRec.getRecversion());
    }

    @TestTemplate
    public void renameOfDecimalKeyComparesKeysAsDatabaseDoes(CallContext cc) {
        clearAll(cc);
        DecimalKeyCursor c = new DecimalKeyCursor(cc);
        c.setId(new BigDecimal("1.00")).setTitle("one").insert();

        //the same key given with a different scale is the same key: no-op, not a collision
        assertTrue(c.tryRename(new BigDecimal("1.0")));
        assertEquals(1, new DecimalKeyCursor(cc).count());
        assertTrue(new DecimalKeyCursor(cc).tryGet(new BigDecimal("1.00")));

        c.rename(new BigDecimal("2.50"));

        DecimalKeyCursor check = new DecimalKeyCursor(cc);
        assertFalse(check.tryGet(new BigDecimal("1.00")));
        assertTrue(check.tryGet(new BigDecimal("2.5")));
        assertEquals("one", check.getTitle());
    }

    @TestTemplate
    public void renameLogsOnlyTheChangeOfTheKey(CallContext cc) {
        clearAll(cc);
        //enable logging of modifications for the "logged" table
        LogsetupCursor logsetup = new LogsetupCursor(cc);
        if (logsetup.tryGet("rename", "logged")) {
            logsetup.setM(true);
            logsetup.update();
        } else {
            logsetup.setGrainid("rename").setTablename("logged")
                    .setI(false).setM(true).setD(false);
            logsetup.insert();
        }
        //the logging manager reads the settings in a transaction of its own
        cc.commit();

        LoggedCursor c = new LoggedCursor(cc);
        c.setId(1).setTitle("one").insert();
        c.setTitle("unsaved modification");

        c.rename(2);

        LogCursor log = new LogCursor(cc);
        log.setRange(log.COLUMNS.grainid(), "rename");
        log.setRange(log.COLUMNS.tablename(), "logged");
        log.orderBy(log.COLUMNS.entryno().desc());
        log.first();
        assertEquals("M", log.getActionType());
        assertEquals("2", log.getPkvalue1());
        //neither the old nor the new values contain the unsaved modification
        assertEquals("1,one", log.getOldvalues());
        assertEquals("2,one", log.getNewvalues());
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

    /**
     * Referential integrity is switched off for H2 in these tests,
     * so there are no foreign keys whose ON UPDATE rules could be checked.
     */
    private static void assumeForeignKeysAreEnforced(CallContext cc) {
        assumeFalse(DBType.H2.equals(cc.getDbAdaptor().getType()),
                "foreign keys are not enforced in H2 test database");
    }

    @TestTemplate
    public void renameOfCompositeKeyCascadesToReferencingTables(CallContext cc) {
        assumeForeignKeysAreEnforced(cc);
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

        child.get(10);
        assertEquals("b", child.getParentCode());
        assertEquals(2, child.getParentNum().intValue());
        child.get(11);
        assertEquals("b", child.getParentCode());
        assertEquals(2, child.getParentNum().intValue());
    }

    @TestTemplate
    public void renameOfCompositeKeySetsNullInReferencingTables(CallContext cc) {
        assumeForeignKeysAreEnforced(cc);
        clearAll(cc);
        ParentCursor parent = new ParentCursor(cc);
        parent.setCode("a").setNum(1).setTitle("parent").insert();
        parent.clear();
        parent.setCode("c").setNum(3).setTitle("other parent").insert();
        ChildSetNullCursor child = new ChildSetNullCursor(cc);
        child.setId(10).setParentCode("a").setParentNum(1).insert();
        child.clear();
        child.setId(11).setParentCode("c").setParentNum(3).insert();

        parent.get("a", 1);
        parent.rename("b", 2);

        assertTrue(new ParentCursor(cc).tryGet("b", 2));

        child.get(10);
        assertNull(child.getParentCode());
        assertNull(child.getParentNum());
        //the record referencing another parent is not affected
        child.get(11);
        assertEquals("c", child.getParentCode());
        assertEquals(3, child.getParentNum().intValue());
    }

    @TestTemplate
    public void renameOfReferencedKeyIsRejectedByNoActionRule(CallContext cc) {
        assumeForeignKeysAreEnforced(cc);
        clearAll(cc);
        ParentCursor parent = new ParentCursor(cc);
        parent.setCode("a").setNum(1).setTitle("parent").insert();
        ChildNoActionCursor child = new ChildNoActionCursor(cc);
        child.setId(10).setParentCode("a").setParentNum(1).insert();
        //the data is committed: the failed statement aborts the transaction on some backends
        cc.commit();

        assertThrows(CelestaException.class, () -> parent.rename("b", 2));
        cc.rollback();

        assertTrue(new ParentCursor(cc).tryGet("a", 1));
        assertFalse(new ParentCursor(cc).tryGet("b", 2));
        child.get(10);
        assertEquals("a", child.getParentCode());
        assertEquals(1, child.getParentNum().intValue());

        clearAll(cc);
        cc.commit();
    }

    @TestTemplate
    public void renameIsNotSupportedForSelfReferencingTable(CallContext cc) {
        clearAll(cc);
        TreeCursor c = new TreeCursor(cc);
        c.setId(1).setTitle("root").insert();
        c.clear();
        c.setId(2).setParentId(1).setTitle("child").insert();

        //the RDBMS handle the renaming in self-referencing tables differently,
        //so Celesta does not allow it at all, whatever the rule of the foreign key
        //and whichever record is renamed
        CelestaException e = assertThrows(CelestaException.class, () -> c.rename(3));
        assertTrue(e.getMessage().contains("references the table itself"), e.getMessage());
        assertThrows(CelestaException.class, () -> c.tryRename(3));

        assertEquals(2, c.getId().intValue());
        TreeCursor check = new TreeCursor(cc);
        check.get(2);
        assertEquals(1, check.getParentId().intValue());
        assertFalse(check.tryGet(3));
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
