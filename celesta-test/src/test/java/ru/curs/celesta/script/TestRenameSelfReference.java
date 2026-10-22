package ru.curs.celesta.script;

import org.junit.jupiter.api.Test;
import ru.curs.celesta.AbstractCelestaTest;
import ru.curs.celesta.CelestaException;
import selfRef.NodeCursor;
import selfRef.NodeSetNullCursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Renaming records of self-referencing tables is not supported, whatever the rule
 * of the foreign key: the RDBMS handle it differently (PostgreSQL cascades the key
 * change onto the renamed record itself and fires the version trigger twice, H2 does
 * not cascade onto the record being updated and leaves a dangling reference, Oracle's
 * trigger emulating the rule cannot update the table it fires on), so Celesta refuses
 * it before any SQL is sent. This class covers the cascading rules; the
 * {@code NO ACTION} case is in {@link TestRename}.
 * <p>
 * The cascading foreign keys have a score of their own, because they cannot be
 * declared in MS SQL Server at all. Since the check is made by Celesta itself,
 * H2 is sufficient to test it.
 */
public class TestRenameSelfReference extends AbstractCelestaTest {

    @Override
    protected String scorePath() {
        return "selfRefScore";
    }

    @Test
    public void renameIsNotSupportedWithCascadingSelfReference() {
        NodeCursor c = new NodeCursor(cc());
        c.deleteAll();
        c.setId(1).setTitle("root").insert();
        c.clear();
        c.setId(2).setParentId(1).setTitle("child").insert();

        //even a record that does not reference itself cannot be renamed in such a table
        CelestaException e = assertThrows(CelestaException.class, () -> c.rename(3));
        assertTrue(e.getMessage().contains("references the table itself"), e.getMessage());
        assertThrows(CelestaException.class, () -> c.tryRename(3));

        assertEquals(2, c.getId().intValue());
        NodeCursor check = new NodeCursor(cc());
        assertTrue(check.tryGet(2));
        assertFalse(check.tryGet(3));
    }

    @Test
    public void renameIsNotSupportedWithSetNullSelfReference() {
        NodeSetNullCursor c = new NodeSetNullCursor(cc());
        c.deleteAll();
        c.setId(1).setTitle("root").insert();
        c.clear();
        c.setId(2).setParentId(1).setTitle("child").insert();

        CelestaException e = assertThrows(CelestaException.class, () -> c.rename(3));
        assertTrue(e.getMessage().contains("references the table itself"), e.getMessage());
        assertThrows(CelestaException.class, () -> c.tryRename(3));

        assertEquals(2, c.getId().intValue());
        NodeSetNullCursor check = new NodeSetNullCursor(cc());
        assertTrue(check.tryGet(2));
        assertFalse(check.tryGet(3));
    }
}
