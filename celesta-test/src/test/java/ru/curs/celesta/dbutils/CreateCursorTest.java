package ru.curs.celesta.dbutils;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;

import org.junit.jupiter.api.*;

import createcursors.MvtableCursor;
import createcursors.PvtableCursor;
import createcursors.WtableCursor;
import ru.curs.celesta.*;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.MaterializedView;
import ru.curs.celesta.score.ParameterizedView;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.ReadOnlyTable;
import ru.curs.celesta.score.SequenceElement;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.View;

public class CreateCursorTest extends AbstractCelestaTest {

    @Override
    protected String scorePath() {
        return "score";
    }

    private Grain g;

    @BeforeEach
    public void before() throws ParseException {
        g = cc().getScore().getGrain("createCursors");
    }

    @AfterEach
    public void after() {
    }

    @Test
    public void createCursorTest() throws ParseException {
        Table wtable = g.getTable("wtable", Table.class);
        assertNotNull(wtable);

        Cursor wtCursor = Cursor.create(wtable, cc());
        assertNotNull(wtCursor);

        wtCursor.deleteAll();

        wtCursor.setValue("data", "test");
        wtCursor.insert();

        wtCursor = Cursor.create(wtable, cc());

        assertEquals("test", new CursorIterator<>(wtCursor).next().getValue("data"));

        try(WtableCursor cursor = new WtableCursor(cc())) {
            assertEquals("test", cursor.iterator().next().getData());
        }
    }

    @Test
    public void createReadOnlyCursorTest() throws ParseException {
        ReadOnlyTable roTable = g.getTable("roTable", ReadOnlyTable.class);
        assertNotNull(roTable);

        ReadOnlyTableCursor rotCursor = ReadOnlyTableCursor.create(roTable, cc());
        assertNotNull(rotCursor);

        assertEquals(0, rotCursor.count());

        assertFalse(new CursorIterator<>(rotCursor).hasNext());
    }

    @Test
    public void createViewCursorTest() throws ParseException {
        try(WtableCursor cursor = new WtableCursor(cc())) {
            cursor.deleteAll();
            cursor.setData("test");
            cursor.insert();
        }

        View wtableDataView = g.getView("wtableDataView");
        assertNotNull(wtableDataView);

        ViewCursor vCursor = ViewCursor.create(wtableDataView, cc());
        assertNotNull(vCursor);

        assertEquals("test", new CursorIterator<>(vCursor).next().getValue("data"));
    }

    @Test
    public void createMaterializedViewCursorTest() throws ParseException {
        try(MvtableCursor cursor = new MvtableCursor(cc())) {
            cursor.deleteAll();
            cursor.clear();
            cursor.setData("test1");
            cursor.insert();

            cursor.clear();
            cursor.setData("test2");
            cursor.insert();

            cursor.clear();
            cursor.setData("test2");
            cursor.insert();
        }

        MaterializedView mvtableDataCountView = g.getMaterializedView("mvtableDataCountView");
        assertNotNull(mvtableDataCountView);

        MaterializedViewCursor mvCursor = MaterializedViewCursor.create(mvtableDataCountView, cc());
        assertNotNull(mvCursor);

        assertEquals(2, mvCursor.count());

        CursorIterator<MaterializedViewCursor> mvci = new CursorIterator<>(mvCursor);

        mvCursor = mvci.next();
        assertEquals("test1", mvCursor.getValue("data"));
        assertEquals(1, mvCursor.getValue("d"));

        mvCursor = mvci.next();
        assertEquals("test2", mvCursor.getValue("data"));
        assertEquals(2, mvCursor.getValue("d"));
    }

    @Test
    public void createParameterizedViewCursorTest() throws ParseException {
        try(PvtableCursor cursor = new PvtableCursor(cc())) {
            cursor.deleteAll();
            cursor.clear();
            cursor.setData(2);
            cursor.insert();

            cursor.clear();
            cursor.setData(4);
            cursor.insert();

            cursor.clear();
            cursor.setData(6);
            cursor.insert();
        }

        ParameterizedView pvtableDataView = g.getParameterizedView("pvtableDataView");
        assertNotNull(pvtableDataView);

        ParameterizedViewCursor pvCursor = ParameterizedViewCursor.create(
                pvtableDataView, cc(), Collections.singletonMap("d", 5));
        assertNotNull(pvCursor);

        assertEquals(2, pvCursor.count());

        CursorIterator<ParameterizedViewCursor> pvci = new CursorIterator<>(pvCursor);

        assertEquals(2, pvci.next().getValue("data"));
        assertEquals(4, pvci.next().getValue("data"));
    }

    @Test
    public void createSequenceTest() throws ParseException {
        SequenceElement seqEl = g.getElement("seq", SequenceElement.class);
        assertNotNull(seqEl);

        Sequence seq = Sequence.create(seqEl, cc());
        assertEquals(3, seq.nextValue());
    }

}
