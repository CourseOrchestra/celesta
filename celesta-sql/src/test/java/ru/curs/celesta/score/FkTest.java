package ru.curs.celesta.score;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import ru.curs.celesta.exception.CelestaParseException;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class FkTest {

    private static final String GRAIN_NAME = "grain";
    private static final String INT_COLUMN_NAME = "intcol";
    private static final String DATE_COLUMN_NAME = "datecol";
    private static final String STRING_COLUMN_NAME = "strcol";
    private static final String TABLE_1_ID_COLUMN_NAME = "id1";
    private static final String TABLE_2_ID_COLUMN_NAME = "id2";
    private static final String TABLE_3_ID_COLUMN_NAME = "id3";
    private static final String TABLE_4_ID_COLUMN_NAME = "id4";
    private static final String TABLE_5_INT_ID_COLUMN_NAME = "id5int";
    private static final String TABLE_5_DATE_ID_COLUMN_NAME = "id5date";

    private Table t1;
    private Table t2;
    private Table t3;
    private Table t4;
    private Table t5;

    @BeforeEach
    void setUp() throws Exception {
        final Grain grain = new Grain(new CelestaSqlTestScore(), GRAIN_NAME);
        GrainPart gp = new GrainPart(grain, true, null);

        // Table 1
        this.t1 = new Table(gp, "t1");
        Column cc = new IntegerColumn(t1, TABLE_1_ID_COLUMN_NAME);
        cc.setNullableAndDefault(false, "IDENTITY");

        t1.addPK(TABLE_1_ID_COLUMN_NAME);
        t1.finalizePK();
        new IntegerColumn(t1, INT_COLUMN_NAME);
        new DateTimeColumn(t1, DATE_COLUMN_NAME);
        StringColumn stringColumn = new StringColumn(t1, STRING_COLUMN_NAME);
        stringColumn.setLength(String.valueOf(2));

        // Table 2
        this.t2 = new Table(gp, "t2");
        cc = new IntegerColumn(t2, TABLE_2_ID_COLUMN_NAME);
        cc.setNullableAndDefault(false, "IDENTITY");
        t2.addPK(TABLE_2_ID_COLUMN_NAME);
        t2.finalizePK();

        new IntegerColumn(t2, INT_COLUMN_NAME);
        new DateTimeColumn(t2, DATE_COLUMN_NAME);
        StringColumn c = new StringColumn(t2, "scol2");
        c.setLength("2");

        c = new StringColumn(t2, "scol5");
        c.setLength("5");

        // Table 3
        this.t3 = new Table(gp, "t3");
        StringColumn t3c = new StringColumn(t3, TABLE_3_ID_COLUMN_NAME);
        t3c.setLength(String.valueOf(5));
        t3c.setNullableAndDefault(false, null);

        t3.addPK(TABLE_3_ID_COLUMN_NAME);
        t3.finalizePK();

        // Table 4
        this.t4 = new Table(gp, "t4");
        StringColumn t4c = new StringColumn(t4, TABLE_4_ID_COLUMN_NAME);
        t4c.setLength(String.valueOf(2));
        t4c.setNullableAndDefault(false, null);

        t4.addPK(TABLE_4_ID_COLUMN_NAME);
        t4.finalizePK();

        // Table 5
        this.t5 = new Table(gp, "t5");
        IntegerColumn t5c = new IntegerColumn(t5, TABLE_5_INT_ID_COLUMN_NAME);
        t5c.setNullableAndDefault(false, null);
        DateTimeColumn t5c2 = new DateTimeColumn(t5, TABLE_5_DATE_ID_COLUMN_NAME);
        t5c2.setNullableAndDefault(false, null);

        t5.addPK(TABLE_5_INT_ID_COLUMN_NAME);
        t5.addPK(TABLE_5_DATE_ID_COLUMN_NAME);
        t5.finalizePK();
    }

    @Test
    @DisplayName("References resolving successfully when their size is zero")
    void testWithoutFk() {
        assertAll(
                () -> assertDoesNotThrow(t1::resolveReferences),
                () -> assertDoesNotThrow(t2::resolveReferences),
                () -> assertTrue(t1.getForeignKeys().isEmpty()),
                () -> assertTrue(t1.getForeignKeys().isEmpty())
        );
    }

    @Test
    @DisplayName("Fails on adding of unknown column")
    void testFailsOnUnknownColumn() {
        ForeignKey fk = new ForeignKey(t1);
        assertThrows(ParseException.class, () -> fk.addColumn("abracadabra"));
    }

    @Test
    @DisplayName("Fails on adding of duplicated column")
    void testFailsOnDuplicatedColumn() throws Exception {
        final ForeignKey fk = new ForeignKey(t1);
        fk.addColumn(INT_COLUMN_NAME);
        assertThrows(ParseException.class, () -> fk.addColumn(INT_COLUMN_NAME));
    }

    @ValueSource(strings = {"", GRAIN_NAME})
    @ParameterizedTest(name = "{index} ==> grainName=''{0}''")
    @DisplayName("Setting of referencedTable adds FK to parent table, but not resolves the referenced table")
    void testSettingOfReferencedTable(String grainName) throws Exception {
        final ForeignKey fk = new ForeignKey(t1);
        fk.addColumn(INT_COLUMN_NAME);
        // Setting of referencedTable adds FK to parent table
        fk.setReferencedTable(grainName, t2.getName());

        final Set<ForeignKey> foreignKeys = t1.getForeignKeys();

        assertAll(
                () -> assertEquals(1, foreignKeys.size()),
                () -> assertEquals(1, foreignKeys.stream().findFirst().get().getColumns().size()),
                () -> assertTrue(foreignKeys.stream().findFirst().get().getColumns().containsKey(INT_COLUMN_NAME)),
                () -> assertSame(
                        t1.getColumn(INT_COLUMN_NAME),
                        foreignKeys.stream().findFirst().get().getColumns().get(INT_COLUMN_NAME)
                ),
                () -> assertNull(fk.getReferencedTable())
        );
    }

    @Test
    @DisplayName("Reference resolving fails without referenced column")
    void testReferenceResolvingFailsWithoutReferencedColumn() throws Exception {
        final ForeignKey fk = new ForeignKey(t1);
        fk.addColumn(INT_COLUMN_NAME);
        // Setting of referencedTable adds FK to parent table
        fk.setReferencedTable(GRAIN_NAME, t2.getName());
        assertThrows(CelestaParseException.class, t1::resolveReferences);
    }

    @Test
    @DisplayName("Reference resolving fails with not existing referenced column")
    void testReferenceResolvingFailsWithNotExistingReferencedColumn() throws Exception {
        createForeignKeyFromT1toT2(t1, INT_COLUMN_NAME, t2, "blahblah");
        assertThrows(CelestaParseException.class, t1::resolveReferences);
    }

    @Test
    @DisplayName("Reference resolving fails with existing referenced not PK column")
    void testReferenceResolvingFailsWithExistingNotPkReferencedColumn() throws Exception {
        createForeignKeyFromT1toT2(t1, INT_COLUMN_NAME, t2, INT_COLUMN_NAME);
        assertThrows(CelestaParseException.class, t1::resolveReferences);
    }

    @Test
    @DisplayName("Reference resolving is success with existing referenced PK column")
    void testReferenceResolvingSuccessWithExistingPkColumn() throws Exception {
        final ForeignKey fk = createForeignKeyFromT1toT2(t1, INT_COLUMN_NAME, t2, TABLE_2_ID_COLUMN_NAME);

        final Set<ForeignKey> foreignKeys = t1.getForeignKeys();

        assertAll(
                () -> assertDoesNotThrow(t1::resolveReferences),
                () -> assertEquals(Collections.singleton(fk), foreignKeys),
                () -> assertEquals(1, fk.getColumns().size()),
                () -> assertTrue(fk.getColumns().containsKey(INT_COLUMN_NAME)),
                () -> assertSame(
                        t1.getColumn(INT_COLUMN_NAME),
                        fk.getColumns().get(INT_COLUMN_NAME)
                ),
                () -> assertSame(t2, fk.getReferencedTable()),
                () -> assertSame(FKRule.NO_ACTION, fk.getDeleteRule()),
                () -> assertSame(FKRule.NO_ACTION, fk.getUpdateRule())
        );
    }

    @Test
    @DisplayName("Creation of duplicated fk fails")
    void testFailsOnCreationOfDuplicatedFk() throws Exception {
        createForeignKeyFromT1toT2(t1, INT_COLUMN_NAME, t2, TABLE_2_ID_COLUMN_NAME);
        assertThrows(
                ParseException.class,
                () -> createForeignKeyFromT1toT2(t1, INT_COLUMN_NAME, t2, TABLE_2_ID_COLUMN_NAME)
        );
    }

    @Test
    @DisplayName("Reference resolving fails on different column types")
    void testReferenceResolvingFailsOnDifferentColumnTypes() throws Exception {
        createForeignKeyFromT1toT2(t1, INT_COLUMN_NAME, t2, DATE_COLUMN_NAME);
        assertThrows(CelestaParseException.class, t1::resolveReferences);
    }

    @Test
    @DisplayName("Reference resolving fails on different column length")
    void testReferenceResolvingFailsOnDifferentColumnLength() throws Exception {
        createForeignKeyFromT1toT2(t1, STRING_COLUMN_NAME, t3, TABLE_3_ID_COLUMN_NAME);
        assertThrows(CelestaParseException.class, t1::resolveReferences);
    }

    @Test
    @DisplayName("Reference resolving is success on same column length")
    void testReferenceResolvingSuccessOnSameColumnLength() throws Exception {
        final ForeignKey fk = createForeignKeyFromT1toT2(t1, STRING_COLUMN_NAME, t4, TABLE_4_ID_COLUMN_NAME);

        final Set<ForeignKey> foreignKeys = t1.getForeignKeys();

        assertAll(
                () -> assertDoesNotThrow(t1::resolveReferences),
                () -> assertEquals(Collections.singleton(fk), foreignKeys),
                () -> assertEquals(1, fk.getColumns().size()),
                () -> assertTrue(fk.getColumns().containsKey(STRING_COLUMN_NAME)),
                () -> assertSame(
                        t1.getColumn(STRING_COLUMN_NAME),
                        fk.getColumns().get(STRING_COLUMN_NAME)
                ),
                () -> assertSame(t4, fk.getReferencedTable())
        );
    }

    @Test
    @DisplayName("Reference resolving fails on not full complex fk")
    void testReferenceResolvingFailsOnNotFullFk() throws Exception {
        createForeignKeyFromT1toT2(t1, INT_COLUMN_NAME, t5, TABLE_5_INT_ID_COLUMN_NAME);
        assertThrows(CelestaParseException.class, t1::resolveReferences);
    }

    @Test
    @DisplayName("Reference resolving is success on full complex fk length")
    void testReferenceResolvingSuccessOnFullFk() throws Exception {
        ForeignKey fk = new ForeignKey(t1);
        fk.addColumn(INT_COLUMN_NAME);
        fk.addReferencedColumn(TABLE_5_INT_ID_COLUMN_NAME);
        fk.addColumn(DATE_COLUMN_NAME);
        fk.addReferencedColumn(TABLE_5_DATE_ID_COLUMN_NAME);
        fk.setReferencedTable(GRAIN_NAME,t5.getName());

        assertDoesNotThrow(t1::resolveReferences);
    }


    @Test
    @DisplayName("Creation of duplicated complex fk fails")
    void testFailsOnCreationOfDuplicatedComplexFk() throws Exception {
        ForeignKey fk1 = new ForeignKey(t1);
        fk1.addColumn(INT_COLUMN_NAME);
        fk1.addReferencedColumn(TABLE_5_INT_ID_COLUMN_NAME);
        fk1.addColumn(DATE_COLUMN_NAME);
        fk1.addReferencedColumn(TABLE_5_DATE_ID_COLUMN_NAME);
        fk1.setReferencedTable(GRAIN_NAME,t5.getName());

        ForeignKey fk2 = new ForeignKey(t1);
        fk2.addColumn(INT_COLUMN_NAME);
        fk2.addReferencedColumn(TABLE_5_INT_ID_COLUMN_NAME);
        fk2.addColumn(DATE_COLUMN_NAME);
        fk2.addReferencedColumn(TABLE_5_DATE_ID_COLUMN_NAME);

        assertThrows(
                ParseException.class,
                () -> fk2.setReferencedTable(GRAIN_NAME, t5.getName())
        );
    }

    @Test
    @DisplayName("Setting fk rule to SET_NULL on not null column invokes an exception")
    void testFailsOnSettingSetNullUpdateRuleForNotNullColumn() throws Exception {
        final ForeignKey fk = createForeignKeyFromT1toT2(t1, TABLE_1_ID_COLUMN_NAME, t2, TABLE_2_ID_COLUMN_NAME);
        assertThrows(ParseException.class, () -> fk.setUpdateRule(FKRule.SET_NULL));
        assertThrows(ParseException.class, () -> fk.setDeleteRule(FKRule.SET_NULL));
    }

    @EnumSource(FKRule.class)
    @DisplayName("Fk rules sets correctly")
    @ParameterizedTest(name = "[{index}]. FkRule = {0}")
    void testFkRules(FKRule fkRule) throws Exception {
        final ForeignKey fk = createForeignKeyFromT1toT2(t1, INT_COLUMN_NAME, t2, TABLE_2_ID_COLUMN_NAME);
        fk.setDeleteRule(fkRule);
        fk.setUpdateRule(fkRule);

        assertSame(fkRule, fk.getDeleteRule());
        assertSame(fkRule, fk.getUpdateRule());
    }

    private ForeignKey createForeignKeyFromT1toT2(Table t1, String t1Column, Table t2, String t2Column)
            throws Exception {
        final ForeignKey fk = new ForeignKey(t1);
        fk.addColumn(t1Column);
        fk.addReferencedColumn(t2Column);
        fk.setReferencedTable(GRAIN_NAME, t2.getName());
        return fk;
    }

}
