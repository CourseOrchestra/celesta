package ru.curs.celesta.dbutils.migrations;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.dbutils.DbUpdater;
import ru.curs.celesta.dbutils.DbUpdaterAccessor;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.meta.DbColumnInfo;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.test.DbUpdaterExtension;
import ru.curs.celesta.test.ScorePath;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(DbUpdaterExtension.class)
public class SequencesMigrationTest {

    private static final String MANUAL_INT_PK_TO_SEQ_BASED_PREFIX
            = "src/test/resources/ru/curs/celesta/dbutils/migrations/manual_int_pk_to_sequence_based/";
    private static final String MANUAL_INT_PK_TO_SEQ_BASED_PREFIX_V1 = MANUAL_INT_PK_TO_SEQ_BASED_PREFIX + "v1";
    private static final String MANUAL_INT_PK_TO_SEQ_BASED_PREFIX_V2 = MANUAL_INT_PK_TO_SEQ_BASED_PREFIX + "v2";

    @TestTemplate
    @DisplayName("Migration like [id int not null] -> [id int default NEXTVAL(id_sequence) not null] is successful")
    void testMigrationPkFromIntWithoutDefaultToSequenceBased(
            @ScorePath(MANUAL_INT_PK_TO_SEQ_BASED_PREFIX_V1) DbUpdater dbUpdater1,
            @ScorePath(MANUAL_INT_PK_TO_SEQ_BASED_PREFIX_V2) DbUpdater dbUpdater2

    ) throws ParseException {
        DBAdaptor dbAdaptor = DbUpdaterAccessor.getDbAdaptor(dbUpdater1);
        ConnectionPool connectionPool = DbUpdaterAccessor.getConnectionPool(dbUpdater1);
        AbstractScore abstractScore = DbUpdaterAccessor.getScore(dbUpdater1);

        Column column = abstractScore.getGrain("manualIntPkToSeqBased")
                .getTable("t")
                .getColumn("id");


        dbUpdater1.updateDb();
        DbColumnInfo oldDbColumnInfo = dbAdaptor.getColumnInfo(connectionPool.get(), column);

        dbUpdater2.updateDb();
        DbColumnInfo newDbColumnInfo = dbAdaptor.getColumnInfo(connectionPool.get(), column);

        assertAll(
                //old
                () -> assertEquals("id", oldDbColumnInfo.getName()),
                () -> assertEquals(IntegerColumn.class, oldDbColumnInfo.getType()),
                () -> assertEquals(false, oldDbColumnInfo.isNullable()),
                () -> assertEquals("", oldDbColumnInfo.getDefaultValue()),
                () -> assertEquals(0, oldDbColumnInfo.getLength()),
                () -> assertEquals(0, oldDbColumnInfo.getScale()),
                () -> assertEquals(false, oldDbColumnInfo.isMax()),
                //new
                () -> assertEquals("id", newDbColumnInfo.getName()),
                () -> assertEquals(IntegerColumn.class, newDbColumnInfo.getType()),
                () -> assertEquals(false, newDbColumnInfo.isNullable()),
                () -> assertEquals("NEXTVAL(idSeq)", newDbColumnInfo.getDefaultValue()),
                () -> assertEquals(0, newDbColumnInfo.getLength()),
                () -> assertEquals(0, newDbColumnInfo.getScale()),
                () -> assertEquals(false, newDbColumnInfo.isMax())
        );
    }
}
