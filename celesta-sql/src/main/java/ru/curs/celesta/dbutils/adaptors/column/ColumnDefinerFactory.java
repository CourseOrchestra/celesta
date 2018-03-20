package ru.curs.celesta.dbutils.adaptors.column;

import ru.curs.celesta.DBType;
import ru.curs.celesta.score.*;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public final class ColumnDefinerFactory {

    private static final Map<DBType, Map<Class<? extends Column>, ColumnDefiner>> COLUMN_DEFINERS = new HashMap<>();
    private static final Map<DBType, Function<Class<? extends Column>, ColumnDefiner>> FACTORY_METHODS = new HashMap<>();
    private static final Map<Class<? extends Column>, Supplier<? extends ColumnDefiner>> H2_METHODS = new HashMap<>();
    private static final Map<Class<? extends Column>, Supplier<? extends ColumnDefiner>> POSTGRES_METHODS = new HashMap<>();
    private static final Map<Class<? extends Column>, Supplier<? extends ColumnDefiner>> MS_SQL_METHODS = new HashMap<>();
    private static final Map<Class<? extends Column>, Supplier<? extends ColumnDefiner>> ORA_METHODS = new HashMap<>();

    static {
        FACTORY_METHODS.put(
                DBType.H2,
                (cls) -> H2_METHODS.get(cls).get()
        );
        FACTORY_METHODS.put(
                DBType.POSTGRESQL,
                (cls) -> POSTGRES_METHODS.get(cls).get()
        );
        FACTORY_METHODS.put(
                DBType.MSSQL,
                (cls) -> MS_SQL_METHODS.get(cls).get()
        );
        FACTORY_METHODS.put(
                DBType.ORACLE,
                (cls) -> ORA_METHODS.get(cls).get()
        );

        H2_METHODS.put(IntegerColumn.class, H2IntegerColumnDefiner::new);
        H2_METHODS.put(FloatingColumn.class, H2FloatingColumnDefiner::new);
        H2_METHODS.put(BooleanColumn.class, H2BooleanColumnDefiner::new);
        H2_METHODS.put(StringColumn.class, H2StringColumnDefiner::new);
        H2_METHODS.put(BinaryColumn.class, H2BinaryColumnDefiner::new);
        H2_METHODS.put(DateTimeColumn.class, H2DateTimeColumnDefiner::new);

        POSTGRES_METHODS.put(IntegerColumn.class, PostgresIntegerColumnDefiner::new);
        POSTGRES_METHODS.put(FloatingColumn.class, PostgresFloatingColumnDefiner::new);
        POSTGRES_METHODS.put(BooleanColumn.class, PostgresBooleanColumnDefiner::new);
        POSTGRES_METHODS.put(StringColumn.class, PostgresStringColumnDefiner::new);
        POSTGRES_METHODS.put(BinaryColumn.class, PostgresBinaryColumnDefiner::new);
        POSTGRES_METHODS.put(DateTimeColumn.class, PostgresDateTimeColumnDefiner::new);

        MS_SQL_METHODS.put(IntegerColumn.class, MsSqlIntegerColumnDefiner::new);
        MS_SQL_METHODS.put(FloatingColumn.class, MsSqlFloatingColumnDefiner::new);
        MS_SQL_METHODS.put(BooleanColumn.class, MsSqlBooleanColumnDefiner::new);
        MS_SQL_METHODS.put(StringColumn.class, MsSqlStringColumnDefiner::new);
        MS_SQL_METHODS.put(BinaryColumn.class, MsSqlBinaryColumnDefiner::new);
        MS_SQL_METHODS.put(DateTimeColumn.class, MsSqlDateTimeColumnDefiner::new);

        ORA_METHODS.put(IntegerColumn.class, OraIntegerColumnDefiner::new);
        ORA_METHODS.put(FloatingColumn.class, OraFloatingColumnDefiner::new);
        ORA_METHODS.put(BooleanColumn.class, OraBooleanColumnDefiner::new);
        ORA_METHODS.put(StringColumn.class, OraStringColumnDefiner::new);
        ORA_METHODS.put(BinaryColumn.class, OraBinaryColumnDefiner::new);
        ORA_METHODS.put(DateTimeColumn.class, OraDateTimeColumnDefiner::new);
    }

    private ColumnDefinerFactory() {
        throw new AssertionError();
    }

    public static ColumnDefiner getColumnDefiner(DBType dbType, Class<? extends Column> cls) {
        Map<Class<? extends Column>, ColumnDefiner> definers = COLUMN_DEFINERS.computeIfAbsent(
                dbType,
                (dt) -> new HashMap<>()
        );

        return definers.computeIfAbsent(
                cls,
                (c) -> FACTORY_METHODS.get(dbType).apply(c)
        );
    }

}
