package ru.curs.celesta.dbutils;

import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.*;
import ru.curs.celesta.syscursors.GrainsCursor;
import ru.curs.celesta.syscursors.RolesCursor;
import ru.curs.celesta.syscursors.TablesCursor;
import ru.curs.celesta.syscursors.UserrolesCursor;

import java.sql.Connection;

/**
 * Класс, выполняющий процедуру обновления базы данных.
 */
public final class DbUpdaterImpl extends DbUpdater<CallContext> {

    private final ICelesta celesta;
    private TablesCursor table;

    static final String EXEC_NATIVE_NOT_SUPPORTED_MESSAGE = "\"EXECUTE NATIVE\" expression is not supported";

    public DbUpdaterImpl(ConnectionPool connectionPool, Score score, boolean forceDdInitialize, DBAdaptor dba,
                         ICelesta celesta, PermissionManager permissionManager, LoggingManager loggingManager) {

        super(connectionPool, score, forceDdInitialize, dba);

        this.celesta = celesta;
    }


    @Override
    protected void initDataAccessors(CallContext context) {
        schemaCursor = new GrainsCursor(context);
        table = new TablesCursor(context);
    }

    @Override
    protected String getSchemasTableName() {
        return GrainsCursor.TABLE_NAME;
    }

    @Override
    protected CallContext createContext() {
        return new SystemCallContext(celesta);
    }

    public void updateSysGrain() {
        try (CallContext context = createContext()) {
            schemaCursor = new GrainsCursor(context);
            table = new TablesCursor(context);

            updateSysGrain(context);
        }
    }


    @Override
    public void updateSysGrain(CallContext context) {
        super.updateSysGrain(context);
        initSecurity(context);
    }

    @Override
    void createSysObjects(Connection conn, Grain sys) throws ParseException {
        super.createSysObjects(conn, sys);

        dbAdaptor.createTable(conn, sys.getElement("tables", Table.class));
        dbAdaptor.createTable(conn, sys.getElement("sequences", Table.class));
        dbAdaptor.createTable(conn, sys.getElement("logsetup", Table.class));
    }


    /**
     * Инициализация записей в security-таблицах. Производится один раз при
     * создании системной гранулы.
     */
    private void initSecurity(CallContext context) {
        RolesCursor roles = new RolesCursor(context);
        roles.clear();
        roles.setId("editor");
        roles.setDescription("full read-write access");
        roles.tryInsert();

        roles.clear();
        roles.setId("reader");
        roles.setDescription("full read-only access");
        roles.tryInsert();

        UserrolesCursor userRoles = new UserrolesCursor(context);
        userRoles.clear();
        userRoles.setRoleid("editor");
        userRoles.setUserid("super");
        userRoles.tryInsert();
    }

    @Override
    protected void processGrainMeta(Grain g) {
        // Обновляем справочник celesta.tables.
        table.setRange("grainid", g.getName());
        while (table.nextInSet()) {
            switch (TableType.getByAbbreviation(table.getTabletype())) {
                case TABLE:
                    table.setOrphaned(!g.getElements(Table.class).containsKey(table.getTablename()));
                    break;
                case VIEW:
                    table.setOrphaned(!g.getElements(View.class).containsKey(table.getTablename()));
                    break;
                case MATERIALIZED_VIEW:
                    table.setOrphaned(!g.getElements(MaterializedView.class).containsKey(table.getTablename()));
                    break;
                case FUNCTION:
                    table.setOrphaned(!g.getElements(ParameterizedView.class).containsKey(table.getTablename()));
                    break;
                default:
                    break;
            }
            table.update();
        }
        for (Table t : g.getElements(Table.class).values()) {
            table.setGrainid(g.getName());
            table.setTablename(t.getName());
            table.setTabletype(TableType.TABLE.getAbbreviation());
            table.setOrphaned(false);
            table.tryInsert();
        }
        for (View v : g.getElements(View.class).values()) {
            table.setGrainid(g.getName());
            table.setTablename(v.getName());
            table.setTabletype(TableType.VIEW.getAbbreviation());
            table.setOrphaned(false);
            table.tryInsert();
        }
        for (MaterializedView mv : g.getElements(MaterializedView.class).values()) {
            table.setGrainid(g.getName());
            table.setTablename(mv.getName());
            table.setTabletype(TableType.MATERIALIZED_VIEW.getAbbreviation());
            table.setOrphaned(false);
            table.tryInsert();
        }
        for (ParameterizedView pv : g.getElements(ParameterizedView.class).values()) {
            table.setGrainid(g.getName());
            table.setTablename(pv.getName());
            table.setTabletype(TableType.FUNCTION.getAbbreviation());
            table.setOrphaned(false);
            table.tryInsert();
        }
    }

    @Override
    protected void beforeGrainUpdating(Grain g) {
        for (DBType dbType : DBType.values()) {
            if (!g.getBeforeSqlList(dbType).isEmpty() || !g.getAfterSqlList(dbType).isEmpty()) {
                throw new CelestaException(EXEC_NATIVE_NOT_SUPPORTED_MESSAGE);
            }
        }
    }

}
