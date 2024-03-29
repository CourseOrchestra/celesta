package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.DBType;
import ru.curs.celesta.ICelesta;
import ru.curs.celesta.SystemCallContext;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.score.BasicTable;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.MaterializedView;
import ru.curs.celesta.score.ParameterizedView;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.TableType;
import ru.curs.celesta.score.View;
import ru.curs.celesta.syscursors.GrainsCursor;
import ru.curs.celesta.syscursors.RolesCursor;
import ru.curs.celesta.syscursors.TablesCursor;
import ru.curs.celesta.syscursors.UserrolesCursor;

import java.sql.Connection;

/**
 * Class performing update procedure of the database.
 */
public final class DbUpdaterImpl extends DbUpdater<CallContext> {

    static final String EXEC_NATIVE_NOT_SUPPORTED_MESSAGE = "\"EXECUTE NATIVE\" expression is not supported";

    private final ICelesta celesta;
    private TablesCursor table;

    public DbUpdaterImpl(ConnectionPool connectionPool, Score score, boolean forceDdInitialize, DBAdaptor dba,
                         ICelesta celesta) {

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

    /**
     * Updates the system grain.
     */
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

        dbAdaptor.createTable(conn, sys.getElement("tables", BasicTable.class));
        dbAdaptor.createTable(conn, sys.getElement("sequences", BasicTable.class));
        dbAdaptor.createTable(conn, sys.getElement("logsetup", BasicTable.class));
    }


    /**
     * Records initialization in security tables. It is run once during creation
     * of the system grain.
     */
    private void initSecurity(CallContext context) {
        RolesCursor roles = new RolesCursor(context);
        roles.clear();
        roles.setId("editor")
                .setDescription("full read-write access")
                .tryInsert();

        roles.clear();
        roles.setId("reader")
                .setDescription("full read-only access")
                .tryInsert();

        UserrolesCursor userRoles = new UserrolesCursor(context);
        userRoles.clear();
        userRoles.setRoleid("editor")
                .setUserid("super")
                .tryInsert();
    }

    @Override
    protected void processGrainMeta(Grain g) {
        // Update directory celesta.tables.
        table.setRange(table.COLUMNS.grainid(), g.getName());
        while (table.nextInSet()) {
            switch (TableType.getByAbbreviation(table.getTabletype())) {
                case TABLE:
                    table.setOrphaned(!g.getElements(BasicTable.class).containsKey(table.getTablename()));
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
        for (BasicTable t : g.getElements(BasicTable.class).values()) {
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
