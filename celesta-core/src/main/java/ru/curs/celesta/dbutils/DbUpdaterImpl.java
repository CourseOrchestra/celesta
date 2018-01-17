package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.meta.*;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.*;
import ru.curs.celesta.syscursors.GrainsCursor;
import ru.curs.celesta.syscursors.RolesCursor;
import ru.curs.celesta.syscursors.TablesCursor;
import ru.curs.celesta.syscursors.TablesCursor.TableType;
import ru.curs.celesta.syscursors.UserRolesCursor;

/**
 * Класс, выполняющий процедуру обновления базы данных.
 */
public final class DbUpdaterImpl extends DbUpdater<CallContext> {

  private final PermissionManager permissionManager;
  private final LoggingManager loggingManager;
  private TablesCursor table;


  public DbUpdaterImpl(ConnectionPool connectionPool, Score score, boolean forceDdInitialize, DBAdaptor dba,
                       PermissionManager permissionManager, LoggingManager loggingManager) {

    super(connectionPool, score, forceDdInitialize, dba);

    this.permissionManager = permissionManager;
    this.loggingManager = loggingManager;
  }


  @Override
  void initDataAccessors(CallContext context) throws CelestaException {
    schemaCursor = new GrainsCursor(context);
    table = new TablesCursor(context);
  }

  @Override
  CallContext createContext() throws CelestaException {
    return new CallContextBuilder()
            .setConnectionPool(connectionPool)
            .setSesContext(BasicCursor.SYSTEMSESSION)
            .setScore(score)
            .setDbAdaptor(dbAdaptor)
            .setPermissionManager(permissionManager)
            .setLoggingManager(loggingManager)
            .createCallContext();
  }

  public void updateSysGrain() throws CelestaException {
    try (CallContext context = createContext()) {
      schemaCursor = new GrainsCursor(context);
      table = new TablesCursor(context);

      updateSysGrain(context);
    }
  }


  @Override
  void updateSysGrain(CallContext context) throws CelestaException {
    super.updateSysGrain(context);
    initSecurity(context);
  }

  @Override
  void createSysObjects(Connection conn, Grain sys) throws CelestaException, ParseException {
    super.createSysObjects(conn, sys);

    dbAdaptor.createTable(conn, sys.getElement("tables", Table.class));
    dbAdaptor.createTable(conn, sys.getElement("sequences", Table.class));
    dbAdaptor.createTable(conn, sys.getElement("logsetup", Table.class));
  }


  /**
   * Инициализация записей в security-таблицах. Производится один раз при
   * создании системной гранулы.
   *
   * @throws CelestaException
   */
  private void initSecurity(CallContext context) throws CelestaException {
    RolesCursor roles = new RolesCursor(context);
    roles.clear();
    roles.setId("editor");
    roles.setDescription("full read-write access");
    roles.tryInsert();

    roles.clear();
    roles.setId("reader");
    roles.setDescription("full read-only access");
    roles.tryInsert();

    UserRolesCursor userRoles = new UserRolesCursor(context);
    userRoles.clear();
    userRoles.setRoleid("editor");
    userRoles.setUserid("super");
    userRoles.tryInsert();
  }

  @Override
  void processGrainMeta(Grain g) throws CelestaException {
    // Обновляем справочник celesta.tables.
    table.setRange("grainid", g.getName());
    while (table.nextInSet()) {
      switch (table.getTabletype()) {
        case TABLE:
          table.setOrphaned(!g.getElements(Table.class).containsKey(table.getTablename()));
          break;
        case VIEW:
          table.setOrphaned(!g.getElements(View.class).containsKey(table.getTablename()));
        case MATERIALIZED_VIEW:
          table.setOrphaned(!g.getElements(MaterializedView.class).containsKey(table.getTablename()));
        case FUNCTION:
          table.setOrphaned(!g.getElements(MaterializedView.class).containsKey(table.getTablename()));
        default:
          break;
      }
      table.update();
    }
    for (Table t : g.getElements(Table.class).values()) {
      table.setGrainid(g.getName());
      table.setTablename(t.getName());
      table.setTabletype(TableType.TABLE);
      table.setOrphaned(false);
      table.tryInsert();
    }
    for (View v : g.getElements(View.class).values()) {
      table.setGrainid(g.getName());
      table.setTablename(v.getName());
      table.setTabletype(TableType.VIEW);
      table.setOrphaned(false);
      table.tryInsert();
    }
    for (MaterializedView mv : g.getElements(MaterializedView.class).values()) {
      table.setGrainid(g.getName());
      table.setTablename(mv.getName());
      table.setTabletype(TableType.MATERIALIZED_VIEW);
      table.setOrphaned(false);
      table.tryInsert();
    }
    for (ParameterizedView pv : g.getElements(ParameterizedView.class).values()) {
      table.setGrainid(g.getName());
      table.setTablename(pv.getName());
      table.setTabletype(TableType.FUNCTION);
      table.setOrphaned(false);
      table.tryInsert();
    }
  }
}
