package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.meta.DBColumnInfo;
import ru.curs.celesta.dbutils.meta.DBFKInfo;
import ru.curs.celesta.dbutils.meta.DBIndexInfo;
import ru.curs.celesta.dbutils.meta.DBPKInfo;
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
public final class DbUpdater {

  private final ConnectionPool connectionPool;
  private final Score score;
  private final boolean forceDdInitialize;
  private final DBAdaptor dba;
  private final PermissionManager permissionManager;
  private final LoggingManager loggingManager;
  private GrainsCursor grain;
  private TablesCursor table;

  private static final Comparator<Grain> GRAIN_COMPARATOR = Comparator.comparingInt(Grain::getDependencyOrder);

  private static final Set<Integer> EXPECTED_STATUSES;

  static {
    EXPECTED_STATUSES = new HashSet<>();
    EXPECTED_STATUSES.add(GrainsCursor.READY);
    EXPECTED_STATUSES.add(GrainsCursor.RECOVER);
    EXPECTED_STATUSES.add(GrainsCursor.LOCK);
  }

  public DbUpdater(ConnectionPool connectionPool, Score score, boolean forceDdInitialize, DBAdaptor dba,
                   PermissionManager permissionManager, LoggingManager loggingManager) {
    this.connectionPool = connectionPool;
    this.score = score;
    this.forceDdInitialize = forceDdInitialize;
    this.dba = dba;
    this.permissionManager = permissionManager;
    this.loggingManager = loggingManager;
  }

  /**
   * Буфер для хранения информации о грануле.
   */
  private class GrainInfo {
    private boolean recover;
    private boolean lock;
    private int length;
    private int checksum;
    private VersionString version;
  }

  /**
   * Выполняет обновление структуры БД на основе разобранной объектной модели.
   *
   * @throws CelestaException в случае ошибки обновления.
   */
  public void updateDb() throws CelestaException {
    try (
            CallContext context = new CallContextBuilder()
                    .setConnectionPool(connectionPool)
                    .setSesContext(BasicCursor.SYSTEMSESSION)
                    .setScore(score)
                    .setDbAdaptor(dba)
                    .setPermissionManager(permissionManager)
                    .setLoggingManager(loggingManager)
                    .createCallContext()
    ) {
      Connection conn = context.getConn();

      grain = new GrainsCursor(context);
      table = new TablesCursor(context);

      // Проверяем наличие главной системной таблицы.
      if (!dba.tableExists(conn, "celesta", "grains")) {
        // Если главной таблицы нет, а другие таблицы есть -- ошибка.
        if (dba.userTablesExist() && !forceDdInitialize)
          throw new CelestaException("No celesta.grains table found in non-empty database.");
        // Если база вообще пустая, то создаём системные таблицы.
        updateSysGrain(context);
      }

      // Теперь собираем в память информацию о гранулах на основании того,
      // что
      // хранится в таблице grains.
      Map<String, GrainInfo> dbGrains = new HashMap<>();
      while (grain.nextInSet()) {

        if (!(EXPECTED_STATUSES.contains(grain.getState())))
          throw new CelestaException("Cannot proceed with database upgrade: there are grains "
              + "not in 'ready', 'recover' or 'lock' state.");
        GrainInfo gi = new GrainInfo();
        gi.checksum = (int) Long.parseLong(grain.getChecksum(), 16);
        gi.length = grain.getLength();
        gi.recover = grain.getState() == GrainsCursor.RECOVER;
        gi.lock = grain.getState() == GrainsCursor.LOCK;
        try {
          gi.version = new VersionString(grain.getVersion());
        } catch (ParseException e) {
          throw new CelestaException(
              String.format("Error while scanning celesta.grains table: %s", e.getMessage()));
        }
        dbGrains.put(grain.getId(), gi);
      }

      // Получаем список гранул на основе метамодели и сортируем его по
      // порядку зависимости.
      List<Grain> grains = new ArrayList<>(score.getGrains().values());
      Collections.sort(grains, GRAIN_COMPARATOR);

      // Выполняем итерацию по гранулам.
      boolean success = true;
      for (Grain g : grains) {
        // Запись о грануле есть?
        GrainInfo gi = dbGrains.get(g.getName());
        if (gi == null) {
          insertGrainRec(g);
          success = updateGrain(g, connectionPool) & success;
        } else {
          // Запись есть -- решение об апгрейде принимается на основе
          // версии и контрольной суммы.
          success = decideToUpgrade(g, gi, connectionPool) & success;
        }
      }
      if (!success)
        throw new CelestaException(
            "Not all grains were updated successfully, see celesta.grains table data for details.");
    }
  }

  public void updateSysGrain() throws CelestaException {
    try (
            CallContext context = new CallContextBuilder()
                    .setConnectionPool(connectionPool)
                    .setSesContext(BasicCursor.SYSTEMSESSION)
                    .setScore(score)
                    .setDbAdaptor(dba)
                    .setPermissionManager(permissionManager)
                    .setLoggingManager(loggingManager)
                    .createCallContext()
    ) {
      grain = new GrainsCursor(context);
      table = new TablesCursor(context);

      updateSysGrain(context);
    }
  }

  private void updateSysGrain(CallContext context) throws CelestaException {
    try {
      Connection conn = context.getConn();
      Grain sys = score.getGrain("celesta");
      dba.createSchemaIfNotExists("celesta");
      dba.createTable(conn, sys.getElement("grains", Table.class));
      dba.createTable(conn, sys.getElement("tables", Table.class));
      dba.createTable(conn, sys.getElement("sequences", Table.class));
      dba.createSysObjects(conn);
      // logsetup -- версионированная таблица, поэтому для её
      // создания уже могут понадобиться системные объекты
      dba.createTable(conn, sys.getElement("logsetup", Table.class));
      insertGrainRec(sys);
      updateGrain(sys, connectionPool);
      initSecurity(context);
    } catch (ParseException e) {
      throw new CelestaException("No 'celesta' grain definition found.");
    }
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

  private void insertGrainRec(Grain g) throws CelestaException {
    grain.init();
    grain.setId(g.getName());
    grain.setVersion(g.getVersion().toString());
    grain.setLength(g.getLength());
    grain.setChecksum(String.format("%08X", g.getChecksum()));
    grain.setState(GrainsCursor.RECOVER);
    grain.setLastmodified(new Date());
    grain.setMessage("");
    grain.insert();
  }

  private boolean decideToUpgrade(Grain g, GrainInfo gi, ConnectionPool connectionPool) throws CelestaException {
    if (gi.lock)
      return true;

    if (gi.recover)
      return updateGrain(g, connectionPool);

    // Как соотносятся версии?
    switch (g.getVersion().compareTo(gi.version)) {
      case LOWER:
        // Старая версия -- не апгрейдим, ошибка.
        throw new CelestaException(
            "Grain '%s' version '%s' is lower than database "
                + "grain version '%s'. Will not proceed with auto-upgrade.",
            g.getName(), g.getVersion().toString(), gi.version.toString());
      case INCONSISTENT:
        // Непонятная (несовместимая) версия -- не апгрейдим,
        // ошибка.
        throw new CelestaException(
            "Grain '%s' version '%s' is inconsistent with database "
                + "grain version '%s'. Will not proceed with auto-upgrade.",
            g.getName(), g.getVersion().toString(), gi.version.toString());
      case GREATER:
        // Версия выросла -- апгрейдим.
        return updateGrain(g, connectionPool);
      case EQUALS:
        // Версия не изменилась: апгрейдим лишь в том случае, если
        // изменилась контрольная сумма.
        if (gi.length != g.getLength() || gi.checksum != g.getChecksum())
          return updateGrain(g, connectionPool);
      default:
        return true;
    }
  }

  /**
   * Выполняет обновление на уровне отдельной гранулы.
   *
   * @param g Гранула.
   * @throws CelestaException в случае ошибки обновления.
   */
  private boolean updateGrain(Grain g, ConnectionPool connectionPool) throws CelestaException {
    // выставление в статус updating
    grain.get(g.getName());
    grain.setState(GrainsCursor.UPGRADING);
    grain.update();
    connectionPool.commit(grain.callContext().getConn());

    // теперь собственно обновление гранулы
    try {
      // Схему создаём, если ещё не создана.
      dba.createSchemaIfNotExists(g.getName());

      // Удаляем все представления
      dropAllViews(g);
      // Удаляем все параметризованные представления
      dropAllParameterizedViews(g);

      // Выполняем удаление ненужных индексов, чтобы облегчить задачу
      // обновления столбцов на таблицах.
      dropOrphanedGrainIndices(g);

      // Сбрасываем внешние ключи, более не включённые в метаданные
      List<DBFKInfo> dbFKeys = dropOrphanedGrainFKeys(g);

      Set<String> modifiedTablesMap = new HashSet<>();
      // Обновляем все таблицы.
      for (Table t : g.getElements(Table.class).values())
        if (updateTable(t, dbFKeys))
          modifiedTablesMap.add(t.getName());

      // Обновляем все индексы.
      updateGrainIndices(g);

      // Обновляем внешние ключи
      updateGrainFKeys(g);

      // Создаём представления заново
      createViews(g);

      // Создаём параметризованные представления заново
      createParameterizedViews(g);

      // Обновляем все материализованные представления.
      for (MaterializedView mv : g.getElements(MaterializedView.class).values()) {
        String tableName = mv.getRefTable().getTable().getName();
        updateMaterializedView(mv, modifiedTablesMap.contains(tableName));
      }

      //Для всех таблиц обновляем триггеры материализованных представлений
      for (Table t : g.getElements(Table.class).values()) {
        final Connection conn = grain.callContext().getConn();
        dba.dropTableTriggersForMaterializedViews(conn, t);
        dba.createTableTriggersForMaterializedViews(conn, t);
      }

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
      // По завершении -- обновление номера версии, контрольной суммы
      // и выставление в статус ready
      grain.setState(GrainsCursor.READY);
      grain.setChecksum(String.format("%08X", g.getChecksum()));
      grain.setLength(g.getLength());
      grain.setLastmodified(new Date());
      grain.setMessage("");
      grain.setVersion(g.getVersion().toString());
      grain.update();
      connectionPool.commit(grain.callContext().getConn());
      return true;
    } catch (CelestaException e) {
      String newMsg = "";
      try {
        grain.callContext().getConn().rollback();
      } catch (SQLException e1) {
        newMsg = ", " + e1.getMessage();
      }
      // Если что-то пошло не так
      grain.setState(GrainsCursor.ERROR);
      grain.setMessage(String.format("%s/%d/%08X: %s", g.getVersion().toString(), g.getLength(), g.getChecksum(),
          e.getMessage() + newMsg));
      grain.update();
      connectionPool.commit(grain.callContext().getConn());
      return false;
    }
  }

  private void createViews(Grain g) throws CelestaException {
    Connection conn = grain.callContext().getConn();
    for (View v : g.getElements(View.class).values())
      dba.createView(conn, v);
  }

  private void dropAllViews(Grain g) throws CelestaException {
    Connection conn = grain.callContext().getConn();
    for (String viewName : dba.getViewList(conn, g))
      dba.dropView(conn, g.getName(), viewName);
  }

  private void createParameterizedViews(Grain g) throws CelestaException {
    Connection conn = grain.callContext().getConn();
    for (ParameterizedView pv : g.getElements(ParameterizedView.class).values())
      dba.createParameterizedView(conn, pv);
  }

  private void dropAllParameterizedViews(Grain g) throws CelestaException {
    Connection conn = grain.callContext().getConn();
    for (String viewName : dba.getParameterizedViewList(conn, g))
      dba.dropParameterizedView(conn, g.getName(), viewName);
  }


  private void updateGrainFKeys(Grain g) throws CelestaException {
    Connection conn = grain.callContext().getConn();
    Map<String, DBFKInfo> dbFKeys = new HashMap<>();
    for (DBFKInfo dbi : dba.getFKInfo(conn, g))
      dbFKeys.put(dbi.getName(), dbi);
    for (Table t : g.getElements(Table.class).values())
      if (t.isAutoUpdate())
        for (ForeignKey fk : t.getForeignKeys()) {
          if (dbFKeys.containsKey(fk.getConstraintName())) {
            // FK обнаружен в базе, апдейтим при необходимости.
            DBFKInfo dbi = dbFKeys.get(fk.getConstraintName());
            if (!dbi.reflects(fk)) {
              dba.dropFK(conn, g.getName(), dbi.getTableName(), dbi.getName());
              dba.createFK(conn, fk);
            }
          } else {
            // FK не обнаружен в базе, создаём с нуля
            dba.createFK(conn, fk);
          }
        }
  }

  private List<DBFKInfo> dropOrphanedGrainFKeys(Grain g) throws CelestaException {
    Connection conn = grain.callContext().getConn();
    List<DBFKInfo> dbFKeys = dba.getFKInfo(conn, g);
    Map<String, ForeignKey> fKeys = new HashMap<>();
    for (Table t : g.getElements(Table.class).values())
      for (ForeignKey fk : t.getForeignKeys())
        fKeys.put(fk.getConstraintName(), fk);
    Iterator<DBFKInfo> i = dbFKeys.iterator();
    while (i.hasNext()) {
      DBFKInfo dbFKey = i.next();
      ForeignKey fKey = fKeys.get(dbFKey.getName());
      if (fKey == null || !dbFKey.reflects(fKey)) {
        dba.dropFK(conn, g.getName(), dbFKey.getTableName(), dbFKey.getName());
        i.remove();
      }
    }
    return dbFKeys;
  }

  private void dropOrphanedGrainIndices(Grain g) throws CelestaException {
    /*
     * В целом метод повторяет код updateGrainIndices, но только в части
		 * удаления индексов. Зачистить все индексы, подвергшиеся удалению или
		 * изменению необходимо перед тем, как будет выполняться обновление
		 * структуры таблиц, чтобы увеличить вероятность успешного результата:
		 * висящие на полях индексы могут помешать процессу.
		 */
    final Connection conn = grain.callContext().getConn();
    Map<String, DBIndexInfo> dbIndices = dba.getIndices(conn, g);
    Map<String, Index> myIndices = g.getIndices();
    // Удаление несуществующих в метаданных индексов.
    for (DBIndexInfo dBIndexInfo : dbIndices.values())
      if (!myIndices.containsKey(dBIndexInfo.getIndexName()))
        dba.dropIndex(g, dBIndexInfo);

    // Удаление индексов, которые будут в дальнейшем изменены, перед
    // обновлением таблиц.
    for (Entry<String, Index> e : myIndices.entrySet()) {
      DBIndexInfo dBIndexInfo = dbIndices.get(e.getKey());
      if (dBIndexInfo != null) {
        boolean reflects = dBIndexInfo.reflects(e.getValue());
        if (!reflects)
          dba.dropIndex(g, dBIndexInfo);

        // Удаление индексов на тех полях, которые подвергнутся
        // изменению
        for (Entry<String, Column> ee : e.getValue().getColumns().entrySet()) {
          DBColumnInfo ci = dba.getColumnInfo(conn, ee.getValue());
          if (ci == null || !ci.reflects(ee.getValue())) {
            dba.dropIndex(g, dBIndexInfo);
            break;
          }
        }
      }
    }
  }

  private void updateGrainIndices(Grain g) throws CelestaException {
    final Connection conn = grain.callContext().getConn();
    Map<String, DBIndexInfo> dbIndices = dba.getIndices(conn, g);
    Map<String, Index> myIndices = g.getIndices();

    // Обновление и создание нужных индексов
    for (Entry<String, Index> e : myIndices.entrySet()) {
      DBIndexInfo dBIndexInfo = dbIndices.get(e.getKey());
      if (dBIndexInfo != null) {
        // БД содержит индекс с таким именем, надо проверить
        // поля и пересоздать индекс в случае необходимости.
        boolean reflects = dBIndexInfo.reflects(e.getValue());
        if (!reflects) {
          dba.dropIndex(g, dBIndexInfo);
          dba.createIndex(conn, e.getValue());
        }
      } else {
        // Создаём не существовавший ранее индекс.
        dba.createIndex(conn, e.getValue());
      }
    }
  }

  private boolean updateTable(Table t, List<DBFKInfo> dbFKeys) throws CelestaException {
    // Если таблица скомпилирована с опцией NO AUTOUPDATE, то ничего не
    // делаем с ней
    if (!t.isAutoUpdate())
      return false;

    final Connection conn = grain.callContext().getConn();

    if (!dba.tableExists(conn, t.getGrain().getName(), t.getName())) {
      // Таблицы не существует в базе данных, создаём с нуля.
      dba.createTable(conn, t);
      return true;
    }

    DBPKInfo pkInfo;
    Set<String> dbColumns = dba.getColumns(conn, t);
    boolean modified = updateColumns(t, conn, dbColumns, dbFKeys);

    // Для версионированных таблиц синхронизируем поле recversion
    if (t.isVersioned())
      if (dbColumns.contains(VersionedElement.REC_VERSION)) {
        DBColumnInfo ci = dba.getColumnInfo(conn, t.getRecVersionField());
        if (!ci.reflects(t.getRecVersionField())) {
          dba.updateColumn(conn, t.getRecVersionField(), ci);
          modified = true;
        }
      } else {
        dba.createColumn(conn, t.getRecVersionField());
        modified = true;
      }


    // Ещё раз проверяем первичный ключ и при необходимости (если его нет
    // или он был сброшен) создаём.
    pkInfo = dba.getPKInfo(conn, t);
    if (pkInfo.isEmpty())
      dba.createPK(conn, t);

    if (modified)
      try {
        dba.manageAutoIncrement(conn, t);
      } catch (SQLException e) {
        throw new CelestaException("Updating table %s.%s failed: %s.", t.getGrain().getName(), t.getName(),
            e.getMessage());
      }

    dba.updateVersioningTrigger(conn, t);

    return modified;
  }

  private void updateMaterializedView(MaterializedView mv, boolean refTableIsModified) throws CelestaException {
    final Connection conn = grain.callContext().getConn();

    boolean mViewExists = dba.tableExists(conn, mv.getGrain().getName(), mv.getName());

    if (mViewExists) {

      if (!refTableIsModified) {

        //В теле insert-триггера должна храниться контрольная сумма.
        String insertTriggerName = mv.getTriggerName(TriggerType.POST_INSERT);
        TriggerQuery query = new TriggerQuery()
            .withSchema(mv.getGrain().getName())
            .withTableName(mv.getRefTable().getTable().getName())
            .withName(insertTriggerName);

        Optional<String> insertTriggerBody = dba.getTriggerBody(conn, query);
        boolean checksumIsMatched = insertTriggerBody.map(b -> b.contains(
              String.format(MaterializedView.CHECKSUM_COMMENT_TEMPLATE, mv.getChecksum()))).orElse(false);
        if (checksumIsMatched) {
          return;
        }
      }

      //Удаляем materialized view
      dba.dropTable(conn, mv);
    }

    //1. Таблицы не существует в базе данных, создаём с нуля.
    dba.createTable(conn, mv);
    //2. Проинициализировать данные материального представления
    dba.initDataForMaterializedView(conn, mv);
  }

  private void dropReferencedFKs(TableElement t, Connection conn, List<DBFKInfo> dbFKeys) throws CelestaException {
    Iterator<DBFKInfo> i = dbFKeys.iterator();
    while (i.hasNext()) {
      DBFKInfo dbFKey = i.next();
      if (t.getGrain().getName().equals(dbFKey.getRefGrainName())
          && t.getName().equals(dbFKey.getRefTableName())) {
        dba.dropFK(conn, t.getGrain().getName(), dbFKey.getTableName(), dbFKey.getName());
        i.remove();
      }
    }
  }

  private boolean updateColumns(TableElement t, final Connection conn, Set<String> dbColumns, List<DBFKInfo> dbFKeys)
      throws CelestaException {
    // Таблица существует в базе данных, определяем: надо ли удалить
    // первичный ключ
    DBPKInfo pkInfo = dba.getPKInfo(conn, t);
    boolean result = false;
    boolean keyDropped = pkInfo.isEmpty();
    if (!(pkInfo.reflects(t) || keyDropped)) {
      dropReferencedFKs(t, conn, dbFKeys);
      dba.dropPK(conn, t, pkInfo.getName());
      keyDropped = true;
    }

    for (Entry<String, Column> e : t.getColumns().entrySet()) {
      if (dbColumns.contains(e.getKey())) {
        // Таблица содержит колонку с таким именем, надо проверить
        // все её атрибуты и при необходимости -- попытаться
        // обновить.
        DBColumnInfo ci = dba.getColumnInfo(conn, e.getValue());
        if (!ci.reflects(e.getValue())) {
          // Если колонка, требующая обновления, входит в первичный
          // ключ -- сбрасываем первичный ключ.
          if (t.getPrimaryKey().containsKey(e.getKey()) && !keyDropped) {
            dropReferencedFKs(t, conn, dbFKeys);
            dba.dropPK(conn, t, pkInfo.getName());
            keyDropped = true;
          }
          dba.updateColumn(conn, e.getValue(), ci);
          result = true;
        }
      } else {
        // Таблица не содержит колонку с таким именем, добавляем
        dba.createColumn(conn, e.getValue());
        result = true;
      }
    }
    return result;
  }

}
