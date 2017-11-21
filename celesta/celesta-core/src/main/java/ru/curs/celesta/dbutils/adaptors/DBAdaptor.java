/*
   (с) 2013 ООО "КУРС-ИТ"  

   Этот файл — часть КУРС:Celesta.
   
   КУРС:Celesta — свободная программа: вы можете перераспространять ее и/или изменять
   ее на условиях Стандартной общественной лицензии GNU в том виде, в каком
   она была опубликована Фондом свободного программного обеспечения; либо
   версии 3 лицензии, либо (по вашему выбору) любой более поздней версии.

   Эта программа распространяется в надежде, что она будет полезной,
   но БЕЗО ВСЯКИХ ГАРАНТИЙ; даже без неявной гарантии ТОВАРНОГО ВИДА
   или ПРИГОДНОСТИ ДЛЯ ОПРЕДЕЛЕННЫХ ЦЕЛЕЙ. Подробнее см. в Стандартной
   общественной лицензии GNU.

   Вы должны были получить копию Стандартной общественной лицензии GNU
   вместе с этой программой. Если это не так, см. http://www.gnu.org/licenses/.

   
   Copyright 2013, COURSE-IT Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see http://www.gnu.org/licenses/.

 */

package ru.curs.celesta.dbutils.adaptors;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.dbutils.meta.DBColumnInfo;
import ru.curs.celesta.dbutils.meta.DBFKInfo;
import ru.curs.celesta.dbutils.meta.DBIndexInfo;
import ru.curs.celesta.dbutils.meta.DBPKInfo;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.score.*;

import javax.naming.OperationNotSupportedException;

/**
 * Адаптер соединения с БД, выполняющий команды, необходимые системе обновления.
 */
public abstract class DBAdaptor implements QueryBuildingHelper, StaticDataAdaptor {

  /*
   * NB для программистов. Класс большой, во избежание хаоса здесь порядок
   * такой: прежде всего -- public final
   * методы, далее --- внутренняя кухня (default final и default static
   * методы), в самом низу -- все объявления абстрактных методов.
   */
  static final String NOT_IMPLEMENTED_YET = "not implemented yet";

  static final Class<?>[] COLUMN_CLASSES = {IntegerColumn.class, StringColumn.class, BooleanColumn.class,
      FloatingColumn.class, BinaryColumn.class, DateTimeColumn.class};
  static final Map<String, Class<? extends Column>> CELESTA_TYPES_COLUMN_CLASSES = new HashMap<>();
  static final String COLUMN_NAME = "COLUMN_NAME";
  static final String ALTER_TABLE = "alter table ";
  static final Pattern HEXSTR = Pattern.compile("0x(([0-9A-Fa-f][0-9A-Fa-f])+)");

  private static DBAdaptor db;
  protected final ConnectionPool connectionPool;


  static {
    CELESTA_TYPES_COLUMN_CLASSES.put(IntegerColumn.CELESTA_TYPE, IntegerColumn.class);
    CELESTA_TYPES_COLUMN_CLASSES.put(FloatingColumn.CELESTA_TYPE, FloatingColumn.class);
    CELESTA_TYPES_COLUMN_CLASSES.put(BooleanColumn.CELESTA_TYPE, BooleanColumn.class);
    CELESTA_TYPES_COLUMN_CLASSES.put(StringColumn.VARCHAR, StringColumn.class);
    CELESTA_TYPES_COLUMN_CLASSES.put(BinaryColumn.CELESTA_TYPE, BinaryColumn.class);
    CELESTA_TYPES_COLUMN_CLASSES.put(DateTimeColumn.CELESTA_TYPE, DateTimeColumn.class);
  }

  public static synchronized DBAdaptor create(
          AppSettings.DBType dbType,
          ConnectionPool connectionPool,
          boolean h2ReferentialIntegrity
  ) throws CelestaException {
    try {
      switch (dbType) {
        case MSSQL:
          db = new MSSQLAdaptor(connectionPool);
          break;
        case ORACLE:
          db = new OraAdaptor(connectionPool);
          break;
        case POSTGRES:
          db = new PostgresAdaptor(connectionPool);
          break;
        case H2:
          db = new H2Adaptor(connectionPool, h2ReferentialIntegrity);
          break;
        case UNKNOWN:
        default:
          db = null;
      }

      return db;
    } catch (Exception e) {
      /* Выводим здесь stacktrace, потому что подробности исключений в блоках статической инициализации
       * проглатываются java машиной
       */
      e.printStackTrace();
      throw e;
    }
  }

  protected DBAdaptor(ConnectionPool connectionPool) {
    this.connectionPool = connectionPool;
    connectionPool.setDbAdaptor(this);
  }

  /**
   * Проверка на валидность соединения.
   *
   * @param conn    соединение.
   * @param timeout тайм-аут.
   * @return true если соединение валидно, иначе false
   * @throws CelestaException при возникновении ошибки работы с БД.
   */
  public boolean isValidConnection(Connection conn, int timeout) throws CelestaException {
    try {
      return conn.isValid(timeout);
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
  }

  /**
   * Получить шаблон имени таблицы.
   */
  public String tableTemplate() {
    return "\"%s\".\"%s\"";
  }

  /**
   * Удалить таблицу.
   *
   * @param conn Соединение с БД
   * @param t    удаляемая таблица
   * @throws CelestaException в случае ошибки работы с БД
   */
  public final void dropTable(Connection conn, TableElement t) throws CelestaException {
    try {
      String sql = String.format("DROP TABLE " + tableTemplate(), t.getGrain().getName(), t.getName());
      Statement stmt = conn.createStatement();
      try {
        stmt.executeUpdate(sql);
      } finally {
        stmt.close();
      }
      dropAutoIncrement(conn, t);
      conn.commit();
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
  }

  abstract public void dropTrigger(Connection conn, TriggerQuery query) throws SQLException;

  /**
   * Возвращает true в том и только том случае, если база данных содержит
   * пользовательские таблицы (т. е. не является пустой базой данных).
   *
   * @throws CelestaException ошибка БД
   */
  public final boolean userTablesExist() throws CelestaException {
    try (Connection conn = connectionPool.get()) {
      return userTablesExist(conn);
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
  }

  /**
   * Создаёт в базе данных схему с указанным именем, если таковая схема ранее
   * не существовала.
   *
   * @param name имя схемы.
   * @throws CelestaException только в том случае, если возник критический сбой при
   *                          создании схемы. Не выбрасывается в случае, если схема с
   *                          данным именем уже существует в базе данных.
   */
  public final void createSchemaIfNotExists(String name) throws CelestaException {
    try (Connection conn = connectionPool.get()) {
      createSchemaIfNotExists(conn, name);
    } catch (SQLException e) {
      throw new CelestaException("Cannot create schema. " + e.getMessage());
    }
  }

  /**
   * Создаёт в базе данных таблицу "с нуля".
   *
   * @param conn Соединение.
   * @param te   Таблица для создания.
   * @throws CelestaException В случае возникновения критического сбоя при создании
   *                          таблицы, в том числе в случае, если такая таблица существует.
   */
  public final void createTable(Connection conn, TableElement te) throws CelestaException {
    String def = tableDef(te);

    try {
      //System.out.println(def); // for debug purposes
      Statement stmt = conn.createStatement();
      try {
        stmt.executeUpdate(def);
      } finally {
        stmt.close();
      }
      manageAutoIncrement(conn, te);
      connectionPool.commit(conn);
      updateVersioningTrigger(conn, te);
    } catch (SQLException e) {
      throw new CelestaException("creating %s: %s", te.getName(), e.getMessage());
    }
  }

  /**
   * Добавляет к таблице новую колонку.
   *
   * @param conn Соединение с БД.
   * @param c    Колонка для добавления.
   * @throws CelestaException при ошибке добавления колонки.
   */
  public final void createColumn(Connection conn, Column c) throws CelestaException {
    String sql = String.format(ALTER_TABLE + tableTemplate() + " add %s", c.getParentTable().getGrain().getName(),
        c.getParentTable().getName(), columnDef(c));
    try {
      Statement stmt = conn.createStatement();
      try {
        stmt.executeUpdate(sql);
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException("creating %s.%s: %s", c.getParentTable().getName(), c.getName(), e.getMessage());
    }
  }

  public static IntegerColumn findIdentityField(TableElement t) {
    IntegerColumn ic = null;
    for (Column c : t.getColumns().values())
      if (c instanceof IntegerColumn && ((IntegerColumn) c).isIdentity()) {
        ic = (IntegerColumn) c;
        break;
      }
    return ic;
  }

  // CHECKSTYLE:OFF 6 parameters
  public final PreparedStatement getUpdateRecordStatement(Connection conn, Table t, boolean[] equalsMask,
                                                          boolean[] nullsMask, List<ParameterSetter> program, String where) throws CelestaException {
    // CHECKSTYLE:ON
    StringBuilder setClause = new StringBuilder();
    if (t.isVersioned()) {
      setClause.append(String.format("\"%s\" = ?", VersionedElement.REC_VERSION));
      program.add(ParameterSetter.createForRecversion());
    }

    int i = 0;
    for (String c : t.getColumns().keySet()) {
      // Пропускаем ключевые поля и поля, не изменившие своего значения
      if (!(equalsMask[i] || t.getPrimaryKey().containsKey(c))) {
        padComma(setClause);
        if (nullsMask[i]) {
          setClause.append(String.format("\"%s\" = NULL", c));
        } else {
          setClause.append(String.format("\"%s\" = ?", c));
          program.add(ParameterSetter.create(t.getColumnIndex(c)));
        }
      }
      i++;
    }

    String sql = String.format("update " + tableTemplate() + " set %s where %s", t.getGrain().getName(),
        t.getName(), setClause.toString(), where);

    // System.out.println(sql);
    return prepareStatement(conn, sql);
  }

  /**
   * Возвращает набор имён столбцов определённой таблицы.
   *
   * @param conn Соединение с БД.
   * @param t    Таблица, по которой просматривать столбцы.
   * @throws CelestaException в случае сбоя связи с БД.
   */
  public Set<String> getColumns(Connection conn, TableElement t) throws CelestaException {
    Set<String> result = new LinkedHashSet<>();
    try {
      DatabaseMetaData metaData = conn.getMetaData();
      ResultSet rs = metaData.getColumns(null, t.getGrain().getName(), t.getName(), null);
      try {
        while (rs.next()) {
          String rColumnName = rs.getString(COLUMN_NAME);
          result.add(rColumnName);
        }
      } finally {
        rs.close();
      }
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
    return result;
  }

  /**
   * Создаёт в грануле индекс на таблице.
   *
   * @param conn  Соединение с БД.
   * @param index описание индекса.
   * @throws CelestaException Если что-то пошло не так.
   */
  public final void createIndex(Connection conn, Index index) throws CelestaException {
    String[] sql = getCreateIndexSQL(index);
    try {
      Statement stmt = conn.createStatement();
      try {
        for (String s : sql)
          stmt.executeUpdate(s);
      } finally {
        stmt.close();
      }
      connectionPool.commit(conn);
    } catch (SQLException e) {
      throw new CelestaException("Cannot create index '%s': %s", index.getName(), e.getMessage());
    }
  }

  /**
   * Создаёт первичный ключ.
   *
   * @param conn соединение с БД.
   * @param fk   первичный ключ
   * @throws CelestaException в случае неудачи создания ключа
   */
  public final void createFK(Connection conn, ForeignKey fk) throws CelestaException {
    LinkedList<StringBuilder> sqlQueue = new LinkedList<>();

    // Строим запрос на создание FK
    StringBuilder sql = new StringBuilder();
    sql.append(ALTER_TABLE);
    sql.append(String.format(tableTemplate(), fk.getParentTable().getGrain().getName(),
        fk.getParentTable().getName()));
    sql.append(" add constraint \"");
    sql.append(fk.getConstraintName());
    sql.append("\" foreign key (");
    boolean needComma = false;
    for (String name : fk.getColumns().keySet()) {
      if (needComma)
        sql.append(", ");
      sql.append('"');
      sql.append(name);
      sql.append('"');
      needComma = true;
    }
    sql.append(") references ");
    sql.append(String.format(tableTemplate(), fk.getReferencedTable().getGrain().getName(),
        fk.getReferencedTable().getName()));
    sql.append("(");
    needComma = false;
    for (String name : fk.getReferencedTable().getPrimaryKey().keySet()) {
      if (needComma)
        sql.append(", ");
      sql.append('"');
      sql.append(name);
      sql.append('"');
      needComma = true;
    }
    sql.append(")");

    switch (fk.getDeleteRule()) {
      case SET_NULL:
        sql.append(" on delete set null");
        break;
      case CASCADE:
        sql.append(" on delete cascade");
        break;
      case NO_ACTION:
      default:
        break;
    }

    sqlQueue.add(sql);
    processCreateUpdateRule(fk, sqlQueue);

    // Построили, выполняем
    for (StringBuilder sqlStmt : sqlQueue) {
      String sqlstmt = sqlStmt.toString();

      // System.out.println("----------------");
      // System.out.println(sqlStmt);

      try {
        Statement stmt = conn.createStatement();
        try {
          stmt.executeUpdate(sqlstmt);
        } finally {
          stmt.close();
        }
      } catch (SQLException e) {
        if (!sqlstmt.startsWith("drop"))
          throw new CelestaException("Cannot create foreign key '%s': %s", fk.getConstraintName(),
              e.getMessage());
      }
    }
  }

  /**
   * Удаляет внешний ключ из базы данных.
   *
   * @param conn      Соединение с БД
   * @param grainName имя гранулы
   * @param tableName Имя таблицы, на которой определён первичный ключ.
   * @param fkName    Имя внешнего ключа.
   * @throws CelestaException В случае сбоя в базе данных.
   */
  public void dropFK(Connection conn, String grainName, String tableName, String fkName) throws CelestaException {
    LinkedList<String> sqlQueue = new LinkedList<>();
    String sql = String.format("alter table " + tableTemplate() + " drop constraint \"%s\"", grainName, tableName,
        fkName);
    sqlQueue.add(sql);
    processDropUpdateRule(sqlQueue, fkName);
    // Построили, выполняем
    for (String sqlStmt : sqlQueue) {
      // System.out.println(sqlStmt);
      try {
        Statement stmt = conn.createStatement();
        try {
          stmt.executeUpdate(sqlStmt);
        } finally {
          stmt.close();
        }
      } catch (SQLException e) {
        if (!sqlStmt.startsWith("drop trigger"))
          throw new CelestaException("Cannot drop foreign key '%s': %s", fkName, e.getMessage());
      }
    }
  }

  void processDropUpdateRule(LinkedList<String> sqlQueue, String fkName) {

  }

  void processCreateUpdateRule(ForeignKey fk, LinkedList<StringBuilder> queue) {
    StringBuilder sql = queue.peek();
    switch (fk.getUpdateRule()) {
      case SET_NULL:
        sql.append(" on update set null");
        break;
      case CASCADE:
        sql.append(" on update cascade");
        break;
      case NO_ACTION:
      default:
        break;
    }
  }

  /**
   * Удаляет в грануле индекс на таблице.
   *
   * @param g           Гранула
   * @param dBIndexInfo Информация об индексе
   * @throws CelestaException Если что-то пошло не так.
   */
  public final void dropIndex(Grain g, DBIndexInfo dBIndexInfo) throws CelestaException {
    String[] sql = getDropIndexSQL(g, dBIndexInfo);

    try (Connection conn = connectionPool.get()) {
      Statement stmt = conn.createStatement();
      for (String s : sql) {
        stmt.executeUpdate(s);
      }
      stmt.close();
    } catch (SQLException e) {
      throw new CelestaException("Cannot drop index '%s': %s ", dBIndexInfo.getIndexName(), e.getMessage());
    }
  }

  /**
   * Возвращает PreparedStatement, содержащий отфильтрованный набор записей.
   *
   * @param conn     Соединение.
   * @param from     Объект для формирования from части запроса.
   * @param orderBy  Порядок сортировки.
   * @param offset   Количество строк для пропуска
   * @param rowCount Количество строк для возврата (limit-фильтр).
   * @param fields   Запрашиваемые столбцы. Если не пришло, то выбираются все.
   * @throws CelestaException Ошибка БД или некорректный фильтр.
   */
  // CHECKSTYLE:OFF 6 parameters
  public final PreparedStatement getRecordSetStatement(
      Connection conn, FromClause from, String whereClause,
      String orderBy, long offset, long rowCount, Set<String> fields
  ) throws CelestaException {
    // CHECKSTYLE:ON
    String sql;

    if (offset == 0 && rowCount == 0) {
      // Запрос не лимитированный -- одинаков для всех СУБД
      // Соединяем полученные компоненты в стандартный запрос
      // SELECT..FROM..WHERE..ORDER BY
      sql = getSelectFromOrderBy(from, whereClause, orderBy, fields);
    } else {
      sql = getLimitedSQL(from, whereClause, orderBy, offset, rowCount, fields);

      // System.out.println(sql);
    }
    try {
      PreparedStatement result = conn.prepareStatement(sql);
      return result;
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
  }

  final String columnDef(Column c) {
    return getColumnDefiner(c).getFullDefinition(c);
  }

  final String tableDef(TableElement te) {
    StringBuilder sb = new StringBuilder();
    // Определение таблицы с колонками
    sb.append(
        String.format("create table " + tableTemplate() + "(\n", te.getGrain().getName(), te.getName()));
    boolean multiple = false;
    for (Column c : te.getColumns().values()) {
      if (multiple)
        sb.append(",\n");
      sb.append("  " + columnDef(c));
      multiple = true;
    }

    if (te instanceof VersionedElement) {
      VersionedElement ve = (VersionedElement) te;
      // У версионированных таблиц - колонка recversion
      if (ve.isVersioned())
        sb.append(",\n").append("  " + columnDef(ve.getRecVersionField()));
    }

    if (te.hasPrimeKey()) {
      sb.append(",\n");
      // Определение первичного ключа, если он должен присутствовать в таблице
      sb.append(String.format("  constraint \"%s\" primary key (", te.getPkConstraintName()));
      multiple = false;
      for (String s : te.getPrimaryKey().keySet()) {
        if (multiple)
          sb.append(", ");
        sb.append('"');
        sb.append(s);
        sb.append('"');
        multiple = true;
      }
      sb.append(")");
    }

    sb.append("\n)");

    return sb.toString();
  }

  final String getSelectFromOrderBy(
      FromClause from, String whereClause, String orderBy, Set<String> fields
  ) {
    final String fieldList = getTableFieldsListExceptBlobs(from.getGe(), fields);
    String sqlfrom = String.format("select %s from %s" , fieldList,
        from.getExpression());

    String sqlwhere = "".equals(whereClause) ? "" : " where " + whereClause;

    return sqlfrom + sqlwhere + " order by " + orderBy;
  }

  public final PreparedStatement getSetCountStatement(Connection conn, FromClause from, String whereClause)
      throws CelestaException {
    String sql = "select count(*) from " + from.getExpression()
        + ("".equals(whereClause) ? "" : " where " + whereClause);
    PreparedStatement result = prepareStatement(conn, sql);

    return result;
  }

  static PreparedStatement prepareStatement(Connection conn, String sql) throws CelestaException {
    try {
      return conn.prepareStatement(sql);
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
  }

  static String getFieldList(Iterable<String> fields) {
    // NB: этот метод возможно нужно будет сделать виртуальным, чтобы учесть
    // особенности синтаксиса разных баз данных
    StringBuilder sb = new StringBuilder();
    for (String c : fields) {
      if (sb.length() > 0)
        sb.append(", ");
      sb.append('"');
      sb.append(c);
      sb.append('"');
    }
    return sb.toString();
  }

  static String getTableFieldsListExceptBlobs(GrainElement t, Set<String> fields) {
    final List<String> flds;

    Predicate<ColumnMeta> notBinary = c -> !BinaryColumn.CELESTA_TYPE.equals(c.getCelestaType());

    if (fields.isEmpty()) {
      flds = t.getColumns().entrySet().stream()
          .filter(e -> notBinary.test(e.getValue()))
          .map(Map.Entry::getKey)
          .collect(Collectors.toList());
    } else {
      flds = fields.stream()
          .filter(f -> notBinary.test(t.getColumns().get(f)))
          .collect(Collectors.toList());
    }
    // К перечню полей версионированных таблиц обязательно добавляем
    // recversion
    if (t instanceof Table && ((Table) t).isVersioned())
      flds.add(VersionedElement.REC_VERSION);

    return getFieldList(flds);
  }

  static void runUpdateColumnSQL(Connection conn, Column c, String sql) throws CelestaException {
    // System.out.println(sql); //for debug
    try {
      Statement stmt = conn.createStatement();
      try {
        stmt.executeUpdate(sql);
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException("Cannot modify column %s on table %s.%s: %s", c.getName(),
          c.getParentTable().getGrain().getName(), c.getParentTable().getName(), e.getMessage());

    }
  }

  static FKRule getFKRule(String rule) {
    if ("NO ACTION".equalsIgnoreCase(rule) || "RECTRICT".equalsIgnoreCase(rule))
      return FKRule.NO_ACTION;
    if ("SET NULL".equalsIgnoreCase(rule))
      return FKRule.SET_NULL;
    if ("CASCADE".equalsIgnoreCase(rule))
      return FKRule.CASCADE;
    return null;
  }

  static void padComma(StringBuilder insertList) {
    if (insertList.length() > 0)
      insertList.append(", ");
  }

  static Set<String> sqlToStringSet(Connection conn, String sql) throws CelestaException {
    Set<String> result = new HashSet<String>();
    try {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      try {
        while (rs.next()) {
          result.add(rs.getString(1));
        }
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
    return result;
  }

  /**
   * Возвращает навигационный PreparedStatement по фильтрованному набору
   * записей.
   *
   * @param conn                  Соединение.
   * @param orderBy               Порядок сортировки (прямой или обратный).
   * @param navigationWhereClause Условие навигационного набора (от текущей записи).
   */
  public abstract PreparedStatement getNavigationStatement(
      Connection conn, FromClause from, String orderBy,
      String navigationWhereClause, Set<String> fields, long offset
  ) throws CelestaException;

  abstract String getLimitedSQL(
      FromClause from, String whereClause, String orderBy, long offset, long rowCount, Set<String> fields
  );

  abstract ColumnDefiner getColumnDefiner(Column c);

  public abstract boolean tableExists(Connection conn, String schema, String name) throws CelestaException;

  public abstract boolean triggerExists(Connection conn, TriggerQuery query) throws SQLException;

  abstract String getSelectTriggerBodySql(TriggerQuery query);

  abstract boolean userTablesExist(Connection conn) throws SQLException;

  abstract void createSchemaIfNotExists(Connection conn, String name) throws SQLException;

  public abstract void manageAutoIncrement(Connection conn, TableElement te) throws SQLException;

  abstract void dropAutoIncrement(Connection conn, TableElement t) throws SQLException;

  public abstract PreparedStatement getOneRecordStatement(Connection conn, TableElement t,
                                                          String where, Set<String> fields) throws CelestaException;

  public abstract PreparedStatement getOneFieldStatement(Connection conn, Column c, String where) throws CelestaException;

  public abstract PreparedStatement deleteRecordSetStatement(Connection conn, TableElement t, String where) throws CelestaException;

  public abstract PreparedStatement getInsertRecordStatement(Connection conn, Table t, boolean[] nullsMask,
                                                             List<ParameterSetter> program) throws CelestaException;

  public abstract int getCurrentIdent(Connection conn, Table t) throws CelestaException;

  public abstract PreparedStatement getDeleteRecordStatement(Connection conn, TableElement t, String where) throws CelestaException;

  public abstract String getInFilterClause(Table table, Table otherTable, List<String> fields,
                                           List<String> otherFields, String whereForOtherTable);

  abstract String[] getCreateIndexSQL(Index index);

  abstract String[] getDropIndexSQL(Grain g, DBIndexInfo dBIndexInfo);


  /**
   * Возвращает информацию о столбце.
   *
   * @param conn Соединение с БД.
   * @param c    Столбец.
   * @throws CelestaException в случае сбоя связи с БД.
   */
  public abstract DBColumnInfo getColumnInfo(Connection conn, Column c) throws CelestaException;

  /**
   * Обновляет на таблице колонку.
   *
   * @param conn Соединение с БД.
   * @param c    Колонка для обновления.
   * @throws CelestaException при ошибке обновления колонки.
   */
  public abstract void updateColumn(Connection conn, Column c, DBColumnInfo actual) throws CelestaException;

  /**
   * Возвращает информацию о первичном ключе таблицы.
   *
   * @param conn Соединение с БД.
   * @param t    Таблица, информацию о первичном ключе которой необходимо
   *             получить.
   * @throws CelestaException в случае сбоя связи с БД.
   */
  public abstract DBPKInfo getPKInfo(Connection conn, TableElement t) throws CelestaException;

  /**
   * Удаляет первичный ключ на таблице с использованием известного имени
   * первичного ключа.
   *
   * @param conn   Соединение с базой данных.
   * @param t      Таблица.
   * @param pkName Имя первичного ключа.
   * @throws CelestaException в случае сбоя связи с БД.
   */
  public abstract void dropPK(Connection conn, TableElement t, String pkName) throws CelestaException;

  /**
   * Создаёт первичный ключ на таблице в соответствии с метаописанием.
   *
   * @param conn Соединение с базой данных.
   * @param t    Таблица.
   * @throws CelestaException неудача создания первичного ключа (например, неуникальные
   *                          записи).
   */
  public abstract void createPK(Connection conn, TableElement t) throws CelestaException;

  public abstract List<DBFKInfo> getFKInfo(Connection conn, Grain g) throws CelestaException;

  /**
   * Возвращает набор индексов, связанных с таблицами, лежащими в указанной
   * грануле.
   *
   * @param conn Соединение с БД.
   * @param g    Гранула, по таблицам которой следует просматривать индексы.
   * @throws CelestaException В случае сбоя связи с БД.
   */
  public abstract Map<String, DBIndexInfo> getIndices(Connection conn, Grain g) throws CelestaException;

  /**
   * Возвращает перечень имён представлений в грануле.
   *
   * @param conn Соединение с БД.
   * @param g    Гранула, перечень имён представлений которой необходимо
   *             получить.
   * @throws CelestaException В случае сбоя связи с БД.
   */
  public List<String> getViewList(Connection conn, Grain g) throws CelestaException {
    String sql = String.format("select table_name from information_schema.views where table_schema = '%s'",
        g.getName());
    List<String> result = new LinkedList<>();
    try {
      Statement stmt = conn.createStatement();
      try {
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
          result.add(rs.getString(1));
        }
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException("Cannot get views list: %s", e.toString());
    }
    return result;
  }

  abstract public List<String> getParameterizedViewList(Connection conn, Grain g) throws CelestaException;

  abstract public void dropParameterizedView (Connection conn, String grainName, String viewName) throws CelestaException;

  public String getCallFunctionSql(ParameterizedView pv) throws CelestaException {
    return String.format(
        tableTemplate() + "(%s)",
        pv.getGrain().getName(), pv.getName(),
        pv.getParameters().keySet().stream()
            .map(p -> "?")
            .collect(Collectors.joining(", "))
    );
  }


  /**
   * Создаёт представление в базе данных на основе метаданных.
   *
   * @param conn Соединение с БД.
   * @param v    Представление.
   * @throws CelestaException Ошибка БД.
   */
  public void createView(Connection conn, View v) throws CelestaException {
    SQLGenerator gen = getViewSQLGenerator();
    try {
      StringWriter sw = new StringWriter();
      BufferedWriter bw = new BufferedWriter(sw);

      v.createViewScript(bw, gen);
      bw.flush();

      String sql = sw.toString();
      // System.out.println(sql);

      Statement stmt = conn.createStatement();
      try {
        stmt.executeUpdate(sql);
      } finally {
        stmt.close();
      }
    } catch (SQLException | IOException e) {
      throw new CelestaException("Error while creating view %s.%s: %s", v.getGrain().getName(), v.getName(),
          e.getMessage());

    }

  }

  public abstract void createParameterizedView(Connection conn, ParameterizedView pv) throws CelestaException;

  /**
   * Возвращает транслятор из языка CelestaSQL в язык нужного диалекта БД.
   */
  public abstract SQLGenerator getViewSQLGenerator();

  /**
   * Удаление представления.
   *
   * @param conn      Соединение с БД.
   * @param grainName Имя гранулы.
   * @param viewName  Имя представления.
   * @throws CelestaException Ошибка БД.
   */
  public void dropView(Connection conn, String grainName, String viewName) throws CelestaException {
    try {
      String sql = String.format("DROP VIEW " + tableTemplate(), grainName, viewName);
      Statement stmt = conn.createStatement();
      try {
        stmt.executeUpdate(sql);
      } finally {
        stmt.close();
      }
      conn.commit();
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
  }

  /**
   * Создаёт или пересоздаёт прочие системные объекты (хранимые процедуры,
   * функции), необходимые для функционирования Celesta на текущей СУБД.
   *
   * @param conn Соединение.
   * @throws CelestaException Ошибка создания объектов.
   */
  public void createSysObjects(Connection conn) throws CelestaException {

  }

  /**
   * Обновляет триггер контроля версий на таблице.
   *
   * @param conn Соединение.
   * @param t    Таблица (версионируемая или не версионируемая).
   * @throws CelestaException Ошибка создания или удаления триггера.
   */
  public abstract void updateVersioningTrigger(Connection conn, TableElement t) throws CelestaException;

  /**
   * Возвращает Process Id текущего подключения к базе данных.
   *
   * @param conn Соединение с БД.
   * @throws CelestaException Если подключение закрылось.
   */
  public abstract int getDBPid(Connection conn);

  /**
   * Транслирует литерал даты Celesta в литерал даты, специфический для базы
   * данных.
   *
   * @param date Литерал даты.
   * @throws CelestaException ошибка парсинга.
   */
  public String translateDate(String date) throws CelestaException {
    try {
      DateTimeColumn.parseISODate(date);
    } catch (ParseException e) {
      throw new CelestaException(e.getMessage());
    }
    return date;
  }

  /**
   * Сбрасывает счётчик IDENTITY на таблице (если он есть).
   *
   * @param conn Соединение с БД
   * @param t    Таблица.
   * @param i    Новое значение счётчика IDENTITY.
   * @throws SQLException Ошибка соединения с БД.
   */
  public void resetIdentity(Connection conn, Table t, int i) throws SQLException {

    Statement stmt = conn.createStatement();
    try {
      String sql = String.format(
          "update \"celesta\".\"sequences\" set \"seqvalue\" = %d "
              + "where \"grainid\" = '%s' and \"tablename\" = '%s'",
          i - 1, t.getGrain().getName(), t.getName());

      // System.out.println(sql);
      int v = stmt.executeUpdate(sql);
      if (v == 0) {
        sql = String.format("insert into \"celesta\".\"sequences\" (\"grainid\", \"tablename\" , \"seqvalue\") "
            + "values ('%s', '%s', %d)", t.getGrain().getName(), t.getName(), i - 1);
        // System.out.println(sql);
        stmt.executeUpdate(sql);
      }
    } finally {
      stmt.close();
    }
  }

  abstract public void createTableTriggersForMaterializedViews(Connection conn, Table t)
      throws CelestaException;

  abstract public void dropTableTriggersForMaterializedViews(Connection conn, Table t)
      throws CelestaException;

  public Optional<String> getTriggerBody(Connection conn, TriggerQuery query) throws CelestaException {
    String sql = getSelectTriggerBodySql(query);

    try {
      Statement stmt = conn.createStatement();
      try {
        Optional<String> result;
        ResultSet rs = stmt.executeQuery(sql);

        if (rs.next()) {
          result = Optional.ofNullable(rs.getString(1));
        } else {
          result = Optional.empty();
        }
        rs.close();
        return result;
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException("Could't select body of trigger %s", query.getName());
    }
  };

  public void initDataForMaterializedView(Connection conn, MaterializedView mv)
      throws CelestaException {
    Table t = mv.getRefTable().getTable();

    String mvIdentifier = String.format(tableTemplate(), mv.getGrain().getName(), mv.getName());
    String mvColumns = mv.getColumns().keySet().stream()
        .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
        .map(alias -> "\"" + alias + "\"")
        .collect(Collectors.joining(", "))
        .concat(", \"").concat(MaterializedView.SURROGATE_COUNT).concat("\"");

    String tableGroupByColumns = mv.getColumns().values().stream()
        .filter(v -> mv.isGroupByColumn(v.getName()))
        .map(v -> {
          Column colRef = mv.getColumnRef(v.getName());
          String groupByColStr =  "\"" + mv.getColumnRef(v.getName()).getName() + "\"";

          if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType()))
            return truncDate(groupByColStr);
          return groupByColStr;
        })
        .collect(Collectors.joining(", "));

    String deleteSql = "TRUNCATE TABLE " + mvIdentifier;

    String colsToSelect = mv.getColumns().keySet().stream()
        .filter(alias -> !MaterializedView.SURROGATE_COUNT.equals(alias))
        .map(alias -> {
          Column colRef = mv.getColumnRef(alias);
          Map<String, Expr> aggrCols = mv.getAggregateColumns();

          if (aggrCols.containsKey(alias)) {
            Expr agrExpr = aggrCols.get(alias);
            if (agrExpr instanceof Count) {
              return "COUNT(*)";
            } else if (agrExpr instanceof Sum) {
              return "SUM(\"" + colRef.getName() + "\")";
            } else {
              throw new RuntimeException(
                  String.format(
                    "Aggregate func of type %s is not supported",
                    agrExpr.getClass().getSimpleName()
                )
              );
            }
          } else {
            if (DateTimeColumn.CELESTA_TYPE.equals(colRef.getCelestaType()))
              return truncDate("\"" + colRef.getName() + "\"");
            return "\"" + colRef.getName() + "\"";

          }
        }).collect(Collectors.joining(", "));

    String selectScript = String.format("SELECT " + colsToSelect + ", COUNT(*)"
        + " FROM " + tableTemplate() + " GROUP BY %s",
        t.getGrain().getName(), t.getName(), tableGroupByColumns);
    String insertSql = String.format("INSERT INTO %s (%s) "  + selectScript, mvIdentifier, mvColumns);

    try {
      Statement stmt = conn.createStatement();

      try {
        stmt.execute(deleteSql);
        stmt.execute(insertSql);
      } finally {
        stmt.close();
      }
    } catch (SQLException e) {
      throw new CelestaException("Can't init data for materialized view %s: %s",
          mvIdentifier, e);
    }
  }

  /**
   * Возвращает sql с функцией округления timestamp до даты.
   * @param dateStr значение, которое нужно округлить
   * @return
   */
  abstract String truncDate(String dateStr);

  @Override
  public List<String> selectStaticStrings(
          List<String> data, String columnName, String orderBy) throws CelestaException {

    //prepare sql
    String sql = data.stream().map(
            str -> {
              final String rowStr = prepareRowColumnForSelectStaticStrings(str, columnName);
              return String.format("SELECT %s %s", rowStr, constantFromSql());
            })
            .collect(Collectors.joining(" UNION ALL "));

    if (orderBy != null && !orderBy.isEmpty())
      sql = sql + " ORDER BY " + orderBy;

    try (Connection conn = connectionPool.get();
      PreparedStatement ps = conn.prepareStatement(sql)
    ) {
      //fill preparedStatement
      AtomicInteger paramCounter = new AtomicInteger(1);
      data.forEach(
              str -> {
                try {
                  ps.setString(paramCounter.getAndIncrement(), str);
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              });

      //execute query and parse result
      ResultSet rs = ps.executeQuery();

      List<String> result = new ArrayList<>();

      while (rs.next()) {
        String str = rs.getString(1);
        result.add(str);
      }

      return result;
    } catch (Exception e) {
      throw new CelestaException("Can't select static data", e);
    }
  }

  String constantFromSql() {
    return "";
  }

  String prepareRowColumnForSelectStaticStrings(String value, String colName) {
    return "? as " + colName;
  }

  @Override
  public int compareStrings(String left, String right) throws CelestaException {

    List<String> comparisons = Arrays.asList("<", "=", ">");

    String sql = comparisons.stream()
            .map(comparison ->
                    "select count(*) " +
                            " FROM ( SELECT " + prepareRowColumnForSelectStaticStrings("?" , "a")
                            + " " + constantFromSql() +  ") r " +
                            " where a " + comparison + " ?"
            )
            .collect(Collectors.joining(" UNION ALL "));

    try (Connection conn = connectionPool.get();
         PreparedStatement ps = conn.prepareStatement(sql)
    ) {
      for (int i = 1; i < comparisons.size() * 2; i += 2) {
        ps.setString(i, left);
        ps.setString(i + 1, right);
      }

      ResultSet rs = ps.executeQuery();

      int result = -1;

      while (rs.next()) {
        boolean compareResult = rs.getBoolean(1);

        if (compareResult)
          break;

        ++result;
      }

      return result;
    } catch (Exception e) {
      throw new CelestaException("Can't compare strings", e);
    }
  }

  public abstract AppSettings.DBType getType();

  @Override
  public boolean supportsCortegeComparing() {
    return false;
  }
}

/**
 * Класс, ответственный за генерацию определения столбца таблицы в разных СУБД.
 */
abstract class ColumnDefiner {
  static final String DEFAULT = "default ";

  abstract String dbFieldType();

  /**
   * Возвращает определение колонки, содержащее имя, тип и NULL/NOT NULL (без
   * DEFAULT). Требуется для механизма изменения колонок.
   *
   * @param c колонка.
   */
  abstract String getMainDefinition(Column c);

  /**
   * Отдельно возвращает DEFAULT-определение колонки.
   *
   * @param c колонка.
   */
  abstract String getDefaultDefinition(Column c);

  /**
   * Возвращает полное определение колонки (для создания колонки).
   *
   * @param c колонка
   */
  String getFullDefinition(Column c) {
    return join(getMainDefinition(c), getDefaultDefinition(c));
  }

  String nullable(Column c) {
    return c.isNullable() ? "null" : "not null";
  }

  /**
   * Соединяет строки через пробел.
   *
   * @param ss массив строк для соединения в виде свободного параметра.
   */
  static String join(String... ss) {
    StringBuilder sb = new StringBuilder();
    boolean multiple = false;
    for (String s : ss)
      if (!"".equals(s)) {
        if (multiple)
          sb.append(' ' + s);
        else {
          sb.append(s);
          multiple = true;
        }
      }
    return sb.toString();
  }
}