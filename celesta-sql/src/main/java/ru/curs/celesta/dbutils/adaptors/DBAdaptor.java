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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.DBType;
import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.dbutils.adaptors.column.ColumnDefiner;
import ru.curs.celesta.dbutils.adaptors.column.ColumnDefinerFactory;
import ru.curs.celesta.dbutils.adaptors.ddl.DdlAdaptor;

import static ru.curs.celesta.dbutils.adaptors.function.CommonFunctions.*;

import ru.curs.celesta.dbutils.adaptors.ddl.DdlConsumer;
import ru.curs.celesta.dbutils.adaptors.ddl.DdlGenerator;
import ru.curs.celesta.dbutils.meta.*;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.dbutils.stmt.ParameterSetter;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.score.*;

import static ru.curs.celesta.dbutils.jdbc.SqlUtils.*;

/**
 * Adapter for connection to the database.
 */
public abstract class DBAdaptor implements QueryBuildingHelper, StaticDataAdaptor {

  /*
   * N.B. for contributors. This class is great, so To avoid chaos,
   * here is the order of (except constructors and fabric methods):
   * first of all -- public final methods,
   * then -- package-private static methods,
   * then -- package-private final methods,
   * then -- package-private methods,
   * then -- package-private abstract methods,
   * then -- public static methods,
   * then -- public final methods,
   * then -- public methods,
   * then -- public abstract methods,
   * then -- private methods
   */

  static final List<Class<? extends Column>> COLUMN_CLASSES = Arrays.asList(IntegerColumn.class, StringColumn.class, BooleanColumn.class,
      FloatingColumn.class, BinaryColumn.class, DateTimeColumn.class);
  static final String COLUMN_NAME = "COLUMN_NAME";

  protected final ConnectionPool connectionPool;
  DdlAdaptor ddlAdaptor;

  //TODO: Javadoc
  protected DBAdaptor(ConnectionPool connectionPool, DdlConsumer ddlConsumer) {
    this.connectionPool = connectionPool;
    this.ddlAdaptor = new DdlAdaptor(getDdlGenerator(), ddlConsumer);
    connectionPool.setDbAdaptor(this);
  }

  abstract DdlGenerator getDdlGenerator();

  // =========> PACKAGE-PRIVATE STATIC METHODS <=========

  /**
   * Creates a PreparedStatement object.
   * @param conn Connection to use.
   * @param sql SQL statement.
   * @return new default PreparedStatement object.
   * @throws CelestaException if a {@link SQLException} occurs.
   */
  static PreparedStatement prepareStatement(Connection conn, String sql) throws CelestaException {
    try {
      return conn.prepareStatement(sql);
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }
  }

  /**
   * Transforms {@link Iterable<String>} of field names into comma separated {@link String} field names.
   * Binary fields are excluded from result.
   * @param t the {@link DataGrainElement} type, that's owner of fields.
   * @param fields {@link Iterable<String>} fields to transform.
   * @return Comma separated {@link String} field names.
   */
  static String getTableFieldsListExceptBlobs(DataGrainElement t, Set<String> fields) {
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
    // To the list of fields of the versioned tables we necessarily add "recversion"
    if (t instanceof Table && ((Table) t).isVersioned())
      flds.add(VersionedElement.REC_VERSION);

    return getFieldList(flds);
  }

  /**
   * Returns {@link FKRule} by input string rule.
   * The method is case-insensitive for rule param.
   * @param rule input string.
   * @return Returns one of the values of {@link FKRule} or null in case of invalid input.
   */
  static FKRule getFKRule(String rule) {
    if ("NO ACTION".equalsIgnoreCase(rule) || "RECTRICT".equalsIgnoreCase(rule))
      return FKRule.NO_ACTION;
    if ("SET NULL".equalsIgnoreCase(rule))
      return FKRule.SET_NULL;
    if ("CASCADE".equalsIgnoreCase(rule))
      return FKRule.CASCADE;
    return null;
  }


  /**
   * Executes sql query and then adds a column values with index 1 to {@link Set<String>} to return.
   * @param conn Connection to use.
   * @param sql Sql query to execute.
   * @return {@link Set<String>} with values of column with index 1,
   *         which were received as a result of the sql query.
   * @throws CelestaException if a {@link SQLException} occurs.
   */
  static Set<String> sqlToStringSet(Connection conn, String sql) throws CelestaException {
    Set<String> result = new HashSet<>();
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

  // =========> END PACKAGE-PRIVATE STATIC METHODS <=========

  // =========> PACKAGE-PRIVATE FINAL METHODS <=========

  /**
   * Return String representation of sql query to select data with "ORDER BY" expression.
   * @param from FROM metadata.
   * @param whereClause WHERE clause to use in resulting query.
   * @param orderBy ORDER BY clause to use in resulting query.
   * @param fields fields for select by a resulting  query.
   * @return Return String representation of sql query to select data with "ORDER BY" expression.
   */
  final String getSelectFromOrderBy(
          FromClause from, String whereClause, String orderBy, Set<String> fields
  ) {
    final String fieldList = getTableFieldsListExceptBlobs(from.getGe(), fields);
    String sqlfrom = String.format("select %s from %s" , fieldList,
            from.getExpression());

    String sqlwhere = "".equals(whereClause) ? "" : " where " + whereClause;

    return sqlfrom + sqlwhere + " order by " + orderBy;
  }

  //TODO:Must be defined in single place
  final String columnDef(Column c) {
    return ColumnDefinerFactory
            .getColumnDefiner(getType(), c.getClass())
            .getFullDefinition(c);
  }
  // =========> END PACKAGE-PRIVATE FINAL METHODS <=========


  // =========> PACKAGE-PRIVATE METHODS <=========
  //TODO: Javadoc
  String constantFromSql() {
    return "";
  }

  //TODO: Javadoc
  String prepareRowColumnForSelectStaticStrings(String value, String colName) {
    return "? as " + colName;
  }

  //TODO: Javadoc
  <T extends Column> ColumnDefiner getColumnDefiner(T c) {
    return getColumnDefiner(c.getClass());
  }

  ColumnDefiner getColumnDefiner(Class<? extends Column> c) {
    return ColumnDefinerFactory.getColumnDefiner(getType(), c);
  }
  // =========> END PACKAGE-PRIVATE METHODS <=========


  // =========> PACKAGE-PRIVATE ABSTRACT METHODS <=========
  //TODO: Javadoc
  abstract String getLimitedSQL(
          FromClause from, String whereClause, String orderBy, long offset, long rowCount, Set<String> fields
  );

  //TODO: Javadoc
  abstract String getSelectTriggerBodySql(TriggerQuery query);

  //TODO: Javadoc
  abstract boolean userTablesExist(Connection conn) throws SQLException;

  //TODO: Javadoc
  abstract void createSchemaIfNotExists(Connection conn, String name) throws CelestaException;

  // =========> END PACKAGE-PRIVATE ABSTRACT METHODS <=========

  // =========> PUBLIC STATIC METHODS <=========
  // =========> END PUBLIC STATIC METHODS <=========


  // =========> PUBLIC FINAL METHODS <=========

  /**
   * Deletes table from RDBMS.
   *
   * @param conn Connection to use.
   * @param t TableElement metadata of deleting table provided by Celesta.
   * @throws CelestaException if a {@link SQLException} occurs.
   */
  public final void dropTable(Connection conn, TableElement t) throws CelestaException {
    this.ddlAdaptor.dropTable(conn, t);
  }

  /**
   * Возвращает true в том и только том случае, если база данных содержит
   * пользовательские таблицы (т. е. не является пустой базой данных).
   *
   * @throws CelestaException ошибка БД
   */
  //TODO: Javadoc In English
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
  //TODO: Javadoc In English
  public final void createSchemaIfNotExists(String name) throws CelestaException {
    try (Connection conn = connectionPool.get()) {
      createSchemaIfNotExists(conn, name);
    } catch (SQLException e) {
      throw new CelestaException("Cannot create schema. " + e.getMessage());
    }
  }

  public final void createColumn(Connection conn, Column c) throws CelestaException {
    this.ddlAdaptor.createColumn(conn, c);
  }

  // CHECKSTYLE:OFF 6 parameters
  //TODO: Javadoc
  public final PreparedStatement getUpdateRecordStatement(Connection conn, Table t, boolean[] equalsMask,
                                                          boolean[] nullsMask, List<ParameterSetter> program, String where)
          throws CelestaException {
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

    String sql = String.format("update " + tableString(t.getGrain().getName(), t.getName()) + " set %s where %s",
            setClause.toString(), where);

    // System.out.println(sql);
    return prepareStatement(conn, sql);
  }

  public final void createIndex(Connection conn, Index index) throws CelestaException {
    this.ddlAdaptor.createIndex(conn, index);
  }

  public final void createFK(Connection conn, ForeignKey fk) throws CelestaException {
    this.ddlAdaptor.createFk(conn, fk);
  }

  /**
   * Удаляет в грануле индекс на таблице.
   *
   * @param g           Гранула
   * @param dBIndexInfo Информация об индексе
   * @throws CelestaException Если что-то пошло не так.
   */
  //TODO: Javadoc In English
  public final void dropIndex(Grain g, DbIndexInfo dBIndexInfo) throws CelestaException {
    try (Connection conn = connectionPool.get()) { //TODO: Why there is a new Connection instance
        ddlAdaptor.dropIndex(conn, g, dBIndexInfo);
    } catch (CelestaException | SQLException e) {
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
  //TODO: Javadoc In English
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

  //TODO: Javadoc
  public final PreparedStatement getSetCountStatement(Connection conn, FromClause from, String whereClause)
          throws CelestaException {
    String sql = "select count(*) from " + from.getExpression()
            + ("".equals(whereClause) ? "" : " where " + whereClause);
    PreparedStatement result = prepareStatement(conn, sql);

    return result;
  }

  //TODO: Javadoc
  public final void dropTrigger(Connection conn, TriggerQuery query) throws CelestaException {
    ddlAdaptor.dropTrigger(conn, query);
  }

  public final void manageAutoIncrement(Connection conn, TableElement t) throws CelestaException {
    ddlAdaptor.manageAutoIncrement(conn, t);
  }

  public void updateVersioningTrigger(Connection conn, TableElement t) throws CelestaException {
    ddlAdaptor.updateVersioningTrigger(conn, t);
  }

  public final void createPK(Connection conn, TableElement t) throws CelestaException {
    this.ddlAdaptor.createPk(conn, t);
  }

  public final SQLGenerator getViewSQLGenerator() {
    return this.ddlAdaptor.getViewSQLGenerator();
  }

  /**
   * Создаёт представление в базе данных на основе метаданных.
   *
   * @param conn Соединение с БД.
   * @param v    Представление.
   * @throws CelestaException Ошибка БД.
   */
  //TODO: Javadoc In English
  public final void createView(Connection conn, View v) throws CelestaException {
    this.ddlAdaptor.createView(conn, v);
  }


  public final void createParameterizedView(Connection conn, ParameterizedView pv) throws CelestaException {
    this.ddlAdaptor.createParameterizedView(conn, pv);
  }

  public final void dropTableTriggersForMaterializedViews(Connection conn, Table t) throws CelestaException {
    this.ddlAdaptor.dropTableTriggersForMaterializedViews(conn, t);
  }

  public final void createTableTriggersForMaterializedViews(Connection conn, Table t) throws CelestaException {
    this.ddlAdaptor.createTableTriggersForMaterializedViews(conn, t);
  }

  public final void executeNative(Connection conn, String sql) throws CelestaException {
    this.ddlAdaptor.executeNative(conn, sql);
  }
  // =========> END PUBLIC FINAL METHODS <=========


  // =========> PUBLIC METHODS <=========
  /**
   * Проверка на валидность соединения.
   *
   * @param conn    соединение.
   * @param timeout тайм-аут.
   * @return true если соединение валидно, иначе false
   * @throws CelestaException при возникновении ошибки работы с БД.
   */
  //TODO: Javadoc In English
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
  //TODO: Javadoc In English
  public String tableString(String schemaName, String tableName) {
    StringBuilder sb = new StringBuilder();

    if (schemaName.startsWith("\""))
      sb.append(schemaName);
    else
      sb.append("\"").append(schemaName).append("\"");

    sb.append(".");

    if (tableName.startsWith("\""))
      sb.append(tableName);
    else
      sb.append("\"").append(tableName).append("\"");

    return sb.toString();
  }

  /**
   * Создаёт в базе данных таблицу "с нуля".
   *
   * @param conn Соединение.
   * @param te   Таблица для создания.
   * @throws CelestaException В случае возникновения критического сбоя при создании
   *                          таблицы, в том числе в случае, если такая таблица существует.
   */
  //TODO: Javadoc In English
  public void createTable(Connection conn, TableElement te) throws CelestaException {
    ddlAdaptor.createTable(conn, te);
  }

  /**
   * Возвращает набор имён столбцов определённой таблицы.
   *
   * @param conn Соединение с БД.
   * @param t    Таблица, по которой просматривать столбцы.
   * @throws CelestaException в случае сбоя связи с БД.
   */
  //TODO: Javadoc In English
  public Set<String> getColumns(Connection conn, TableElement t) throws CelestaException {
    Set<String> result = new LinkedHashSet<>();
    try {
      DatabaseMetaData metaData = conn.getMetaData();
      ResultSet rs = metaData.getColumns(null,
              t.getGrain().getName(),
              t.getName(), null);
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
   * Удаляет внешний ключ из базы данных.
   *
   * @param conn      Соединение с БД
   * @param schemaName имя гранулы
   * @param tableName Имя таблицы, на которой определён первичный ключ.
   * @param fkName    Имя внешнего ключа.
   * @throws CelestaException В случае сбоя в базе данных.
   */
  //TODO: Javadoc In English
  public void dropFK(Connection conn, String schemaName, String tableName, String fkName) throws CelestaException {
    try {
      this.ddlAdaptor.dropFK(conn, schemaName, tableName, fkName);
    } catch (CelestaException e) {
        throw new CelestaException("Cannot drop foreign key '%s': %s", fkName, e.getMessage());
    }
  }

  //TODO: Javadoc
  public void dropParameterizedView(Connection conn, String schemaName, String viewName) throws CelestaException {
    this.ddlAdaptor.dropParameterizedView(conn, schemaName, viewName);
  }

  /**
   * Возвращает перечень имён представлений в грануле.
   *
   * @param conn Соединение с БД.
   * @param g    Гранула, перечень имён представлений которой необходимо
   *             получить.
   * @throws CelestaException В случае сбоя связи с БД.
   */
  //TODO: Javadoc In English
  public List<String> getViewList(Connection conn, Grain g) throws CelestaException {
    String sql = String.format("select table_name from information_schema.views where table_schema = '%s'",
            g.getName());
    List<String> result = new LinkedList<>();
    try (ResultSet rs = executeQuery(conn, sql)) {
        while (rs.next()) {
          result.add(rs.getString(1));
        }
    } catch (SQLException | CelestaException e) {
      throw new CelestaException("Cannot get views list: %s", e.toString());
    }
    return result;
  }

  //TODO: Javadoc
  public String getCallFunctionSql(ParameterizedView pv) throws CelestaException {
    return String.format(
            tableString(pv.getGrain().getName(), pv.getName()) + "(%s)",
            pv.getParameters().keySet().stream()
                    .map(p -> "?")
                    .collect(Collectors.joining(", "))
    );
  }


  //TODO: Javadoc
  public void createSequence(Connection conn, SequenceElement s) throws CelestaException {
    ddlAdaptor.createSequence(conn, s);
  }

  //TODO: Javadoc
  public void alterSequence(Connection conn, SequenceElement s) throws CelestaException {
    ddlAdaptor.alterSequence(conn, s);
  }

  //TODO: Javadoc
  public void dropSequence(Connection conn, SequenceElement s) throws CelestaException {
    String sql = String.format("DROP SEQUENCE " + tableString(s.getGrain().getName(), s.getName()));
    executeUpdate(conn, sql);
  }

  /**
   * Удаление представления.
   *
   * @param conn      Соединение с БД.
   * @param schemaName Имя гранулы.
   * @param viewName  Имя представления.
   * @throws CelestaException Ошибка БД.
   */
  //TODO: Javadoc In English
  public void dropView(Connection conn, String schemaName, String viewName) throws CelestaException {
    ddlAdaptor.dropView(conn, schemaName, viewName);
  }

  /**
   * Создаёт или пересоздаёт прочие системные объекты (хранимые процедуры,
   * функции), необходимые для функционирования Celesta на текущей СУБД.
   *
   * @param conn Соединение.
   * @throws CelestaException Ошибка создания объектов.
   */
  //TODO: Javadoc In English
  public void createSysObjects(Connection conn, String sysSchemaName) throws CelestaException {

  }

  /**
   * Транслирует литерал даты Celesta в литерал даты, специфический для базы
   * данных.
   *
   * @param date Литерал даты.
   * @throws CelestaException ошибка парсинга.
   */
  //TODO: Javadoc In English
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
  //TODO: Javadoc In English
  public void resetIdentity(Connection conn, Table t, int i) throws CelestaException {

      String sql = String.format(
              "update \"celesta\".\"sequences\" set \"seqvalue\" = %d "
                      + "where \"grainid\" = '%s' and \"tablename\" = '%s'",
              i - 1, t.getGrain().getName(), t.getName());

      // System.out.println(sql);
      int v = executeUpdate(conn, sql);
      if (v == 0) {
        sql = String.format("insert into \"celesta\".\"sequences\" (\"grainid\", \"tablename\" , \"seqvalue\") "
                + "values ('%s', '%s', %d)", t.getGrain().getName(), t.getName(), i - 1);
        // System.out.println(sql);
        executeUpdate(conn, sql);
      }

  }

  //TODO: Javadoc
  public Optional<String> getTriggerBody(Connection conn, TriggerQuery query) throws CelestaException {
    String sql = getSelectTriggerBodySql(query);

    try (ResultSet rs = executeQuery(conn, sql)) {
        Optional<String> result;

        if (rs.next()) {
          result = Optional.ofNullable(rs.getString(1));
        } else {
          result = Optional.empty();
        }

        return result;
    } catch (CelestaException | SQLException e) {
      throw new CelestaException("Could't select body of trigger %s", query.getName());
    }
  }

  //TODO: Javadoc
  public void initDataForMaterializedView(Connection conn, MaterializedView mv) throws CelestaException {
      this.ddlAdaptor.initDataForMaterializedView(conn, mv);
  }

  //TODO: Javadoc
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

  //TODO: Javadoc
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

  //TODO: Javadoc
  @Override
  public boolean supportsCortegeComparing() {
    return false;
  }

  /**
   * Удаляет первичный ключ на таблице с использованием известного имени
   * первичного ключа.
   *
   * @param conn   Соединение с базой данных.
   * @param t      Таблица.
   * @param pkName Имя первичного ключа.
   * @throws CelestaException в случае сбоя связи с БД.
   */
  //TODO: Javadoc In English
  public void dropPk(Connection conn, TableElement t, String pkName) throws CelestaException {
    ddlAdaptor.dropPk(conn, t, pkName);
  }

  /**
   * Обновляет на таблице колонку.
   *
   * @param conn Соединение с БД.
   * @param c    Колонка для обновления.
   * @throws CelestaException при ошибке обновления колонки.
   */
  //TODO: Javadoc In English
  public void updateColumn(Connection conn, Column c, DbColumnInfo actual) throws CelestaException {
    ddlAdaptor.updateColumn(conn, c, actual);
  }
  // =========> END PUBLIC METHODS <=========

  // =========> PUBLIC ABSTRACT METHODS <=========
  /**
   * Возвращает навигационный PreparedStatement по фильтрованному набору
   * записей.
   *
   * @param conn                  Соединение.
   * @param orderBy               Порядок сортировки (прямой или обратный).
   * @param navigationWhereClause Условие навигационного набора (от текущей записи).
   */
  //TODO: Javadoc In English
  public abstract PreparedStatement getNavigationStatement(
      Connection conn, FromClause from, String orderBy,
      String navigationWhereClause, Set<String> fields, long offset
  ) throws CelestaException;

  //TODO: Javadoc
  public abstract boolean tableExists(Connection conn, String schema, String name) throws CelestaException;

  //TODO: Javadoc
  public abstract boolean triggerExists(Connection conn, TriggerQuery query) throws SQLException;

  //TODO: Javadoc
  public abstract PreparedStatement getOneRecordStatement(Connection conn, TableElement t,
                                                          String where, Set<String> fields) throws CelestaException;
  //TODO: Javadoc
  public abstract PreparedStatement getOneFieldStatement(Connection conn, Column c, String where) throws CelestaException;

  //TODO: Javadoc
  public abstract PreparedStatement deleteRecordSetStatement(Connection conn, TableElement t, String where) throws CelestaException;

  //TODO: Javadoc
  public abstract PreparedStatement getInsertRecordStatement(Connection conn, Table t, boolean[] nullsMask,
                                                             List<ParameterSetter> program) throws CelestaException;
  //TODO: Javadoc
  public abstract int getCurrentIdent(Connection conn, Table t) throws CelestaException;

  //TODO: Javadoc
  public abstract PreparedStatement getDeleteRecordStatement(Connection conn, TableElement t, String where) throws CelestaException;


  /**
   * Возвращает информацию о столбце.
   *
   * @param conn Соединение с БД.
   * @param c    Столбец.
   * @throws CelestaException в случае сбоя связи с БД.
   */
  //TODO: Javadoc In English
  public abstract DbColumnInfo getColumnInfo(Connection conn, Column c) throws CelestaException;

  /**
   * Возвращает информацию о первичном ключе таблицы.
   *
   * @param conn Соединение с БД.
   * @param t    Таблица, информацию о первичном ключе которой необходимо
   *             получить.
   * @throws CelestaException в случае сбоя связи с БД.
   */
  //TODO: Javadoc In English
  public abstract DbPkInfo getPKInfo(Connection conn, TableElement t) throws CelestaException;

  //TODO: Javadoc
  public abstract List<DbFkInfo> getFKInfo(Connection conn, Grain g) throws CelestaException;

  /**
   * Возвращает набор индексов, связанных с таблицами, лежащими в указанной
   * грануле.
   *
   * @param conn Соединение с БД.
   * @param g    Гранула, по таблицам которой следует просматривать индексы.
   * @throws CelestaException В случае сбоя связи с БД.
   */
  //TODO: Javadoc In English
  public abstract Map<String, DbIndexInfo> getIndices(Connection conn, Grain g) throws CelestaException;

  //TODO: Javadoc
  public abstract List<String> getParameterizedViewList(Connection conn, Grain g) throws CelestaException;

  /**
   * Возвращает Process Id текущего подключения к базе данных.
   *
   * @param conn Соединение с БД.
   * @throws CelestaException Если подключение закрылось.
   */
  //TODO: Javadoc In English
  public abstract int getDBPid(Connection conn);

  //TODO: Javadoc
  public abstract DBType getType();

  //TODO: Javadoc
  public abstract long nextSequenceValue(Connection conn, SequenceElement s) throws CelestaException;

  //TODO: Javadoc
  public abstract boolean sequenceExists(Connection conn, String schema, String name) throws CelestaException;

  //TODO: Javadoc
  public abstract DbSequenceInfo getSequenceInfo(Connection conn, SequenceElement s) throws CelestaException;
  // =========> END PUBLIC ABSTRACT METHODS <=========
}