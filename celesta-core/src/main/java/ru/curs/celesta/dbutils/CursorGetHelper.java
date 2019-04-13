package ru.curs.celesta.dbutils;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.stmt.PreparedStatementHolderFactory;
import ru.curs.celesta.dbutils.stmt.PreparedStmtHolder;
import ru.curs.celesta.score.TableElement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ioann
 * @since 2017-07-06
 */
class CursorGetHelper {

  private static final Logger LOGGER = LoggerFactory.getLogger(CursorGetHelper.class);

  @FunctionalInterface
  interface ParseResultFunction {
    void apply(ResultSet rs) throws SQLException;
  }

  @FunctionalInterface
  interface ParseResultCallBack {
    void apply();
  }

  private final DBAdaptor db;
  private final Connection conn;
  private final TableElement meta;
  private final String tableName;
  private final Set<String> fields;

  private final PreparedStmtHolder get;

  public CursorGetHelper(DBAdaptor db, Connection conn, TableElement meta,
                         String tableName, Set<String> fields) {
    this.db = db;
    this.conn = conn;
    this.meta = meta;
    this.tableName = tableName;
    this.fields = fields;

    this.get = PreparedStatementHolderFactory.createGetHolder(meta, db, conn);
  }


  PreparedStmtHolder getHolder() {
    return get;
  }


  final boolean internalGet(ParseResultFunction parseResultFunc, Optional<ParseResultCallBack> initXRecFunc,
                            int recversion, Object... values) {
    PreparedStatement g = prepareGet(recversion, values);
    LOGGER.trace("{}", g);
    try (ResultSet rs = g.executeQuery()){
        boolean result = rs.next();
        if (result) {
          parseResultFunc.apply(rs);
          initXRecFunc.ifPresent(ParseResultCallBack::apply);
        }
        return result;
    } catch (SQLException e) {
      throw new CelestaException(e.getMessage());
    }

  }


  final PreparedStatement prepareGet(int recversion, Object... values) {
    if (meta.getPrimaryKey().size() != values.length) {
      throw new CelestaException("Invalid number of 'get' arguments for '%s': expected %d, provided %d.",
          tableName, meta.getPrimaryKey().size(), values.length);
    }
    PreparedStatement result = get.getStatement(values, recversion);
    return result;
  }


  static class CursorGetHelperBuilder {
    private DBAdaptor db;
    private Connection conn;
    private TableElement meta;
    private String tableName;
    private Set<String> fields = Collections.emptySet();

    CursorGetHelperBuilder withDb(DBAdaptor db) {
      this.db = db;
      return this;
    }

    CursorGetHelperBuilder withConn(Connection conn) {
      this.conn = conn;
      return this;
    }

    CursorGetHelperBuilder withMeta(TableElement meta) {
      this.meta = meta;
      return this;
    }

    CursorGetHelperBuilder withTableName(String tableName) {
      this.tableName = tableName;
      return this;
    }

    CursorGetHelperBuilder withFields(Set<String> fields) {
      if (!fields.isEmpty()) {
        this.fields = fields;
      }
      return this;
    }

    CursorGetHelper build() {
      return new CursorGetHelper(db, conn, meta, tableName, fields);
    }
  }
}
