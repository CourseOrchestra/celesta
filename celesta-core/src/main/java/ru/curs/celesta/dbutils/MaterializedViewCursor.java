package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.PermissionDeniedException;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.MaterializedView;
import ru.curs.celesta.score.ParseException;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Materialized view Cursor.
 *
 * @author ioann
 * @since 2017-07-06
 */
public abstract class MaterializedViewCursor extends BasicCursor {

  private MaterializedView meta = null;
  private final CursorGetHelper getHelper;


  public MaterializedViewCursor(CallContext context) {
    super(context);

    CursorGetHelper.CursorGetHelperBuilder cghb = new CursorGetHelper.CursorGetHelperBuilder();
    cghb.withDb(db())
        .withConn(conn())
        .withMeta(meta())
        .withTableName(_objectName());

    getHelper = cghb.build();
  }

  public MaterializedViewCursor(CallContext context, ColumnMeta<?>... columns) {
      this(context, Arrays.stream(columns).map(ColumnMeta::getName).collect(Collectors.toSet()));
  }

  public MaterializedViewCursor(CallContext context, Set<String> fields) {
    super(context, fields);

    CursorGetHelper.CursorGetHelperBuilder cghb = new CursorGetHelper.CursorGetHelperBuilder();
    cghb.withDb(db())
        .withConn(conn())
        .withMeta(meta())
        .withTableName(_objectName())
        .withFields(fieldsForStatement);

    getHelper = cghb.build();
  }

  /**
   * Creates a materialized view specific cursor.
   *
   * @param view  Cursor related materialized view
   * @param callContext  Call context that is used for cursor creation
   * @return
   */
  public static MaterializedViewCursor create(MaterializedView view, CallContext callContext) {
      return (MaterializedViewCursor) BasicCursor.create(view, callContext);
  }

  /**
   * Creates a materialized view specific cursor.
   *
   * @param view  Cursor related materialized view
   * @param callContext  Call context that is used for cursor creation
   * @param fields  Fields the cursor should operate on
   * @return
   */
  public static MaterializedViewCursor create(MaterializedView view, CallContext callContext, Set<String> fields) {
      return (MaterializedViewCursor) BasicCursor.create(view, callContext, fields);
  }

  /**
   * Returns materialized view description (meta information).
   *
   * @return
   */
  @Override
  public MaterializedView meta() {
    if (meta == null) {
      try {
        meta = callContext().getScore()
            .getGrain(_grainName()).getElement(_objectName(), MaterializedView.class);
      } catch (ParseException e) {
        throw new CelestaException(e.getMessage());
      }
    }

    return meta;
  }


  @Override
  final void appendPK(List<String> l, List<Boolean> ol, final Set<String> colNames) {
    // Always add to the end of OrderBy the fields of the primary key following in
    // a natural order.
    for (String colName : meta().getPrimaryKey().keySet()) {
      if (!colNames.contains(colName)) {
        l.add(String.format("\"%s\"", colName));
        ol.add(Boolean.FALSE);
      }
    }
  }

  /**
   * Performs a search of a record by the key fields, throwing an exception if
   * the record is not found.
   *
   * @param values   values of the key fields
   */
  public final void get(Object... values) {
    if (!tryGet(values)) {
      StringBuilder sb = new StringBuilder();
      for (Object value : values) {
        if (sb.length() > 0) {
          sb.append(", ");
        }
        sb.append(value == null ? "null" : value.toString());
      }
      throw new CelestaException("There is no %s (%s).", _objectName(), sb.toString());
    }
  }

  /**
   * Tries to perform a search of a record by the key fields, returning a value whether
   * the record was found or not.
   *
   * @param values  values of the key fields
   * @return  {@code true} if the record is found, otherwise - {@code false}
   */
  public final boolean tryGet(Object... values) {
    if (!canRead()) {
      throw new PermissionDeniedException(callContext(), meta(), Action.READ);
    }

    return getHelper.internalGet(this::_parseResult, Optional.empty(),
        0, values);
  }

  /**
   * Retrieves a record from the database that corresponds to the fields of current
   * primary key.
   *
   * @return  {@code true} if the record is retrieved, otherwise - {@code false}
   */
  public final boolean tryGetCurrent() {
    if (!canRead()) {
      throw new PermissionDeniedException(callContext(), meta(), Action.READ);
    }
    return getHelper.internalGet(this::_parseResult, Optional.empty(),
        0, _currentKeyValues());
  }

  /**
   * Returns an array of field values of the primary key.
   *
   * @return
   */
  public Object[] getCurrentKeyValues() {
    return _currentKeyValues();
  }

  // CHECKSTYLE:OFF
    /*
     * This group of methods is named according to Python rules, and not Java.
     * In Python names of protected methods are started with an underscore symbol.
     * When using methods without an underscore symbol conflicts with attribute names
     * may happen.
     */

  protected abstract Object[] _currentKeyValues();

}
