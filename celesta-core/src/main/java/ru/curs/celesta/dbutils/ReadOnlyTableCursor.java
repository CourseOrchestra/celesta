package ru.curs.celesta.dbutils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.ReadOnlyTable;

/**
 * Cursor for tables that are defined only for reading.
 */
public abstract class ReadOnlyTableCursor extends BasicCursor {
    private ReadOnlyTable meta = null;

    public ReadOnlyTableCursor(CallContext context) {
        super(context);
    }

    public ReadOnlyTableCursor(CallContext context, ColumnMeta<?>... columns) {
        this(context, Arrays.stream(columns).map(ColumnMeta::getName).collect(Collectors.toSet()));
    }

    public ReadOnlyTableCursor(CallContext context, Set<String> fields) {
        super(context, fields);
    }

    /**
     * Creates a read only table specific cursor.
     *
     * @param table  Cursor related table
     * @param callContext  Call context that is used for cursor creation
     * @return
     */
    public static ReadOnlyTableCursor create(ReadOnlyTable table, CallContext callContext) {
        return (ReadOnlyTableCursor) BasicCursor.create(table, callContext);
    }

    /**
     * Creates a table specific cursor.
     *
     * @param table  Cursor related table
     * @param callContext  Call context that is used for cursor creation
     * @param fields  Fields the cursor should operate on
     * @return
     */
    public static ReadOnlyTableCursor create(ReadOnlyTable table, CallContext callContext, Set<String> fields) {
        return (ReadOnlyTableCursor) BasicCursor.create(table, callContext, fields);
    }

    @Override
    public final ReadOnlyTable meta() {
        if (meta == null) {
            try {
                meta = callContext().getScore()
                        .getGrain(_grainName()).getElement(_objectName(), ReadOnlyTable.class);
            } catch (ParseException e) {
                throw new CelestaException(e.getMessage());
            }
        }

        return meta;
    }

    @Override
    final void appendPK(List<String> l, List<Boolean> ol, final Set<String> colNames) {

        if (meta().getPrimaryKey().isEmpty() && colNames.isEmpty()) {
            // If there's absolutely no sorting it will be sorted by the first field. 
            l.add(String.format("\"%s\"", meta().getColumns().keySet().iterator().next()));
            ol.add(Boolean.FALSE);
        } else {
            // Always add to the end of OrderBy the fields of the primary key following in
            // a natural order.
            for (String colName : meta().getPrimaryKey().keySet()) {
                if (!colNames.contains(colName)) {
                    l.add(String.format("\"%s\"", colName));
                    ol.add(Boolean.FALSE);
                }
            }
        }
    }

}
