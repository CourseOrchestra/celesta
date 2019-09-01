package ru.curs.celesta.dbutils;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.filter.In;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.View;

/**
 * Base class of cursor for viewing data in views.
 */
public abstract class ViewCursor extends BasicCursor implements InFilterSupport {

    private View meta = null;
    private InFilterHolder inFilterHolder;

    public ViewCursor(CallContext context) {
        super(context);
        inFilterHolder = new InFilterHolder(this);
    }

    public ViewCursor(CallContext context, ColumnMeta<?>... columns) {
        this(context, Arrays.stream(columns).map(ColumnMeta::getName).collect(Collectors.toSet()));
    }

    public ViewCursor(CallContext context, Set<String> fields) {
        super(context, fields);
        inFilterHolder = new InFilterHolder(this);
    }

    /**
     * Creates a view specific cursor.
     *
     * @param view  Cursor related view
     * @param callContext  Call context that is used for cursor creation
     * @return
     */
    public static ViewCursor create(View view, CallContext callContext) {
        return (ViewCursor) BasicCursor.create(view, callContext);
    }

    /**
     * Creates a view specific cursor.
     *
     * @param view  Cursor related view
     * @param callContext  Call context that is used for cursor creation
     * @param fields  Fields the cursor should operate on
     * @return
     */
    public static ViewCursor create(View view, CallContext callContext, Set<String> fields) {
        return (ViewCursor) BasicCursor.create(view, callContext, fields);
    }

    /**
     * Returns view description (meta information).
     *
     * @return
     */
    @Override
    public View meta() {
        if (meta == null) {
            try {
                meta = callContext().getScore()
                        .getGrain(_grainName()).getElement(_objectName(), View.class);
            } catch (ParseException e) {
                throw new CelestaException(e.getMessage());
            }
        }

        return meta;
    }

    @Override
    final void appendPK(List<String> l, List<Boolean> ol, final Set<String> colNames) {
        // The views are always sorted by the first column if there's no sorting at all.
        if (colNames.isEmpty()) {
            l.add(String.format("\"%s\"", meta().getColumns().keySet().iterator().next()));
            ol.add(Boolean.FALSE);
        }
    }


    @Override
    public FieldsLookup setIn(BasicCursor otherCursor) {
        return inFilterHolder.setIn(otherCursor);
    }

    @Override
    public In getIn() {
        return inFilterHolder.getIn();
    }

    @Override
    protected void resetSpecificState() {
        inFilterHolder = new InFilterHolder(this);
    }

    @Override
    protected void clearSpecificState() {
        inFilterHolder = new InFilterHolder(this);
    }

    @Override
    protected void copySpecificFiltersFrom(BasicCursor bc) {
        ViewCursor c = (ViewCursor) bc;
        inFilterHolder = c.inFilterHolder;
    }

    @Override
    boolean isEquivalentSpecific(BasicCursor bc) {
        ViewCursor c = (ViewCursor) bc;
        return Objects.equals(inFilterHolder, c.inFilterHolder);
    }

}
