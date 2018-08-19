package ru.curs.celesta.dbutils;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.filter.In;
import ru.curs.celesta.dbutils.filter.value.FieldsLookup;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.View;

/**
 * Базовый класс курсора для посмотра данных в представлениях.
 */
public abstract class ViewCursor extends BasicCursor implements InFilterSupport {

    private View meta = null;
    private InFilterHolder inFilterHolder;

    public ViewCursor(CallContext context) {
        super(context);
        inFilterHolder = new InFilterHolder(this);
    }

    public ViewCursor(CallContext context, Set<String> fields) {
        super(context, fields);
        inFilterHolder = new InFilterHolder(this);
    }

    /**
     * Описание представления (метаинформация).
     */
    @Override
    public View meta() {
        if (meta == null)
            try {
                meta = callContext().getScore()
                        .getGrain(_grainName()).getElement(_objectName(), View.class);
            } catch (ParseException e) {
                throw new CelestaException(e.getMessage());
            }
        return meta;
    }

    @Override
    final void appendPK(List<String> l, List<Boolean> ol, Set<String> colNames) {
        // для представлений мы сортируем всегда по первому столбцу, если
        // сортировки нет вообще
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
