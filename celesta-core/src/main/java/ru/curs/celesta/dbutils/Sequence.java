package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.SequenceElement;

public abstract class Sequence extends BasicDataAccessor {

    private SequenceElement meta = null;

    public Sequence(CallContext context) {
        super(context);
    }

    public long nextValue() {
        return db().nextSequenceValue(conn(), meta());
    }

    @Override
    public void clear() {

    }

    /**
     * Описание представления (метаинформация).
     */
    @Override
    public final SequenceElement meta() {
        if (meta == null)
            try {
                meta = callContext().getScore()
                        .getGrain(_grainName()).getElement(_objectName(), SequenceElement.class);
            } catch (ParseException e) {
                throw new CelestaException(e.getMessage());
            }
        return meta;
    }
}
