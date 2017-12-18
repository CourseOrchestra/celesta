package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Sequence;

public abstract class SequenceCursor extends BasicDataAccessor {

    private Sequence meta = null;

    public SequenceCursor(CallContext context) throws CelestaException {
        super(context);
    }

    public long nextValue() throws CelestaException {
        return db().nextSequenceValue(conn(), meta());
    }

    @Override
    public void clear() throws CelestaException {

    }

    /**
     * Описание представления (метаинформация).
     *
     * @throws CelestaException
     *             в случае ошибки извлечения метаинформации (в норме не должна
     *             происходить).
     */
    @Override
    public final Sequence meta() throws CelestaException {
        if (meta == null)
            try {
                meta = callContext().getScore()
                        .getGrain(_grainName()).getElement(_tableName(), Sequence.class);
            } catch (ParseException e) {
                throw new CelestaException(e.getMessage());
            }
        return meta;
    }
}
