package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.SequenceElement;

/**
 * Sequence class for working with DB sequences. 
 */
public abstract class Sequence extends BasicDataAccessor {

    private SequenceElement meta = null;

    public Sequence(CallContext context) {
        super(context);
    }

    /**
     * Creates a sequence for a sequence grain element.
     *
     * @param sequence  Sequence element
     * @param callContext  Call context that is used for sequence creation
     * @return
     */
    public static Sequence create(SequenceElement sequence, CallContext callContext) {
        try {
            final String namespace = sequence.getGrain().getNamespace().getValue();
            String sequenceClassName =
                    sequence.getName().substring(0, 1).toUpperCase() + sequence.getName().substring(1) + "Sequence";
            sequenceClassName =
                    (namespace.isEmpty() ? "" : namespace + ".") + sequenceClassName;
            Class<?> sequenceClass =
                    Class.forName(sequenceClassName, true, Thread.currentThread().getContextClassLoader());

            return (Sequence) sequenceClass.getConstructor(CallContext.class).newInstance(callContext);
            
        } catch (ReflectiveOperationException ex) {
            throw new CelestaException("Sequence creation failed for grain element: " + sequence.getName(), ex);
        }
    }

    /**
     * Returns the <em>next value</em> of the sequence.
     *
     * @return
     */
    public final long nextValue() {
        return db().nextSequenceValue(conn(), meta());
    }

    @Override
    public void clear() {

    }

    /**
     * Description of representation (meta information).
     *
     * @return
     */
    @Override
    public final SequenceElement meta() {
        if (meta == null) {
            try {
                meta = callContext().getScore()
                        .getGrain(_grainName()).getElement(_objectName(), SequenceElement.class);
            } catch (ParseException e) {
                throw new CelestaException(e.getMessage());
            }
        }

        return meta;
    }

}
