package seq;

import javax.annotation.Generated;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.dbutils.CelestaGenerated;
import ru.curs.celesta.dbutils.Sequence;

@Generated(
        value = "ru.curs.celesta.plugin.maven.CursorGenerator",
        date = "2020-02-25T10:50:49"
)
@CelestaGenerated
public class SeqSequence extends Sequence {

    private static final String GRAIN_NAME = "test";
    private static final String OBJECT_NAME = "seq";

    public SeqSequence(CallContext context) {
        super(context);
    }

    @Override
    protected String _grainName() {
        return GRAIN_NAME;
    }

    @Override
    protected String _objectName() {
        return OBJECT_NAME;
    }

}
