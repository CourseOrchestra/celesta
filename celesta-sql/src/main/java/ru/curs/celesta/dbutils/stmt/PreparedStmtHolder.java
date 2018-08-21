package ru.curs.celesta.dbutils.stmt;

import ru.curs.celesta.CelestaException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * A container for parameterized prepared statement.
 */
public abstract class PreparedStmtHolder {
    private PreparedStatement stmt;
    private final List<ParameterSetter> program = new LinkedList<>();

    public boolean isStmtValid()  {
        try {
            return !(stmt == null || stmt.isClosed());
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    /**
     * Returns prepared statement with refreshed parameters.
     *
     * @param rec
     *            Array of record fields' values.
     *
     * @
     *             something wrong.
     */
    public synchronized PreparedStatement getStatement(Object[] rec, int recversion)  {
        if (!isStmtValid()) {
            program.clear();
            stmt = initStatement(program);
            // everything should be initialized at this point
            if (!isStmtValid())
                throw new IllegalStateException();
        }
        int i = 1;
        for (ParameterSetter f : program) {
            f.execute(stmt, i++, rec, recversion);
        }
        //System.out.println(stmt.toString());
        return stmt;
    }

    public synchronized void close() {
        try {
            if (stmt != null)
                stmt.close();
        } catch (SQLException e) {
            e = null;
        }
        stmt = null;
        program.clear();
    }

    protected abstract PreparedStatement initStatement(List<ParameterSetter> program) ;

}
