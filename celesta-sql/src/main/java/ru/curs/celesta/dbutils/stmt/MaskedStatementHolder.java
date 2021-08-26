package ru.curs.celesta.dbutils.stmt;

import java.sql.PreparedStatement;

/**
 * Holder for a statement which depends on nulls mask.
 */
public abstract class MaskedStatementHolder extends PreparedStmtHolder {
    private int[] nullsMaskIndices;
    private boolean[] nullsMask;

    @Override
    public synchronized PreparedStatement getStatement(Object[] rec, int recversion)  {
        reusable: if (isStmtValid()) {
            for (int i = 0; i < nullsMask.length; i++) {
                if (rec[nullsMaskIndices[i]] == null != nullsMask[i]) {
                    close();
                    break reusable;
                }
            }
            return super.getStatement(rec, recversion);
        }
        nullsMaskIndices = getNullsMaskIndices();
        nullsMask = new boolean[nullsMaskIndices.length];
        for (int i = 0; i < nullsMask.length; i++) {
            nullsMask[i] = rec[nullsMaskIndices[i]] == null;
        }

        return super.getStatement(rec, recversion);
    }

    @Override
    public synchronized void close() {
        super.close();
        nullsMaskIndices = null;
    }

    public boolean[] getNullsMask() {
        return nullsMask;
    }

    protected abstract int[] getNullsMaskIndices();

}
