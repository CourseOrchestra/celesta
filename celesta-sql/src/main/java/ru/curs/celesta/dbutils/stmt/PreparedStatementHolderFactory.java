package ru.curs.celesta.dbutils.stmt;

import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.query.FromClause;
import ru.curs.celesta.dbutils.term.CsqlWhereTermsMaker;
import ru.curs.celesta.dbutils.term.FromTerm;
import ru.curs.celesta.dbutils.term.WhereTerm;
import ru.curs.celesta.score.BasicTable;
import ru.curs.celesta.score.TableElement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class PreparedStatementHolderFactory {

    public static MaskedStatementHolder createInsertHolder(BasicTable meta, DBAdaptor dbAdaptor, Connection conn) {
        return new MaskedStatementHolder() {

            @Override
            protected int[] getNullsMaskIndices()  {
                // we monitor all columns for nulls
                int[] result = new int[meta.getColumns().size()];
                for (int i = 0; i < result.length; i++) {
                    result[i] = i;
                }
                return result;
            }

            @Override
            protected PreparedStatement initStatement(List<ParameterSetter> program)  {
                return dbAdaptor.getInsertRecordStatement(conn, meta, getNullsMask(), program);
            }

        };
    }

    public static PreparedStmtHolder createGetHolder(TableElement meta, DBAdaptor dbAdaptor, Connection conn) {
        return new PreparedStmtHolder() {
            @Override
            protected PreparedStatement initStatement(List<ParameterSetter> program)  {
                WhereTerm where = CsqlWhereTermsMaker.getPKWhereTermForGet(meta);
                where.programParams(program, dbAdaptor);
                return dbAdaptor.getOneRecordStatement(conn, meta, where.getWhere(), Collections.emptySet());
            }
        };
    }

    public static PreparedStmtHolder createUpdateHolder(BasicTable meta, DBAdaptor dbAdaptor, Connection conn,
                                                        Supplier<boolean[]> updateMaskSupplier,
                                                        Supplier<boolean[]> nullUpdateMaskSupplier) {
        return new PreparedStmtHolder() {
            @Override
            protected PreparedStatement initStatement(List<ParameterSetter> program)  {
                WhereTerm where = CsqlWhereTermsMaker.getPKWhereTerm(meta);
                PreparedStatement result = dbAdaptor.getUpdateRecordStatement(
                        conn, meta, updateMaskSupplier.get(), nullUpdateMaskSupplier.get(), program, where.getWhere()
                );
                where.programParams(program, dbAdaptor);
                return result;
            }
        };
    }

    //TODO:MUST BE REFACTORED!!!
    public static PreparedStmtHolder createFindSetHolder(
            DBAdaptor dbAdaptor, Connection conn, Supplier<FromClause> fromClauseSupplier,
            Supplier<FromTerm> fromTermSupplier, Supplier<WhereTerm> whereTermSupplier,
            Supplier<String> orderBySupplier, Supplier<Long> offsetSupplier,
            Supplier<Long> rowCountSupplier, Supplier<Set<String>> fieldsForStatementSupplier) {
        return new PreparedStmtHolder() {
            @Override
            protected PreparedStatement initStatement(List<ParameterSetter> program)
                     {
                FromClause from = fromClauseSupplier.get();
                FromTerm fromTerm = fromTermSupplier.get();

                if (fromTerm == null) {
                    fromTerm = new FromTerm(from.getParameters());
                }

                WhereTerm where = whereTermSupplier.get();
                fromTerm.programParams(program, dbAdaptor);
                where.programParams(program, dbAdaptor);
                return dbAdaptor.getRecordSetStatement(conn, from, where.getWhere(), orderBySupplier.get(),
                        offsetSupplier.get(), rowCountSupplier.get(), fieldsForStatementSupplier.get());
            }
        };
    }

}
