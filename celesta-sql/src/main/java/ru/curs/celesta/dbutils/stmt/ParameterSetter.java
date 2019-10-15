package ru.curs.celesta.dbutils.stmt;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.dbutils.filter.Range;
import ru.curs.celesta.dbutils.filter.SingleValue;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * An element of parameter setting program.
 */
public abstract class ParameterSetter {

    QueryBuildingHelper queryBuildingHelper;

    public abstract void execute(PreparedStatement stmt, int paramNum, Object[] rec, int recversion);

    ParameterSetter(QueryBuildingHelper queryBuildingHelper) {
        this.queryBuildingHelper = queryBuildingHelper;
    }

    protected void setParam(PreparedStatement stmt, int i, Object v)  {
        try {
            if (v == null) {
                stmt.setNull(i, java.sql.Types.NULL);
            } else if (v instanceof Integer) {
                stmt.setInt(i, (Integer) v);
            } else if (v instanceof Double) {
                stmt.setDouble(i, (Double) v);
            } else if (v instanceof BigDecimal) {
                stmt.setBigDecimal(i, (BigDecimal) v);
            } else if (v instanceof String) {
                stmt.setString(i, (String) v);
            } else if (v instanceof Boolean) {
                stmt.setBoolean(i, (Boolean) v);
            } else if (v instanceof Date) {
                Timestamp d = new Timestamp(((Date) v).getTime());
                stmt.setTimestamp(i, d);
            } else if (v instanceof ZonedDateTime) {
                ZonedDateTime zdt = (ZonedDateTime) v;
                zdt = this.queryBuildingHelper. prepareZonedDateTimeForParameterSetter(stmt.getConnection(), zdt);
                Timestamp t = Timestamp.valueOf(zdt.toLocalDateTime());
                Calendar cal = new GregorianCalendar();
                cal.setTimeZone(TimeZone.getTimeZone(zdt.getZone()));
                stmt.setTimestamp(i, t, cal);
            } else if (v instanceof BLOB) {
                stmt.setBinaryStream(i, ((BLOB) v).getInStream(), ((BLOB) v).size());
            }
        } catch (SQLException e) {
            throw new CelestaException(e.getMessage());
        }
    }

    public static ParameterSetter create(int i, QueryBuildingHelper queryBuildingHelper) {
        return new FieldParameterSetter(queryBuildingHelper, i);
    }

    public static ParameterSetter create(SingleValue v, QueryBuildingHelper queryBuildingHelper) {
        return new SingleValueParameterSetter(queryBuildingHelper, v);
    }

    public static ParameterSetter createForValueFrom(Range r, QueryBuildingHelper queryBuildingHelper) {
        return new ValueFromParameterSetter(queryBuildingHelper, r);
    }

    public static ParameterSetter createForValueTo(Range r, QueryBuildingHelper queryBuildingHelper) {
        return new ValueToParameterSetter(queryBuildingHelper, r);
    }

    public static ParameterSetter createForRecversion(QueryBuildingHelper queryBuildingHelper) {
        return new RecVersionParameterSetter(queryBuildingHelper);
    }

    public static ArbitraryParameterSetter createArbitrary(Object v, QueryBuildingHelper queryBuildingHelper) {
        return new ArbitraryParameterSetter(queryBuildingHelper, v);
    }

}
