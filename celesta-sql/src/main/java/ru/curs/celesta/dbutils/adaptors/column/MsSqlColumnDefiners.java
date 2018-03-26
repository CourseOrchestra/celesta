package ru.curs.celesta.dbutils.adaptors.column;

import static ru.curs.celesta.dbutils.adaptors.constants.MsSqlConstants.*;

import ru.curs.celesta.score.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

class MsSqlIntegerColumnDefiner extends MsSqlColumnDefiner {
    @Override
    public String dbFieldType() {
        return "int";
    }

    @Override
    public String getMainDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType(), nullable(c));
    }

    @Override
    public String getDefaultDefinition(Column c) {
        IntegerColumn ic = (IntegerColumn) c;
        SequenceElement s = ic.getSequence();
        if (s != null) {
            return msSQLDefault(c) + "next value for " + s.getGrain().getQuotedName() + "." + s.getQuotedName();
        } else if (ic.getDefaultValue() != null)
            return msSQLDefault(c) + ic.getDefaultValue();
        return "";
    }

    @Override
    public String getLightDefaultDefinition(Column c) {
        IntegerColumn ic = (IntegerColumn) c;
        if (ic.getDefaultValue() != null)
            return DEFAULT + ic.getDefaultValue();
        return "";
    }
}

class MsSqlFloatingColumnDefiner extends MsSqlColumnDefiner {
    @Override
    public String dbFieldType() {
        return "float(" + DOUBLE_PRECISION + ")";
    }

    @Override
    public String getMainDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType(), nullable(c));
    }

    @Override
    public String getDefaultDefinition(Column c) {
        FloatingColumn ic = (FloatingColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
            defaultStr = msSQLDefault(c) + ic.getDefaultValue();
        }
        return defaultStr;
    }

    @Override
    public String getLightDefaultDefinition(Column c) {
        FloatingColumn ic = (FloatingColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
            defaultStr = DEFAULT + ic.getDefaultValue();
        }
        return defaultStr;
    }
}

class MsSqlDecimalColumnDefiner extends MsSqlColumnDefiner {
    @Override
    public String dbFieldType() {
        return "decimal";
    }

    @Override
    public String getMainDefinition(Column c) {
        DecimalColumn dc = (DecimalColumn)c;
        String fieldType = String.format("%s(%s,%s)", dbFieldType(), dc.getPrecision(), dc.getScale());
        return join(c.getQuotedName(), fieldType, nullable(c));
    }

    @Override
    public String getDefaultDefinition(Column c) {
        DecimalColumn dc = (DecimalColumn)c;
        String defaultStr = "";
        if (dc.getDefaultValue() != null) {
            defaultStr = msSQLDefault(c) + dc.getDefaultValue();
        }
        return defaultStr;
    }

    @Override
    public String getLightDefaultDefinition(Column c) {
        DecimalColumn dc = (DecimalColumn) c;
        String defaultStr = "";
        if (dc.getDefaultValue() != null) {
            defaultStr = DEFAULT + dc.getDefaultValue();
        }
        return defaultStr;
    }
}

class MsSqlStringColumnDefiner extends MsSqlColumnDefiner {
    @Override
    public String dbFieldType() {
        return "nvarchar";
    }

    @Override
    public String getMainDefinition(Column c) {
        StringColumn ic = (StringColumn) c;
        String fieldType = String.format("%s(%s)", dbFieldType(), ic.isMax() ? "max" : ic.getLength());
        return join(c.getQuotedName(), fieldType, nullable(c));
    }

    @Override
    public String getDefaultDefinition(Column c) {
        StringColumn ic = (StringColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
            defaultStr = msSQLDefault(c) + StringColumn.quoteString(ic.getDefaultValue());
        }
        return defaultStr;
    }

    @Override
    public String getLightDefaultDefinition(Column c) {
        StringColumn ic = (StringColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
            defaultStr = DEFAULT + StringColumn.quoteString(ic.getDefaultValue());
        }
        return defaultStr;
    }
}

class MsSqlBinaryColumnDefiner extends MsSqlColumnDefiner {
    @Override
    public String dbFieldType() {
        return "varbinary(max)";
    }

    @Override
    public String getMainDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType(), nullable(c));
    }

    @Override
    public String getDefaultDefinition(Column c) {
        BinaryColumn ic = (BinaryColumn) c;

        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
            defaultStr = msSQLDefault(c) + ic.getDefaultValue();
        }
        return defaultStr;

    }

    @Override
    public String getLightDefaultDefinition(Column c) {
        BinaryColumn ic = (BinaryColumn) c;

        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
            defaultStr = DEFAULT + ic.getDefaultValue();
        }
        return defaultStr;
    }
}

class MsSqlDateTimeColumnDefiner extends MsSqlColumnDefiner {
    @Override
    public String dbFieldType() {
        return "datetime";
    }

    @Override
    public String getMainDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType(), nullable(c));
    }

    @Override
    public String getDefaultDefinition(Column c) {
        DateTimeColumn ic = (DateTimeColumn) c;
        String defaultStr = "";
        if (ic.isGetdate()) {
            defaultStr = msSQLDefault(c) + "getdate()";
        } else if (ic.getDefaultValue() != null) {
            DateFormat df = new SimpleDateFormat("yyyyMMdd");
            defaultStr = String.format(msSQLDefault(c) + " '%s'", df.format(ic.getDefaultValue()));
        }
        return defaultStr;
    }

    @Override
    public String getLightDefaultDefinition(Column c) {
        DateTimeColumn ic = (DateTimeColumn) c;
        String defaultStr = "";
        if (ic.isGetdate()) {
            defaultStr = DEFAULT + "getdate()";
        } else if (ic.getDefaultValue() != null) {
            DateFormat df = new SimpleDateFormat("yyyyMMdd");
            defaultStr = String.format(DEFAULT + " '%s'", df.format(ic.getDefaultValue()));
        }
        return defaultStr;
    }
}

class MsSqlBooleanColumnDefiner extends MsSqlColumnDefiner {
    @Override
    public String dbFieldType() {
        return "bit";
    }

    @Override
    public String getMainDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType(), nullable(c));
    }

    @Override
    public String getDefaultDefinition(Column c) {
        BooleanColumn ic = (BooleanColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
            defaultStr = msSQLDefault(c) + "'" + ic.getDefaultValue() + "'";
        }
        return defaultStr;
    }

    @Override
    public String getLightDefaultDefinition(Column c) {
        BooleanColumn ic = (BooleanColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
            defaultStr = DEFAULT + "'" + ic.getDefaultValue() + "'";
        }
        return defaultStr;
    }
}
