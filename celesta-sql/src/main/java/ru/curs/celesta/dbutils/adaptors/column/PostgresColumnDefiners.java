package ru.curs.celesta.dbutils.adaptors.column;

import static ru.curs.celesta.dbutils.adaptors.constants.CommonConstants.*;
import static ru.curs.celesta.dbutils.adaptors.constants.OpenSourceConstants.*;

import ru.curs.celesta.score.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;

class PostgresIntegerColumnDefiner extends ColumnDefiner {
    @Override
    public String dbFieldType() {
        return "int4";
    }

    @Override
    public String getMainDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType(), nullable(c));
    }

    @Override
    public String getDefaultDefinition(Column c) {
        IntegerColumn ic = (IntegerColumn) c;
        String defaultStr = "";
        SequenceElement s = ic.getSequence();
        if (s != null) {
            defaultStr = DEFAULT + "nextval('" + s.getGrain().getQuotedName() + "." + s.getQuotedName() + "')";
        } else if (ic.getDefaultValue() != null) {
            defaultStr = DEFAULT + ic.getDefaultValue();
        }
        return defaultStr;
    }
}

class PostgresFloatingColumnDefiner extends ColumnDefiner {
    @Override
    public String dbFieldType() {
        return "float8"; // double precision";
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
            defaultStr = DEFAULT + ic.getDefaultValue();
        }
        return defaultStr;
    }
}

class PostgresDecimalColumnDefiner extends ColumnDefiner {
    @Override
    public String dbFieldType() {
        return "numeric";
    }

    @Override
    public String getMainDefinition(Column c) {
        DecimalColumn dc = (DecimalColumn)c;
        String fieldType = String.format("%s(%s,%s)", dbFieldType(), dc.getPrecision(), dc.getScale());
        return join(c.getQuotedName(), fieldType, nullable(c));
    }

    @Override
    public String getDefaultDefinition(Column c) {
        DecimalColumn dc = (DecimalColumn) c;
        String defaultStr = "";
        if (dc.getDefaultValue() != null) {
            defaultStr = DEFAULT + dc.getDefaultValue();
        }
        return defaultStr;
    }
}

class PostgresBooleanColumnDefiner extends ColumnDefiner {
    @Override
    public String dbFieldType() {
        return "bool";
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
            defaultStr = DEFAULT + "'" + ic.getDefaultValue() + "'";
        }
        return defaultStr;
    }
}

class PostgresStringColumnDefiner extends ColumnDefiner {
    @Override
    public String dbFieldType() {
        return "varchar";
    }

    @Override
    public String getMainDefinition(Column c) {
        StringColumn ic = (StringColumn) c;
        String fieldType = ic.isMax() ? "text" : String.format("%s(%s)", dbFieldType(), ic.getLength());
        return join(c.getQuotedName(), fieldType, nullable(c));
    }

    @Override
    public String getDefaultDefinition(Column c) {
        StringColumn ic = (StringColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
            defaultStr = DEFAULT + StringColumn.quoteString(ic.getDefaultValue());
        }
        return defaultStr;
    }
}

class PostgresBinaryColumnDefiner extends ColumnDefiner {
    @Override
    public String dbFieldType() {
        return "bytea";
    }

    @Override
    public String getMainDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType(), nullable(c));
    }

    @Override
    public String getDefaultDefinition(Column c) {
        BinaryColumn bc = (BinaryColumn) c;
        String defaultStr = "";
        if (bc.getDefaultValue() != null) {
            Matcher m = HEXSTR.matcher(bc.getDefaultValue());
            m.matches();
            defaultStr = DEFAULT + String.format("E'\\\\x%s'", m.group(1));
        }
        return defaultStr;
    }
}

class PostgresDateTimeColumnDefiner extends ColumnDefiner {
    @Override
    public String dbFieldType() {
        return "timestamp";
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
            defaultStr = DEFAULT + NOW;
        } else if (ic.getDefaultValue() != null) {
            DateFormat df = new SimpleDateFormat("yyyyMMdd");
            defaultStr = String.format(DEFAULT + " '%s'", df.format(ic.getDefaultValue()));
        }
        return defaultStr;
    }
}

class PostgresZonedDateTimeColumnDefiner extends ColumnDefiner {
    @Override
    public String dbFieldType() {
        return "timestamptz";
    }

    @Override
    public String getMainDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType(), nullable(c));
    }

    @Override
    public String getDefaultDefinition(Column c) {
        return "";
    }
}
