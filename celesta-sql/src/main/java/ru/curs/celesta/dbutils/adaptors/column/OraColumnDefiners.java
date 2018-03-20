package ru.curs.celesta.dbutils.adaptors.column;

import static ru.curs.celesta.dbutils.adaptors.function.OraFunctions.*;

import ru.curs.celesta.score.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;


class OraIntegerColumnDefiner extends OraColumnDefiner {
    @Override
    public String dbFieldType() {
        return "number";
    }

    @Override
    public String getInternalDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType());
    }

    @Override
    public String getDefaultDefinition(Column c) {
        IntegerColumn ic = (IntegerColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
            defaultStr = DEFAULT + ic.getDefaultValue();
        }
        return defaultStr;
    }
}

class OraFloatingColumnDefiner extends OraColumnDefiner {
    @Override
    public String dbFieldType() {
        return "real";
    }

    @Override
    public String getInternalDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType());
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

class OraStringColumnDefiner extends OraColumnDefiner {
    @Override
    public String dbFieldType() {
        return "nvarchar2";
    }

    // Пустая DEFAULT-строка не сочетается с NOT NULL в Oracle.
    @Override
    public String nullable(Column c) {
        StringColumn ic = (StringColumn) c;
        return ("".equals(ic.getDefaultValue())) ? "null" : super.nullable(c);
    }

    @Override
    public String getInternalDefinition(Column c) {
        StringColumn ic = (StringColumn) c;
        String fieldType = ic.isMax() ? "nclob" : String.format("%s(%s)", dbFieldType(), ic.getLength());
        return join(c.getQuotedName(), fieldType);
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

class OraBinaryColumnDefiner extends OraColumnDefiner {
    @Override
    public String dbFieldType() {
        return "blob";
    }

    @Override
    public String getInternalDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType());
    }

    @Override
    public String getDefaultDefinition(Column c) {
        BinaryColumn ic = (BinaryColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
            // Отрезаем 0x и закавычиваем
            defaultStr = String.format(DEFAULT + "'%s'", ic.getDefaultValue().substring(2));
        }
        return defaultStr;
    }
}

class OraDateTimeColumnDefiner extends OraColumnDefiner {
    @Override
    public String dbFieldType() {
        return "timestamp";
    }

    @Override
    public String getInternalDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType());
    }

    @Override
    public String getDefaultDefinition(Column c) {
        DateTimeColumn ic = (DateTimeColumn) c;
        String defaultStr = "";
        if (ic.isGetdate()) {
            defaultStr = DEFAULT + "sysdate";
        } else if (ic.getDefaultValue() != null) {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            defaultStr = String.format(DEFAULT + "date '%s'", df.format(ic.getDefaultValue()));
        }
        return defaultStr;
    }
}

class OraBooleanColumnDefiner extends OraColumnDefiner {
    @Override
    public String dbFieldType() {
        return "number";
    }

    @Override
    public String getInternalDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType());
    }

    @Override
    public String getDefaultDefinition(Column c) {
        BooleanColumn ic = (BooleanColumn) c;
        String defaultStr = "";
        if (ic.getDefaultValue() != null) {
            defaultStr = DEFAULT + (ic.getDefaultValue() ? "1" : "0");
        }
        return defaultStr;
    }

    @Override
    public String getFullDefinition(Column c) {
        String check = String.format("constraint %s check (%s in (0, 1))", getBooleanCheckName(c),
                c.getQuotedName());
        return join(getInternalDefinition(c), getDefaultDefinition(c), nullable(c), check);
    }
}