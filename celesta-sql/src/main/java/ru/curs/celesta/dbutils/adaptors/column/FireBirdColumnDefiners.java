package ru.curs.celesta.dbutils.adaptors.column;

import ru.curs.celesta.score.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import static ru.curs.celesta.dbutils.adaptors.constants.FireBirdConstants.CURRENT_TIMESTAMP;


class FireBirdIntegerColumnDefiner extends FireBirdColumnDefiner {
    @Override
    public String dbFieldType() {
        return "integer";
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


class FireBirdFloatingColumnDefiner extends FireBirdColumnDefiner {
    @Override
    public String dbFieldType() {
        return "FLOAT";
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

class FireBirdDecimalColumnDefiner extends FireBirdColumnDefiner {
    @Override
    public String dbFieldType() {
        return "DECIMAL";
    }

    @Override
    public String getInternalDefinition(Column c) {
        DecimalColumn dc = (DecimalColumn) c;
        String fieldType = String.format("%s(%s,%s)", dbFieldType(), dc.getPrecision(), dc.getScale());
        return join(c.getQuotedName(), fieldType);
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

class FireBirdBooleanColumnDefiner extends FireBirdColumnDefiner {
    @Override
    public String getInternalDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType());
    }

    @Override
    public String dbFieldType() {
        return "SMALLINT";
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
        String check = String.format("check (%s in (0, 1))", c.getQuotedName());
        return join(getInternalDefinition(c), getDefaultDefinition(c), nullable(c), check);
    }
}

class FireBirdStringColumnDefiner extends FireBirdColumnDefiner {
    @Override
    public String dbFieldType() {
        return "varchar";
    }

    @Override
    public String getInternalDefinition(Column c) {
        StringColumn ic = (StringColumn) c;
        String fieldType = ic.isMax() ? "blob sub_type text" : String.format("%s(%s)", dbFieldType(), ic.getLength());
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

class FireBirdBinaryColumnDefiner extends FireBirdColumnDefiner {
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

class FireBirdDateTimeColumnDefiner extends FireBirdColumnDefiner {
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
            defaultStr = DEFAULT + CURRENT_TIMESTAMP;
        } else if (ic.getDefaultValue() != null) {
            DateFormat df = new SimpleDateFormat("dd.MM.yyyy");
            defaultStr = String.format(DEFAULT + " '%s'", df.format(ic.getDefaultValue()));
        }
        return defaultStr;
    }
}


class FireBirdZonedDateTimeColumnDefiner extends FireBirdColumnDefiner {
    @Override
    public String dbFieldType() {
        return "timestamp with time zone";
    }

    @Override
    public String getInternalDefinition(Column c) {
        return join(c.getQuotedName(), dbFieldType());
    }

    @Override
    public String getDefaultDefinition(Column c) {
        return "";
    }
}
