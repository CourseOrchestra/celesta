package ru.curs.celesta.dbutils.adaptors.constants;

import java.util.regex.Pattern;

public final class CommonConstants {

    public static final Pattern HEXSTR = Pattern.compile("0x(([0-9A-Fa-f][0-9A-Fa-f])+)");
    public static final String ALTER_TABLE = "alter table ";


    private CommonConstants() {
        throw new AssertionError();
    }

}
