package ru.curs.celesta.dbutils.adaptors.constants;

import java.util.regex.Pattern;

/**
 * Holder class for common constants.
 */
public final class CommonConstants {

    /**
     * Regular expression to check against hexadecimal strings like: 0xDEADBEEF.
     */
    public static final Pattern HEXSTR = Pattern.compile("0x(([0-9A-Fa-f][0-9A-Fa-f])+)");

    /**
     * "alter table " constant.
     */
    public static final String ALTER_TABLE = "alter table ";


    private CommonConstants() {
    }

}
