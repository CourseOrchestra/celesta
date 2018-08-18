package ru.curs.lyra.grid;

import java.math.BigInteger;
import java.util.Date;

public class DateFieldEnumerator extends KeyEnumerator {

    private Date value;

    public static BigInteger MIN = BigInteger.valueOf(-2208988800000L); // 1900-01-01
    public static BigInteger MAX = BigInteger.valueOf(4102444800000L); // 2100-01-01

    private static final BigInteger CARD = MAX.subtract(MIN).add(BigInteger.ONE);

    @Override
    public BigInteger cardinality() {
        return CARD;
    }

    @Override
    public BigInteger getOrderValue() {
        return BigInteger.valueOf(value.getTime()).subtract(MIN);
    }

    @Override
    public void setOrderValue(BigInteger value) {
        this.value = new Date(value.add(MIN).longValueExact());
    }

    @Override
    public void setValue(Object value) {
        if (value instanceof Date) {
            Date d = (Date) value;
            long t = d.getTime();
            if (t < MIN.longValue()) {
                throw new IllegalArgumentException(String.format(
                        "LyraGrid cannot represent a date earlier than 1900-01-01. Found %s",
                        d.toString()
                ));
            }
            if (t > MAX.longValue()) {
                throw new IllegalArgumentException(String.format(
                        "LyraGrid cannot represent a date later than 2100-01-01. Found %s",
                        d.toString()
                ));
            }
            this.value = (Date) value;
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public Date getValue() {
        return value;
    }

}
