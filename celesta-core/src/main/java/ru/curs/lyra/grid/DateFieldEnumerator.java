package ru.curs.lyra.grid;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Date;

public class DateFieldEnumerator extends KeyEnumerator {
    public static final long MIN_TIMESTAMP = Instant.parse("1900-01-01T00:00:00.00Z").toEpochMilli();
    public static final long MAX_TIMESTAMP = Instant.parse("2100-01-01T00:00:00.00Z").toEpochMilli();

    private static final BigInteger MIN = BigInteger.valueOf(MIN_TIMESTAMP);
    private static final BigInteger MAX = BigInteger.valueOf(MAX_TIMESTAMP);
    private static final BigInteger CARD = MAX.subtract(MIN).add(BigInteger.ONE);

    private Date value;

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
