package ru.curs.celesta.syscursors;

import ru.curs.celesta.ICallContext;

import java.util.Date;

public interface ISchemaCursor {

    /**
     * Статус "готов".
     */
    int READY = 0;
    /**
     * Статус "в процессе обновления".
     */
    int UPGRADING = 1;
    /**
     * Статус "ошибка".
     */
    int ERROR = 2;
    /**
     * Статус "обновить!".
     */
    int RECOVER = 3;

    /**
     * Статус "не обновлять!".
     */
    int LOCK = 4;


    ICallContext callContext();

    void setState(Integer state);

    void setChecksum(String checksum);

    void setLength(Integer length);

    Date getLastmodified();

    void setLastmodified(Date lastmodified);

    void setMessage(String message);

    void setVersion(String version);

    void update();

    void get(Object... values);

    void init();

    void setId(String id);

    String getId();

    Integer getState();

    String getVersion();

    Integer getLength();

    String getChecksum();

    boolean nextInSet();

    void insert();
}
