package ru.curs.celesta.syscursors;

import ru.curs.celesta.ICallContext;

import java.util.Date;

public interface ISchemaElementCursor {

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

    String getGrainId();

    void setGrainId(String grainId);

    String getId();

    void setId(String id);

    String getType();

    void setType(String type);

    Date getLastModified();

    void setLastModified(Date lastModified);

    Integer getState();

    void setState(Integer state);

    void setMessage(String message);


    void update();

    void get(Object... values);

    boolean tryGet(Object... values);

    void init();

    boolean nextInSet();

    void insert();

    ICallContext callContext();
}
