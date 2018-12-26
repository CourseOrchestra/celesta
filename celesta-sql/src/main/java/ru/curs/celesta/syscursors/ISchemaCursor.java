package ru.curs.celesta.syscursors;

import ru.curs.celesta.ICallContext;

import java.util.Date;

public interface ISchemaCursor {

    /**
     * Status "ready".
     */
    int READY = 0;
    /**
     * Status "in process of upgrading".
     */
    int UPGRADING = 1;
    /**
     * Status "error".
     */
    int ERROR = 2;
    /**
     * Status "do update!".
     */
    int RECOVER = 3;

    /**
     * Status "do not update!".
     */
    int LOCK = 4;


    ICallContext callContext();

    /**
     * Sets state to the schema cursor.
     *
     * @param state  state
     */
    void setState(Integer state);

    /**
     * Sets checksum to the schema cursor.
     *
     * @param checksum  checksum
     */
    void setChecksum(String checksum);

    /**
     * Sets length to the schema cursor.
     *
     * @param length  length
     */
    void setLength(Integer length);

    /**
     * Returns 'last modified' date of the schema cursor.
     *
     * @return
     */
    Date getLastmodified();

    /**
     * Sets 'last modified' date to the schema cursor.
     *
     * @param lastmodified  'last modified' date
     */
    void setLastmodified(Date lastmodified);

    /**
     * Sets message to the schema cursor.
     *
     * @param message  message
     */
    void setMessage(String message);

    /**
     * Sets version to the schema cursor.
     *
     * @param version  version
     */
    void setVersion(String version);

    void update();

    void get(Object... values);

    void init();

    /**
     * Sets ID to the schema cursor.
     *
     * @param id  ID
     */
    void setId(String id);

    /**
     * Returns ID of the schema cursor.
     *
     * @return
     */
    String getId();

    /**
     * Returns state of the schema cursor.
     *
     * @return
     */
    Integer getState();

    /**
     * Returns version of the schema cursor.
     *
     * @return
     */
    String getVersion();

    /**
     * Returns length of the schema cursor.
     *
     * @return
     */
    Integer getLength();

    /**
     * Returns checksum of the schema cursor.
     *
     * @return
     */
    String getChecksum();

    boolean nextInSet();

    void insert();
}
