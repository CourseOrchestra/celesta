package ru.curs.celesta.dbutils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

/**
 * Class for working with BLOB fields.
 */
public final class BLOB implements Cloneable {
    private DataPage data;
    private boolean isModified;
    private int size;

    /**
     * Empty (NULL) BLOB.
     */
    public BLOB() {
    }

    /**
     * BLOB based on data from stream.
     *
     * @param source  Stream from which the data is being read to BLOB
     * @throws IOException  on reading error
     */
    BLOB(final InputStream source) throws IOException {
        InputStream counter = new InputStream() {
            @Override
            public int read() throws IOException {
                int result = source.read();
                if (result >= 0) {
                    size++;
                }
                return result;
            }
        };
        int buf = counter.read();
        data = buf < 0 ? new DataPage(0) : new DataPage(buf, counter);
    }

    /**
     * Returns a BLOB clone pointing at the same data page.
     *
     * @return
     */
    @Override
    public BLOB clone() {
        BLOB result = new BLOB();
        result.data = data;
        result.size = size;
        return result;
    }

    /**
     * Whether BLOB data has been changed.
     *
     * @return
     */
    public boolean isModified() {
        return isModified;
    }

    /**
     * Returns a stream for data reading.
     *
     * @return
     */
    public InputStream getInStream() {
        return data == null ? null : data.getInStream();
    }

    /**
     * Returns a stream for data writing, at the same time dropping off current data from BLOB.
     *
     * @return
     */
    public OutputStream getOutStream() {
        isModified = true;
        data = new DataPage();
        size = 0;
        return new OutputStream() {
            private DataPage tail = data;

            @Override
            public void write(int b) {
                tail = tail.write(b);
                size++;
            }
        };
    }

    void saveToJDBCBlob(Blob b) throws SQLException {
        DataPage currPage = data;
        int i = 1;
        while (currPage != null && currPage.pos == currPage.data.length) {
            i += b.setBytes(i, currPage.data);
            currPage = currPage.nextPage;
        }
        if (currPage != null) {
            b.setBytes(i, currPage.data, 0, currPage.pos);
        }
    }

    /**
     * Whether current field accepts a value of {@code NULL}.
     *
     * @return
     */
    public boolean isNull() {
        return data == null;
    }

    /**
     * Resets BLOB to {@code NULL}.
     */
    public void setNull() {
        isModified = isModified || (data != null);
        size = 0;
        data = null;
    }

    /**
     * Returns data size.
     *
     * @return
     */
    public int size() {
        return size;
    }

    /**
     * Data of BLOB field.
     */
    private static final class DataPage {
        private static final int DEFAULT_PAGE_SIZE = 0xFFFF;
        private static final int BYTE_MASK = 0xFF;

        private final byte[] data;
        private DataPage nextPage;
        private int pos;

        DataPage() {
            this(DEFAULT_PAGE_SIZE);
        }

        private DataPage(int size) {
            data = new byte[size];
        }

        private DataPage(int firstByte, InputStream source) throws IOException {
            this();
            int buf = firstByte;
            while (pos < data.length && buf >= 0) {
                data[pos++] = (byte) buf;
                buf = source.read();
            }
            nextPage = buf < 0 ? null : new DataPage(buf, source);
        }

        DataPage write(int b) {
            if (pos < data.length) {
                data[pos++] = (byte) (b & BYTE_MASK);
                return this;
            } else {
                DataPage result = new DataPage();
                nextPage = result;
                return result.write(b);
            }
        }

        InputStream getInStream() {
            return new InputStream() {
                private int i = 0;
                private DataPage currentPage = DataPage.this;

                @Override
                public int read() {
                    if (i < currentPage.pos) {
                        return (int) currentPage.data[i++] & BYTE_MASK;
                    } else if (currentPage.nextPage != null) {
                        i = 0;
                        currentPage = currentPage.nextPage;
                        return read();
                    } else {
                        return -1;
                    }
                }
            };
        }
    }
}
