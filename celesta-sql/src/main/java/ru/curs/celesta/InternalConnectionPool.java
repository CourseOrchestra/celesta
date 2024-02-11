package ru.curs.celesta;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.curs.celesta.dbutils.adaptors.DBAdaptor;

/**
 * Database connection pool.
 */
public final class InternalConnectionPool implements ConnectionPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(InternalConnectionPool.class);

    private final ConcurrentLinkedQueue<CelestaConnection> pool = new ConcurrentLinkedQueue<>();
    private final String jdbcConnectionUrl;
    private final String login;
    private final String password;
    private DBAdaptor dbAdaptor;
    private volatile boolean isClosed;

    private InternalConnectionPool(String jdbcConnectionUrl, String login, String password) {
        this.login = login;
        this.password = password;
        this.jdbcConnectionUrl = jdbcConnectionUrl;

        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    /**
     * Factory method for connection pool creation.
     *
     * @param configuration  connection pool configuration.
     * @return
     */
    public static InternalConnectionPool create(ConnectionPoolConfiguration configuration) {
        return new InternalConnectionPool(configuration.getJdbcConnectionUrl(),
                configuration.getLogin(), configuration.getPassword());
    }

    /**
     * Sets DB adaptor.
     *
     * @param dbAdaptor  DB adaptor.
     */
    @Override
    public void setDbAdaptor(DBAdaptor dbAdaptor) {
        this.dbAdaptor = dbAdaptor;
    }

    /**
     * Returns a connection from pool.
     *
     * @return connection from pool
     */
    @Override
    public Connection get() {

        if (isClosed) {
            throw new CelestaException("ConnectionPool is closed");
        }

        // First, we are trying to provide a connection from pool
        Connection c = pool.poll();
        while (c != null) {
            try {
                if (dbAdaptor.isValidConnection(c, 1)) {
                    return c;
                }
            } catch (CelestaException e) {
                // do something to make CheckStyle happy ))
                c = null;
            }
            c = pool.poll();
        }

        try {
            if (login.isEmpty()) {
                c = DriverManager.getConnection(jdbcConnectionUrl);
            } else {
                c = DriverManager.getConnection(jdbcConnectionUrl, login, password);
            }
            c.setAutoCommit(false);

            return new CelestaConnection(c) {
                @Override
                public void close() {
                    try {
                        if (isClosed) {
                            getConnection().close();
                        } else {
                            commit();
                            pool.add(this);
                        }
                    } catch (SQLException ex) {
                        LOGGER.error("Error on connection closing", ex);
                    }
                }
            };

        } catch (SQLException e) {
            throw new CelestaException("Could not connect to %s with error: %s",
                    PasswordHider.maskPassword(jdbcConnectionUrl), e.getMessage());
        }

    }

    /**
     * Closes up connection pool and all its connections, and makes it inaccessible.
     */
    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            CelestaConnection c;
            while ((c = pool.poll()) != null) {
                c.close();
            }
        }
    }


    /**
     * Returns the number of connections available in the pool.
     *
     * @return count of available connections
     */
    public Object poolSize() {
        return pool.size();
    }

    /**
     * If the poll is closed then this method will return true.
     *
     * @return <code>true</code> if the pool is closed;
     *            <code>false</code> otherwise.
     */
    @Override
    public boolean isClosed() {
        return isClosed;
    }
}

/**
 * Utility class for hiding password in JDBC connection string. Copied out from
 * FormsServer project.
 */
final class PasswordHider {
    // Password for Oracle is always between / and @ (and cannot contain @).
    private static final Pattern ORA_PATTERN = Pattern.compile("/[^@]+@");
    // In MS SQL is everything thought out and if password contains ;, it is exchanged to {;}
    private static final Pattern MSSQL_PATTERN = Pattern.compile("(password)=([^{;]|(\\{(;})|[^;]?))+(;|$)",
            Pattern.CASE_INSENSITIVE);
    // In POSTGRESQL JDBC-URL will work incorrectly if password contains &
    private static final Pattern POSTGRESQL_PATTERN = Pattern.compile("(password)=[^&]+(&|$)",
            Pattern.CASE_INSENSITIVE);
    // And this is for the case if JDBC driver is unknown to the science
    private static final Pattern GENERIC_PATTERN = Pattern.compile("(password)=.+$", Pattern.CASE_INSENSITIVE);

    private PasswordHider() {

    }

    /**
     * Method that masks password in the JDBC connection string.
     *
     * @param url  String containing JDBC connection URL
     */
    public static String maskPassword(String url) {
        if (url == null) {
            return null;
        }
        Matcher m;
        StringBuilder sb = new StringBuilder();
        if (url.toLowerCase().startsWith("jdbc:oracle")) {
            m = ORA_PATTERN.matcher(url);
            while (m.find()) {
                m.appendReplacement(sb, "/*****@");
            }
        } else if (url.toLowerCase().startsWith("jdbc:sqlserver")) {
            m = MSSQL_PATTERN.matcher(url);
            while (m.find()) {
                m.appendReplacement(sb, m.group(1) + "=*****" + m.group(5));
            }
        } else if (url.toLowerCase().startsWith("jdbc:postgresql")) {
            m = POSTGRESQL_PATTERN.matcher(url);
            while (m.find()) {
                m.appendReplacement(sb, m.group(1) + "=*****" + m.group(2));
            }
        } else {
            m = GENERIC_PATTERN.matcher(url);
            while (m.find()) {
                m.appendReplacement(sb, m.group(1) + "=*****");
            }
        }
        m.appendTail(sb);
        return sb.toString();
    }

}
