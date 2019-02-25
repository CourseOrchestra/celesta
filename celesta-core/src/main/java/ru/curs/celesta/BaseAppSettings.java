package ru.curs.celesta;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Base class for application settings.
 */
public abstract class BaseAppSettings {

    /**
     * In memory H2 DB connection url.
     */
    public static final String H2_IN_MEMORY_URL = "jdbc:h2:mem:celesta;DB_CLOSE_DELAY=-1";

    private final Properties properties;

    private final String scorePath;
    private final DBType dbType;
    private final String databaseConnection;
    private final boolean h2ReferentialIntegrity;
    private final int h2Port;
    private final String login;
    private final String password;
    private final Logger logger;
    private final boolean skipDBUpdate;
    private final boolean forceDBInitialize;
    private final boolean logLogins;

    {
        logger = Logger.getLogger("ru.curs.flute");
        logger.setLevel(Level.INFO);
    }

    public BaseAppSettings(final Properties properties) {
        this.properties = properties;

        StringBuffer sb = new StringBuffer();

        // Read the settings and check them as thoroughly as possible at this
        // point.

        scorePath = properties.getProperty("score.path", "").trim();
        if (!scorePath.isEmpty()) {
            checkEntries(scorePath, "score.path", sb);
        }

        h2ReferentialIntegrity = Boolean.parseBoolean(properties.getProperty("h2.referential.integrity", "false"));
        final boolean h2InMemory = Boolean.parseBoolean(properties.getProperty("h2.in-memory", "false"));
        int h2PortTmp = 0;
        try {
            h2PortTmp = Integer.parseInt(properties.getProperty("h2.port", "0"));
        } catch (NumberFormatException e) {
            sb.append("h2.port should contain a port number.\n");
        }
        h2Port = h2PortTmp;
        //Если настройка h2.in-memory установлена в true - игнорируем настройку строки jdbc подключения и вводим свою
        if (h2InMemory) {
            if (h2Port > 0) {
                databaseConnection = String.format(
                        "jdbc:h2:tcp://localhost:%d/mem:celesta", h2Port);
            } else {
                databaseConnection = H2_IN_MEMORY_URL;
            }
            login = "";
            password = "";
        } else {
            String url = properties.getProperty("database.connection", "").trim();
            if ("".equals(url)) {
                url = properties.getProperty("rdbms.connection.url", "").trim();
            }
            databaseConnection = url;
            login = properties.getProperty("rdbms.connection.username", "").trim();
            password = properties.getProperty("rdbms.connection.password", "").trim();

            if ("".equals(databaseConnection)) {
                sb.append("No JDBC URL given (rdbms.connection.url).\n");
            }
        }

        dbType = DBType.resolveByJdbcUrl(databaseConnection);
        if (dbType == DBType.UNKNOWN) {
            sb.append("Cannot recognize RDBMS type or unsupported database.");
        }

        String lf = properties.getProperty("log.file");
        if (lf != null) {
            try {
                FileHandler fh = new FileHandler(lf, true);
                fh.setFormatter(new SimpleFormatter());
                logger.addHandler(fh);
            } catch (IOException e) {
                sb.append("Could not access or create log file ").append(lf).append('\n');
            }
        }

        skipDBUpdate = Boolean.parseBoolean(properties.getProperty("skip.dbupdate", "").trim());
        forceDBInitialize = Boolean.parseBoolean(properties.getProperty("force.dbinitialize", "").trim());
        logLogins = Boolean.parseBoolean(properties.getProperty("log.logins", "").trim());

        if (sb.length() > 0) {
            throw new CelestaException(sb.toString());
        }

    }

    protected static void checkEntries(String path, String propertyName, StringBuffer sb) {
        if (!path.isEmpty()) {
            for (String pathEntry : path.split(File.pathSeparator)) {
                File pathFile = new File(pathEntry);
                if (!(pathFile.isDirectory() && pathFile.canRead())) {
                    sb.append(String.format("Invalid %s entry: %s%n", propertyName, pathEntry));
                }
            }
        }
    }

    /**
     * Returns database type on the basis of JDBC connection string.
     *
     * @return
     */
    public DBType getDBType() {
        return dbType;
    }

    /**
     * Returns logger that can be used to log messages.
     *
     * @return
     */
    public Logger getLogger() {
        return logger;
    }

    /**
     * Returns parameter value "Skip database update phase".
     *
     * @return
     */
    public boolean getSkipDBUpdate() {
        return skipDBUpdate;
    }

    /**
     * Returns parameter value "Force non-empty database initialization".
     *
     * @return
     */
    public boolean getForceDBInitialize() {
        return forceDBInitialize;
    }

    /**
     * Returns parameter value "logging of log-ins and log-outs of users".
     *
     * @return
     */
    public boolean getLogLogins() {
        return logLogins;
    }

    /**
     * Returns parameter value "score.path".
     *
     * @return
     */
    public String getScorePath() {
        return scorePath;
    }

    /**
     * Returns parameter value "JDBC connection class".
     *
     * @return
     */
    public String getDbClassName() {
        return dbType.getDriverClassName();
    }

    /**
     * Returns parameter value "JDBC connection string".
     *
     * @return
     */
    public String getDatabaseConnection() {
        return databaseConnection;
    }

    /**
     * Returns flag of support for unique constraint (if it is switched off then
     * it is possible to insert records without referencing mandatory external records).
     *
     * @return
     */
    public boolean isH2ReferentialIntegrity() {
        return h2ReferentialIntegrity;
    }

    /**
     * Returns login for the database.
     *
     * @return
     */
    public String getDBLogin() {
        return login;
    }

    /**
     * Returns password for the database.
     *
     * @return
     */
    public String getDBPassword() {
        return password;
    }

    /**
     * Returns properties that were used to initialize Celesta. Attention:
     * it makes sense using this object as read only, dynamic change of these
     * properties doesn't lead to anything.
     *
     * @return
     */
    public Properties getSetupProperties() {
        return properties;
    }

    /**
     * Returns port of H2 DB.
     *
     * @return
     */
    public int getH2Port() {
        return h2Port;
    }

}
