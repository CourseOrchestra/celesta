package ru.curs.celesta;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public abstract class BaseAppSettings {

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
        if (scorePath.isEmpty()) {
            sb.append("No score path given (score.path).\n");
        } else {
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
                databaseConnection = "jdbc:h2:mem:celesta;DB_CLOSE_DELAY=-1";
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
     * Возвращает тип базы данных на основе JDBC-строки подключения.
     */
    public DBType getDBType() {
        return dbType;
    }

    /**
     * Возвращает логгер, в который можно записывать сообщения.
     */
    public Logger getLogger() {
        return logger;
    }

    /**
     * Значение параметра "пропускать фазу обновления базы данных".
     */
    public boolean getSkipDBUpdate() {
        return skipDBUpdate;
    }

    /**
     * Значение параметра "заставлять обновлять непустую базу данных".
     */
    public boolean getForceDBInitialize() {
        return forceDBInitialize;
    }

    /**
     * Значение параметра "логировать входы и выходы пользователей".
     */
    public boolean getLogLogins() {
        return logLogins;
    }

    /**
     * Значение параметра "score.path".
     */
    public String getScorePath() {
        return scorePath;
    }

    /**
     * Значение параметра "Класс JDBC-подключения".
     */
    public String getDbClassName() {
        return dbType.getDriverClassName();
    }

    /**
     * Значение параметра "Строка JDBC-подключения".
     */
    public String getDatabaseConnection() {
        return databaseConnection;
    }

    /**
     * Флаг поддержки для uniq constraint (отключение позволяет, например,
     * вставлять записи без наличия ссылок на обязательные внешние записи).
     */
    public boolean isH2ReferentialIntegrity() {
        return h2ReferentialIntegrity;
    }

    /**
     * Логин к базе данных.
     */
    public String getDBLogin() {
        return login;
    }

    /**
     * Пароль к базе данных.
     */
    public String getDBPassword() {
        return password;
    }

    /**
     * Возвращает свойства, с которыми была инициализирована Челеста. Внимание:
     * данный объект имеет смысл использовать только на чтение, динамическое
     * изменение этих свойств не приводит ни к чему.
     */
    public Properties getSetupProperties() {
        return properties;
    }

    public int getH2Port() {
        return h2Port;
    }
}
