package ru.curs.celesta;

import org.h2.tools.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.curs.celesta.dbutils.DbUpdaterBuilder;
import ru.curs.celesta.dbutils.DbUpdaterImpl;
import ru.curs.celesta.dbutils.ILoggingManager;
import ru.curs.celesta.dbutils.IPermissionManager;
import ru.curs.celesta.dbutils.IProfiler;
import ru.curs.celesta.dbutils.LoggingManager;
import ru.curs.celesta.dbutils.PermissionManager;
import ru.curs.celesta.dbutils.ProfilingManager;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.adaptors.configuration.DbAdaptorFactory;
import ru.curs.celesta.dbutils.adaptors.ddl.JdbcDdlConsumer;
import ru.curs.celesta.event.TriggerDispatcher;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.discovery.ScoreByScorePathDiscovery;
import ru.curs.celesta.score.discovery.ScoreByScoreResourceDiscovery;
import ru.curs.celesta.score.discovery.ScoreDiscovery;
import ru.curs.celesta.ver.CelestaVersion;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;

/**
 * Celesta instance.
 */
public final class Celesta implements ICelesta {

    /**
     * Celesta version.
     *
     * @see CelestaVersion
     */
    public static final String VERSION = CelestaVersion.VERSION;

    protected static final String FILE_PROPERTIES = "celesta.properties";

    private static final Logger LOGGER = LoggerFactory.getLogger(Celesta.class);

    private final BaseAppSettings appSettings;
    private final Score score;
    private final ConnectionPool connectionPool;
    private final DBAdaptor dbAdaptor;
    private final TriggerDispatcher triggerDispatcher = new TriggerDispatcher();

    private Optional<Server> server;
    private final LoggingManager loggingManager;
    private final PermissionManager permissionManager;
    private final ProfilingManager profiler;

    Celesta(BaseAppSettings appSettings, ConnectionPool connectionPool) {
        this.appSettings = appSettings;
        this.connectionPool = connectionPool;
        manageH2Server();

        // CELESTA STARTUP SEQUENCE
        // 1. Parsing of grains description.
        LOGGER.info("Celesta initialization: score parsing...");

        try {
            ScoreDiscovery scoreDiscovery = this.appSettings.getScorePath().isEmpty()
                    ? new ScoreByScoreResourceDiscovery()
                    : new ScoreByScorePathDiscovery(appSettings.getScorePath());
            this.score = new Score.ScoreBuilder<>(Score.class)
                    .scoreDiscovery(scoreDiscovery)
                    .build();
        } catch (ParseException e) {
            throw new CelestaException(e);
        }
        CurrentScore.set(this.score);
        LOGGER.info("done.");

        LOGGER.info(this.score.describeGrains());

        // 2. Updating database structure.
        // Since at this stage meta information is already in use, theCelesta and ConnectionPool
        // have to be initialized.
        DbAdaptorFactory dac = new DbAdaptorFactory()
                .setDbType(appSettings.getDBType())
                .setDdlConsumer(new JdbcDdlConsumer())
                .setConnectionPool(connectionPool)
                .setH2ReferentialIntegrity(appSettings.isH2ReferentialIntegrity());

        dbAdaptor = dac.createDbAdaptor();

        this.loggingManager = new LoggingManager(this);
        this.permissionManager = new PermissionManager(this);
        this.profiler = new ProfilingManager(this);

        if (!appSettings.getSkipDBUpdate()) {
            LOGGER.info("Celesta initialization: database {} upgrade...",
                    PasswordHider.maskPassword(appSettings.getDatabaseConnection()));

            DbUpdaterImpl dbUpdater = new DbUpdaterBuilder()
                    .dbAdaptor(dbAdaptor)
                    .connectionPool(connectionPool)
                    .score(score)
                    .forceDdInitialize(appSettings.getForceDBInitialize())
                    .setCelesta(this)
                    .build();

            dbUpdater.updateDb();
            LOGGER.info("done.");
        } else {
            LOGGER.info("Celesta initialization: database upgrade...skipped.");
        }

    }

    @Override
    public Properties getSetupProperties() {
        return appSettings.getSetupProperties();
    }

    @Override
    public IPermissionManager getPermissionManager() {
        return permissionManager;
    }

    @Override
    public ILoggingManager getLoggingManager() {
        return loggingManager;
    }

    @Override
    public ConnectionPool getConnectionPool() {
        return connectionPool;
    }

    @Override
    public IProfiler getProfiler() {
        return profiler;
    }

    @Override
    public DBAdaptor getDBAdaptor() {
        return dbAdaptor;
    }

    @Override
    public TriggerDispatcher getTriggerDispatcher() {
        return this.triggerDispatcher;
    }

    @Override
    public Score getScore() {
        return score;
    }

    /**
     * Stops working of Celesta. After the call the instance of Celesta becomes unusable.
     */
    @Override
    public void close() {
        connectionPool.close();
        server.ifPresent(Server::shutdown);
    }

    /**
     * Creates Celesta instance with specified properties and {@link DataSource}.
     *
     * @param properties Celesta initialization properties. All the properties regarding db connection will be ignored,
     *                   but {@code rdbms.connection.url} is still required in order for
     *                   Celesta to define the database type (you may pass only the
     *                   prefix, e. g. {@code jdbc:postgresql})
     * @param dataSource Provided data source.
     * @return
     */
    public static Celesta createInstance(Properties properties, DataSource dataSource) {
        return createInstance(properties, new DatasourceConnectionPool(dataSource));
    }


    /**
     * Creates Celesta instance with specified properties and ConnectionPool.
     *
     * @param properties     Celesta initialization properties. All the properties regarding db connection
     *                       will be ignored, but {@code rdbms.connection.url} is still required in order for
     *                       Celesta to define the database type (you may pass only the
     *                       prefix, e. g. {@code jdbc:postgresql})
     * @param connectionPool Provided connection pool (either {@link DatasourceConnectionPool} or
     *                       {@link InternalConnectionPool}).
     * @return
     */
    public static Celesta createInstance(Properties properties, ConnectionPool connectionPool) {
        AppSettings appSettings = preInit(properties);
        return new Celesta(appSettings, connectionPool);
    }

    /**
     * Creates Celesta instance with the specified properties and internal connection pool.
     *
     * @param properties Celesta initialization properties
     * @return
     */
    public static Celesta createInstance(Properties properties) {
        AppSettings appSettings = preInit(properties);
        ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
        cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
        cpc.setDriverClassName(appSettings.getDbClassName());
        cpc.setLogin(appSettings.getDBLogin());
        cpc.setPassword(appSettings.getDBPassword());
        return new Celesta(appSettings, InternalConnectionPool.create(cpc));
    }

    /**
     * Creates Celesta instance with properties specified in <b>celesta.properties</b>
     * file.
     *
     * @return
     */
    public static Celesta createInstance() {
        Properties properties = loadPropertiesDynamically();
        return createInstance(properties);
    }

    private static AppSettings preInit(Properties properties) {
        LOGGER.info("Celesta ver. {}", VERSION != null ? VERSION : "N/A (invalid build?)");
        LOGGER.info("Celesta pre-initialization: system settings reading...");
        AppSettings appSettings = new AppSettings(properties);
        LOGGER.info("done.");
        return appSettings;
    }

    /**
     * Reads and returns properties from <b>celesta.properties</b> file.
     *
     * @return
     */
    public static Properties loadPropertiesDynamically() {
        // Разбираемся с настроечным файлом: читаем его и превращаем в
        // Properties.
        Properties properties = new Properties();
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            InputStream in = loader.getResourceAsStream(FILE_PROPERTIES);
            try (in) {
                if (in == null) {
                    throw new CelestaException(
                            String.format("Couldn't find file %s on classpath.", FILE_PROPERTIES)
                    );
                }
                properties.load(in);
            }
        } catch (IOException e) {
            throw new CelestaException(
                    String.format("IOException while reading %s file: %s", FILE_PROPERTIES, e.getMessage())
            );
        }

        return properties;
    }

    private void manageH2Server() {
        if (appSettings.getH2Port() > 0) {
            try {
                LOGGER.info("H2 server starting on port {}...", appSettings.getH2Port());
                server = Optional.of(Server.createTcpServer(
                        "-tcpPort",
                        Integer.toString(appSettings.getH2Port()),
                        "-ifNotExists",
                        "-tcpAllowOthers").start());

                LOGGER.info("done.");

                CurrentScore.global(true);
            } catch (SQLException e) {
                throw new CelestaException(e);
            }
        } else {
            server = Optional.empty();
            CurrentScore.global(false);
        }
    }


    /**
     * Returns if profiling mode is set (whether the time of method calls
     * is written to 'calllog' table).
     *
     * @return
     */
    public boolean isProfilemode() {
        return profiler.isProfilemode();
    }

    /**
     * Returns the behavior {@code NULLS FIRST} of current database.
     *
     * @return
     */
    public boolean nullsFirst() {
        return dbAdaptor.nullsFirst();
    }

    /**
     * Sets profiling mode.
     *
     * @param profilemode profiling mode
     */
    public void setProfilemode(boolean profilemode) {
        profiler.setProfilemode(profilemode);
    }

}
