package ru.curs.celesta;

import org.h2.tools.Server;
import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.adaptors.configuration.DbAdaptorFactory;
import ru.curs.celesta.dbutils.adaptors.ddl.JdbcDdlConsumer;
import ru.curs.celesta.event.TriggerDispatcher;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.discovery.DefaultScoreDiscovery;
import ru.curs.celesta.score.discovery.ScoreDiscovery;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;

public class Celesta implements ICelesta, AutoCloseable {

    protected static final String FILE_PROPERTIES = "celesta.properties";

    private final BaseAppSettings appSettings;
    private final Score score;
    final ConnectionPool connectionPool;
    final DBAdaptor dbAdaptor;
    private final TriggerDispatcher triggerDispatcher = new TriggerDispatcher();
    private final ScoreDiscovery scoreDiscovery = new DefaultScoreDiscovery();

    private Optional<Server> server;
    final LoggingManager loggingManager;
    final PermissionManager permissionManager;
    final ProfilingManager profiler;

    final Set<CallContext> contexts = Collections.synchronizedSet(new LinkedHashSet<CallContext>());

    public Celesta(BaseAppSettings appSettings, int phasesCount) {
        this.appSettings = appSettings;
        manageH2Server();

        // CELESTA STARTUP SEQUENCE
        // 1. Разбор описания гранул.
        System.out.printf("Celesta initialization: phase 1/%s score parsing...", phasesCount);

        try {
            this.score = new Score.ScoreBuilder<>(Score.class)
                    .path(appSettings.getScorePath())
                    .scoreDiscovery(getScoreDiscovery())
                    .build();
        } catch (ParseException e) {
            throw new CelestaException(e);
        }
        CurrentScore.set(this.score);
        System.out.println("done.");

        // 2. Обновление структуры базы данных.
        // Т. к. на данном этапе уже используется метаинформация, то theCelesta и ConnectionPool
        // необходимо проинициализировать.
        ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
        cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
        cpc.setDriverClassName(appSettings.getDbClassName());
        cpc.setLogin(appSettings.getDBLogin());
        cpc.setPassword(appSettings.getDBPassword());
        connectionPool = ConnectionPool.create(cpc);

        DbAdaptorFactory dac = new DbAdaptorFactory()
                .setDbType(appSettings.getDBType())
                .setDdlConsumer(new JdbcDdlConsumer())
                .setConnectionPool(connectionPool)
                .setH2ReferentialIntegrity(appSettings.isH2ReferentialIntegrity());

        dbAdaptor = dac.createDbAdaptor();

        this.loggingManager = new LoggingManager(this, dbAdaptor);
        this.permissionManager = new PermissionManager(this, dbAdaptor);
        this.profiler = new ProfilingManager(this, dbAdaptor);

        if (!appSettings.getSkipDBUpdate()) {
            System.out.printf("Celesta initialization: phase 2/%s database upgrade...", phasesCount);

            DbUpdaterImpl dbUpdater = new DbUpdaterBuilder()
                    .dbAdaptor(dbAdaptor)
                    .connectionPool(connectionPool)
                    .score(score)
                    .forceDdInitialize(appSettings.getForceDBInitialize())
                    .setCelesta(this)
                    .setPermissionManager(permissionManager)
                    .setLoggingManager(loggingManager)
                    .build();

            dbUpdater.updateDb();
            System.out.println("done.");
        } else {
            System.out.printf("Celesta initialization: phase 2/%s database upgrade...skipped.%n", phasesCount);
        }

    }

    /**
     * Returns the set of active (running) call contexts (for monitoring/debug
     * purposes).
     */
    public Collection<CallContext> getActiveContexts() {
        return Collections.unmodifiableCollection(contexts);
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
     * Останавливает работу Celesta. После вызова экземпляр Celesta становится непригодным для использования.
     */
    @Override
    public void close() {
        connectionPool.close();
        server.ifPresent(Server::shutdown);
    }

    public static Celesta createInstance(Properties properties) {
        AppSettings appSettings = preInit(properties);
        return new Celesta(appSettings, 3);
    }

    public static Celesta createInstance() {
        Properties properties = loadPropertiesDynamically();
        return createInstance(properties);
    }

    private static AppSettings preInit(Properties properties) {
        System.out.print("Celesta pre-initialization: phase 1/2 system settings reading...");
        AppSettings appSettings = new AppSettings(properties);
        System.out.println("done.");
        return appSettings;
    }

    public static Properties loadPropertiesDynamically() {
        // Разбираемся с настроечным файлом: читаем его и превращаем в
        // Properties.
        Properties properties = new Properties();
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            InputStream in = loader.getResourceAsStream(FILE_PROPERTIES);
            if (in == null) {
                throw new CelestaException(
                        String.format("Couldn't find file %s on classpath.", FILE_PROPERTIES)
                );
            }
            try {
                properties.load(in);
            } finally {
                in.close();
            }
        } catch (IOException e) {
            throw new CelestaException(
                    String.format("IOException while reading %s file: %s", FILE_PROPERTIES, e.getMessage())
            );
        }

        return properties;
    }

    protected ScoreDiscovery getScoreDiscovery() {
        return this.scoreDiscovery;
    }

    private void manageH2Server() {
        if (appSettings.getH2Port() > 0) {
            try {
                System.out.printf("H2 server starting on port %d...", appSettings.getH2Port());
                server = Optional.of(Server.createTcpServer(
                        "-tcpPort",
                        Integer.toString(appSettings.getH2Port()),
                        "-tcpAllowOthers").start());
                System.out.println("done.");

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
     * Режим профилирования (записывается ли в таблицу calllog время вызовов
     * процедур).
     */
    public boolean isProfilemode() {
        return profiler.isProfilemode();
    }

    /**
     * Возвращает поведение NULLS FIRST текущей базы данных.
     */
    public boolean nullsFirst() {
        return dbAdaptor.nullsFirst();
    }

    /**
     * Устанавливает режим профилирования.
     *
     * @param profilemode режим профилирования.
     */
    public void setProfilemode(boolean profilemode) {
        profiler.setProfilemode(profilemode);
    }

    @Override
    public CallContext callContext() {
        return CallContext
                .builder()
                .setUserId(SUPER)
                .setCelesta(this)
                .setProcName("init")
                .createCallContext();
    }
}
