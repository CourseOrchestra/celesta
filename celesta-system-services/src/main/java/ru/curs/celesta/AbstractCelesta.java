package ru.curs.celesta;

import org.h2.tools.Server;
import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.adaptors.configuration.DbAdaptorFactory;
import ru.curs.celesta.dbutils.adaptors.ddl.JdbcDdlConsumer;
import ru.curs.celesta.event.TriggerDispatcher;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.discovery.ScoreDiscovery;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractCelesta<T extends SessionContext> implements ICelesta, AutoCloseable {

    static final String FILE_PROPERTIES = "celesta.properties";

    private final AppSettings appSettings;
    private final Score score;
    final ConnectionPool connectionPool;
    final DBAdaptor dbAdaptor;
    private final Optional<SessionLogManager> sessionLogManager;
    private final TriggerDispatcher triggerDispatcher = new TriggerDispatcher();

    private Optional<Server> server;
    final LoggingManager loggingManager;
    final PermissionManager permissionManager;
    final ProfilingManager profiler;

    final ConcurrentHashMap<String, T> sessions = new ConcurrentHashMap<>();
    final Set<CallContext> contexts = Collections.synchronizedSet(new LinkedHashSet<CallContext>());

    public AbstractCelesta(AppSettings appSettings, int phasesCount) {
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

        this.sessionLogManager = appSettings.getLogLogins()
                ? Optional.of(new SessionLogManager(this, appSettings.getLogLogins()))
                : Optional.empty();
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
     * Связывает идентификатор сессии и идентификатор пользователя.
     *
     * @param sessionId Имя сессии.
     * @param userId    Имя пользователя.
     */
    public void login(String sessionId, String userId) {
        if (sessionId == null)
            throw new IllegalArgumentException("Session id is null.");
        if (userId == null)
            throw new IllegalArgumentException("User id is null.");
        // Создавать новый SessionContext имеет смысл лишь в случае, когда
        // нет старого.
        T oldSession = sessions.get(sessionId);
        if (oldSession == null || !userId.equals(oldSession.getUserId())) {
            T session = sessionContext(userId, sessionId);
            sessions.put(sessionId, session);

            sessionLogManager.ifPresent(s -> s.logLogin(session));
        }
    }

    /**
     * Завершает сессию (удаляет связанные с ней данные).
     *
     * @param sessionId имя сессии.
     * @param timeout   признак разлогинивания по таймауту.
     */
    public T logout(String sessionId, boolean timeout) {
        T sc = sessions.remove(sessionId);
        if (sc != null) {
            sessionLogManager.ifPresent(s -> s.logLogout(sc, timeout));
        }
        return sc;
    }


    /**
     * Фиксирует (при наличии включённой настройки log.logins) неудачный логин.
     *
     * @param userId Имя пользователя, под которым производился вход.
     */
    public void failedLogin(String userId) {
        sessionLogManager.ifPresent(s -> s.logFailedLogin(userId));
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

    abstract ScoreDiscovery getScoreDiscovery();

    public abstract T getSystemSessionContext();

    abstract T sessionContext(String userId, String sessionId);

    /**
     * Initializes and returns new CallContext for specified SessionContext
     *
     * @param sessionContext
     * @return CallContext
     */
    public CallContext callContext(T sessionContext) {
        return sessionContext.callContextBuilder()
                .setCelesta(this)
                .setConnectionPool(connectionPool)
                .setSesContext(sessionContext)
                .setScore(score)
                .setDbAdaptor(dbAdaptor)
                .setPermissionManager(permissionManager)
                .setLoggingManager(loggingManager)
                .createCallContext();
    }

    @Override
    public CallContext callContext() {
        return callContext(getSystemSessionContext());
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
}
