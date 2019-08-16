package ru.curs.celesta.test;

import org.junit.jupiter.api.extension.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import ru.curs.celesta.*;
import ru.curs.celesta.dbutils.DbUpdater;
import ru.curs.celesta.dbutils.DbUpdaterImpl;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.adaptors.configuration.DbAdaptorFactory;
import ru.curs.celesta.dbutils.adaptors.ddl.JdbcDdlConsumer;
import ru.curs.celesta.score.AbstractScore;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.discovery.ScoreByScorePathDiscovery;
import ru.curs.celesta.test.mock.CelestaImpl;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class DbUpdaterExtension implements TestTemplateInvocationContextProvider, BeforeAllCallback, AfterAllCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(DbUpdaterExtension.class);

    static {
        Locale.setDefault(Locale.US);
    }

    private static final List<DBType> supportedDbTypes = Arrays.stream(DBType.values())
            .filter(dbType -> !DBType.UNKNOWN.equals(dbType))
            .collect(Collectors.toList());

    private final EnumMap<DBType, JdbcDatabaseContainer<?>> containers = new EnumMap<>(DBType.class);
    private final EnumMap<DBType, DBAdaptor> dbAdaptors = new EnumMap<>(DBType.class);

    private final Map<DBType, ConnectionPool> connectionPools = new HashMap<>();


    @Override
    public boolean supportsTestTemplate(ExtensionContext extensionContext) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
            ExtensionContext extensionContext) {

        return supportedDbTypes.stream()
                .map(this::invocationContext);
    }

    private TestTemplateInvocationContext invocationContext(final DBType dbType) {
        return new TestTemplateInvocationContext() {
            @Override
            public String getDisplayName(int invocationIndex) {
                return dbType.name();
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                return Collections.singletonList(new ParameterResolver() {
                    @Override
                    public boolean supportsParameter(ParameterContext parameterContext,
                                                     ExtensionContext extensionContext)
                            throws ParameterResolutionException {
                        return parameterContext.getParameter()
                                .getType().equals(DbUpdater.class);
                    }

                    @Override
                    public Object resolveParameter(ParameterContext parameterContext,
                                                   ExtensionContext extensionContext)
                            throws ParameterResolutionException {
                        ScorePath scorePath = parameterContext.getParameter().getAnnotation(ScorePath.class);
                        return DbUpdaterExtension.this.createDbUpdater(dbType, scorePath.value());
                    }
                });
            }
        };
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        this.startDbs();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        this.clearDbs();
    }

    private void startDbs() {

        final String emptyScorePath = "src/test/resources/emptyScore";

        PostgreSQLContainer<?> postgreSQLContainer = ContainerUtils.POSTGRE_SQL;
        postgreSQLContainer.start();
        containers.put(DBType.POSTGRESQL, postgreSQLContainer);
        OracleContainer oracleContainer = ContainerUtils.ORACLE;
        oracleContainer.start();
        containers.put(DBType.ORACLE, oracleContainer);
        MSSQLServerContainer<?> mssqlServerContainer = ContainerUtils.MSSQL;
        mssqlServerContainer.start();
        containers.put(DBType.MSSQL, mssqlServerContainer);

        supportedDbTypes.forEach(
                dbType -> {
                    ConnectionPool connectionPool =
                            this.createConnectionPool(dbType, this.containers.get(dbType));
                    DBAdaptor dbAdaptor = new DbAdaptorFactory()
                            .setDbType(dbType)
                            .setDdlConsumer(new JdbcDdlConsumer())
                            .setConnectionPool(connectionPool)
                            .createDbAdaptor();

                    this.dbAdaptors.put(dbType, dbAdaptor);
                    this.connectionPools.put(dbType, connectionPool);


                    this.createDbUpdater(dbType, emptyScorePath).updateSystemSchema();
                }
        );
    }

    private void clearDbs() {
        try {
            this.connectionPools.get(DBType.H2).get().createStatement().execute("SHUTDOWN");
        } catch (SQLException ex) {
            LOGGER.error("Error on shutting down DB", ex);
        }

        containers.forEach(
            (b, c) -> {
                this.connectionPools.get(b).close();
                ContainerUtils.cleanUp(c);
            });
    }

    private ConnectionPool createConnectionPool(DBType dbType, JdbcDatabaseContainer<?> container) {
        final ConnectionPoolConfiguration connectionPoolConfiguration = new ConnectionPoolConfiguration();

        final String jdbcUrl = Optional.ofNullable(container)
                .map(c -> c.getJdbcUrl().replace("localhost", "0.0.0.0"))
                .orElse(BaseAppSettings.H2_IN_MEMORY_URL);

        final String login = Optional.ofNullable(container)
                .map(JdbcDatabaseContainer::getUsername)
                .orElse("");

        final String password = Optional.ofNullable(container)
                .map(JdbcDatabaseContainer::getPassword)
                .orElse("");

        connectionPoolConfiguration.setJdbcConnectionUrl(jdbcUrl);
        connectionPoolConfiguration.setLogin(login);
        connectionPoolConfiguration.setPassword(password);
        connectionPoolConfiguration.setDriverClassName(dbType.getDriverClassName());

        return ConnectionPool.create(connectionPoolConfiguration);
    }

    private DbUpdater<?> createDbUpdater(DBType dbType, String scorePath) {

        final Score score;

        try {
            score = new AbstractScore.ScoreBuilder<>(Score.class)
                    .scoreDiscovery(new ScoreByScorePathDiscovery(scorePath))
                    .build();
        } catch (ParseException e) {
            throw new CelestaException(e);
        }

        DBAdaptor dbAdaptor = this.dbAdaptors.get(dbType);
        ConnectionPool connectionPool = connectionPools.get(dbType);
        connectionPool.setDbAdaptor(dbAdaptor);


        CelestaImpl celesta = new CelestaImpl(dbAdaptor, connectionPool, score);

        return new DbUpdaterImpl(connectionPool, score, true, dbAdaptor,
                celesta);
    }

}
