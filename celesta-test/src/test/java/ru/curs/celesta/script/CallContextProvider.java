package ru.curs.celesta.script;

import org.junit.jupiter.api.extension.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.SystemCallContext;
import ru.curs.celesta.test.ContainerUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Stream;

public class CallContextProvider implements TestTemplateInvocationContextProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(CallContextProvider.class);

    static {
        Locale.setDefault(Locale.US);
    }

    enum Backend {
        H2, PostgreSQL, Oracle, MSSQL, FireBird
    }

    private final EnumMap<Backend, JdbcDatabaseContainer<?>> containers = new EnumMap<>(Backend.class);
    private final EnumMap<Backend, Celesta> celestas = new EnumMap<>(Backend.class);

    private CallContext currentContext;

    @Override
    public boolean supportsTestTemplate(ExtensionContext extensionContext) {
        return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext extensionContext) {
        return Arrays.stream(Backend.values()).map(this::invocationContext);
    }

    private TestTemplateInvocationContext invocationContext(final Backend backend) {
        return new TestTemplateInvocationContext() {

            @Override
            public String getDisplayName(int invocationIndex) {
                return backend.name();
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                return Collections.singletonList(new ParameterResolver() {
                    @Override
                    public boolean supportsParameter(ParameterContext parameterContext,
                                                     ExtensionContext extensionContext) {
                        return parameterContext.getParameter()
                                .getType().equals(CallContext.class);
                    }

                    @Override
                    public Object resolveParameter(ParameterContext parameterContext,
                                                   ExtensionContext extensionContext) {
                        currentContext = new SystemCallContext(celestas.get(backend));
                        return currentContext;
                    }
                });
            }
        };
    }

    public void closeCurrentContext() {
        if (currentContext != null) {
            currentContext.rollback();
            currentContext.close();
        }
    }

    public void startCelestas() {
        celestas.put(Backend.H2, celestaFromH2());

        containers.put(Backend.PostgreSQL, ContainerUtils.POSTGRE_SQL);
        celestas.put(Backend.PostgreSQL, celestaFromContainer(containers.get(Backend.PostgreSQL)));

        containers.put(Backend.Oracle, ContainerUtils.ORACLE);
        celestas.put(Backend.Oracle, celestaFromContainer(containers.get(Backend.Oracle)));

        containers.put(Backend.MSSQL, ContainerUtils.MSSQL);
        celestas.put(Backend.MSSQL, celestaFromContainer(containers.get(Backend.MSSQL)));

        containers.put(Backend.FireBird, ContainerUtils.FIREBIRD);
        celestas.put(Backend.FireBird, celestaFromContainer(containers.get(Backend.FireBird)));

    }

    public void stopCelestas() {

        celestas.computeIfPresent(Backend.H2,
            (b, c) -> {
                try {
                    c.getConnectionPool().get().createStatement().execute("SHUTDOWN");
                } catch (SQLException ex) {
                    LOGGER.error("Error during DB shutdown", ex);
                }
                return null;
            });

        containers.forEach(
            (b, c) -> {
                this.celestas.get(b).close();
                ContainerUtils.cleanUp(c);
            });
    }

    private static Celesta celestaFromH2() {
        Properties params = new Properties();
        params.setProperty("score.path", "score");
        params.setProperty("h2.in-memory", "true");
        return Celesta.createInstance(params);
    }

    private static Celesta celestaFromContainer(JdbcDatabaseContainer container) {
        container.start();
        Properties properties = new Properties();
        properties.put("score.path", "score");
        properties.put("rdbms.connection.url", container.getJdbcUrl().replace("localhost", "0.0.0.0"));
        properties.put("rdbms.connection.username", container.getUsername());
        properties.put("rdbms.connection.password", container.getPassword());
        properties.put("force.dbinitialize", "true");
        return Celesta.createInstance(properties);
    }
}
