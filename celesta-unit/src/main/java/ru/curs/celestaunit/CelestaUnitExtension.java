package ru.curs.celestaunit;

import org.junit.jupiter.api.extension.*;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import ru.curs.celesta.*;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.Table;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Extension class for JUnit5 tests.
 * <p>
 * Creates Celesta using Score Path parameter and H2 embedded in-memory database.
 */
public class CelestaUnitExtension implements BeforeAllCallback,
        AfterAllCallback, ParameterResolver, AfterEachCallback {

    static final String DEFAULT_SCORE = "src/main/resources/score";

    private final String scorePath;
    private final boolean referentialIntegrity;
    private final boolean truncateAfterEach;
    private final Namespace namespace;

    private Celesta celesta;


    public CelestaUnitExtension() {
        this(builder());
    }

    private CelestaUnitExtension(Builder builder) {
        scorePath = builder.scorePath;
        referentialIntegrity = builder.referentialIntegrity;
        truncateAfterEach = builder.truncateAfterEach;
        namespace = Namespace.create(this);
    }

    /**
     * Returns builder for CelestaUnitExtension instance, allowing
     * to override default settings.
     */
    public static Builder builder() {
        return new Builder();
    }


    @Override
    public void afterAll(ExtensionContext extensionContext) {
        try {
            celesta.getConnectionPool().get().createStatement().execute("SHUTDOWN");
            celesta.close();
        } catch (Exception e) {
            throw new CelestaException(e);
        }
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        Properties params = new Properties();
        params.setProperty("score.path", scorePath);
        params.setProperty("h2.in-memory", "true");
        celesta = Celesta.createInstance(params);
        assertSame(celesta.getSetupProperties(), params);
        try (Connection conn = celesta.getConnectionPool().get();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SET REFERENTIAL_INTEGRITY " + referentialIntegrity);
        } catch (SQLException e) {
            throw new CelestaException(e);
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return parameterContext.getParameter().getType() == CallContext.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        CallContext ctx = new SystemCallContext(celesta, extensionContext.getDisplayName());
        extensionContext.getStore(namespace)
                .put(extensionContext.getUniqueId(), ctx);
        return ctx;
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        CallContext ctx = extensionContext
                .getStore(namespace)
                .remove(extensionContext.getUniqueId(), CallContext.class);
        if (ctx != null) {
            ctx.close();
        }

        if (truncateAfterEach) {
            try (Connection conn = celesta.getConnectionPool().get();
                 Statement stmt = conn.createStatement()) {
                stmt.execute("SET REFERENTIAL_INTEGRITY FALSE");
                for (Map.Entry<String, Grain> e : celesta.getScore().getGrains().entrySet()) {
                    if ((!e.getKey().equals(Score.SYSTEM_SCHEMA_NAME))) {
                        Grain grain = e.getValue();
                        for (Table table : grain.getTables().values()) {
                            stmt.execute(String.format("truncate table %s.%s",
                                    grain.getQuotedName(),
                                    table.getQuotedName()));
                        }
                    }
                }
                stmt.execute("SET REFERENTIAL_INTEGRITY " + referentialIntegrity);
            } catch (SQLException e) {
                throw new CelestaException(e);
            }
        }
    }

    /**
     * Score path.
     */
    String getScorePath() {
        return scorePath;
    }

    /**
     * Is referential integrity check set on H2.
     */
    boolean isReferentialIntegrity() {
        return referentialIntegrity;
    }

    /**
     * Is every table truncated after each test.
     */
    boolean isTruncateAfterEach() {
        return truncateAfterEach;
    }

    /**
     * Returns Celesta instance.
     */
    Celesta getCelesta() {
        return celesta;
    }

    public static final class Builder {
        private String scorePath = DEFAULT_SCORE;
        private boolean referentialIntegrity = false;
        private boolean truncateAfterEach = true;

        private Builder() {
        }

        public Builder withScorePath(String scorePath) {
            this.scorePath = scorePath;
            return this;
        }

        public Builder withReferentialIntegrity(boolean referentialIntegrity) {
            this.referentialIntegrity = referentialIntegrity;
            return this;
        }

        public Builder withTruncateAfterEach(boolean truncateAfterEach) {
            this.truncateAfterEach = truncateAfterEach;
            return this;
        }

        /**
         * Generates CelestaUnitExtension with given parameters.
         */
        public CelestaUnitExtension build() {
            return new CelestaUnitExtension(this);
        }
    }
}
