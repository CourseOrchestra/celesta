package ru.curs.celestaunit;

import org.junit.jupiter.api.extension.*;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.SystemCallContext;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.BasicTable;
import ru.curs.celesta.score.SequenceElement;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Extension class for JUnit5 tests.
 * <p>
 * Creates Celesta using Score Path parameter and H2 embedded in-memory database.
 */
public final class CelestaUnitExtension implements BeforeAllCallback,
        AfterAllCallback, ParameterResolver, BeforeEachCallback, AfterEachCallback {

    static final String DEFAULT_SCORE = Stream.of("src/main/celestasql", "src/test/celestasql")
            .filter(
                    s -> {
                        File f = new File(s);
                        return f.isDirectory() && f.canRead();
                    }
            ).collect(Collectors.joining(File.pathSeparator));

    private final String scorePath;
    private final boolean referentialIntegrity;
    private final boolean truncateTables;
    private final boolean resetSequences;
    private final Namespace namespace;

    private Celesta celesta;

    public CelestaUnitExtension() {
        this(builder());
    }

    private CelestaUnitExtension(Builder builder) {
        scorePath = builder.builderScorePath;
        referentialIntegrity = builder.builderReferentialIntegrity;
        truncateTables = builder.builderTruncateTables;
        resetSequences = builder.builderResetSequences;
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
            try (Statement statement = celesta.getConnectionPool().get().createStatement()) {
                statement.execute("SHUTDOWN");
            }
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
    public void beforeEach(ExtensionContext extensionContext) {
        if (truncateTables) {
            truncateTables();
        }
        if (resetSequences) {
            resetSequences();
        }
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        CallContext ctx = extensionContext
                .getStore(namespace)
                .remove(extensionContext.getUniqueId(), CallContext.class);
        if (ctx != null) {
            ctx.close();
        }
    }

    private void truncateTables() {
        try (Connection conn = celesta.getConnectionPool().get();
             Statement stmt = conn.createStatement()) {
            if (referentialIntegrity) {
                stmt.execute("SET REFERENTIAL_INTEGRITY FALSE");
            }
            for (Map.Entry<String, Grain> e : celesta.getScore().getGrains().entrySet()) {
                if (!Score.SYSTEM_SCHEMA_NAME.equals(e.getKey())) {
                    Grain grain = e.getValue();
                    for (BasicTable table : grain.getTables().values()) {
                        stmt.execute(String.format("truncate table %s.%s",
                                grain.getQuotedName(),
                                table.getQuotedName()));
                    }
                }
            }
            if (referentialIntegrity) {
                stmt.execute("SET REFERENTIAL_INTEGRITY TRUE");
            }
        } catch (SQLException e) {
            throw new CelestaException(e);
        }
    }

    private void resetSequences() {
        try (Connection conn = celesta.getConnectionPool().get();
             Statement stmt = conn.createStatement()) {
            for (Grain grain : celesta.getScore().getGrains().values()) {
                for (String seqName : grain.getElements(SequenceElement.class).keySet()) {
                    stmt.execute(String.format("ALTER SEQUENCE \"%s\".\"%s\" RESTART WITH 1", grain.getName(), seqName));
                    conn.commit();
                }
            }
        } catch (SQLException e) {
            throw new CelestaException(e);
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
     * Is every table truncated before each test.
     */
    boolean isTruncateTables() {
        return truncateTables;
    }

    /**
     * Is every sequence reset before each test.
     */
    public boolean isResetSequences() {
        return resetSequences;
    }

    /**
     * Builder for CelestaUnitExtension, that allows to
     * override defaults.
     */
    public static final class Builder {
        private String builderScorePath = DEFAULT_SCORE;
        private boolean builderReferentialIntegrity = true;
        private boolean builderTruncateTables = true;
        private boolean builderResetSequences = true;

        private Builder() {
        }

        /**
         * Sets score path.
         *
         * @param scorePath Score path (maybe relative to project root).
         */
        public Builder withScorePath(String scorePath) {
            this.builderScorePath = scorePath;
            return this;
        }

        /**
         * Sets referential integrity.
         *
         * @param referentialIntegrity Set to false to disable.
         */
        public Builder withReferentialIntegrity(boolean referentialIntegrity) {
            this.builderReferentialIntegrity = referentialIntegrity;
            return this;
        }

        /**
         * Sets tables truncation before each test (true by default).
         *
         * @param truncateTables Set to true to truncateTables each table before each test.
         */
        public Builder withTruncateTables(boolean truncateTables) {
            this.builderTruncateTables = truncateTables;
            return this;
        }

        /**
         * Resets sequences before each test (true by default).
         *
         * @param resetSequences Set to true to reset sequences before each test
         */
        public Builder withResetSequences(boolean resetSequences) {
            this.builderResetSequences = resetSequences;
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
