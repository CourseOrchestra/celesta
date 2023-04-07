package ru.curs.celesta.test.common;

import org.firebirdsql.testcontainers.FirebirdContainer;

import java.util.ArrayList;
import java.util.List;

public final class AdvancedFireBirdContainer extends FirebirdContainer<AdvancedFireBirdContainer> {

    private static final String EXEC_SQL_TEMPLATE = "./usr/local/firebird/bin/isql -input %s";

    private boolean firstStart = true;

    public AdvancedFireBirdContainer() {
        super("jacobalberty/firebird:v4.0");
    }

    @Override
    public void start() {
        super.start();

        if (firstStart) {
            firstStart = false;
            this.recreateDbWithBiggerPageSize();
        } else {
            this.createDb();
        }

    }

    public void dropDb() {

        try {
            String dropDbSql = String.format(
                    "echo \"CONNECT '/firebird/data/%s' user '%s' password '%s';\n"
                            + "DROP DATABASE;\" > dropDb.sql",
                    getDatabaseName(),
                    getUsername(),
                    getPassword()
            );

            List<ExecResult> execResults = new ArrayList<>();

            execResults.add(execInContainer("bash", "-c", dropDbSql));
            execResults.add(execInContainer("bash", "-c", String.format(EXEC_SQL_TEMPLATE, "dropDb.sql")));

            validateExecResults(execResults);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void createDb() {
        try {
            String createDbSql =
                String.format(
                        "echo \"CREATE DATABASE '/firebird/data/%s' user '%s' password '%s' page_size = 8192;\" "
                                + "> createDb.sql",
                        getDatabaseName(),
                        getUsername(),
                        getPassword()
                );


            List<ExecResult> execResults = new ArrayList<>();

            execResults.add(execInContainer("bash", "-c", createDbSql));
            execResults.add(execInContainer("bash", "-c", String.format(EXEC_SQL_TEMPLATE, "createDb.sql")));

            validateExecResults(execResults);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void validateExecResults(List<ExecResult> execResults) {
        execResults.stream().filter(er -> er.getExitCode() != 0)
            .findFirst()
            .ifPresent(er -> {
                        throw new RuntimeException("Couldn't start container. "
                                + "Execution failed with status " + er.getExitCode() + " : " + er.getStderr());
                    }
            );
    }

    private void recreateDbWithBiggerPageSize() {
        this.dropDb();
        this.createDb();
    }
}
