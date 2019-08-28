package ru.curs.celesta.test.common;

import org.firebirdsql.testcontainers.FirebirdContainer;

import java.util.ArrayList;
import java.util.List;

public class AdvancedFireBirdContainer extends FirebirdContainer {

    public AdvancedFireBirdContainer() {
        super("jacobalberty/firebird:4.0");
    }

    @Override
    public void start() {
        super.start();

        try {
            String dropDbSql = String.format(
                "echo \"CONNECT '/firebird/data/%s' user '%s' password '%s';\n" +
                    "DROP DATABASE;\" > dropDb.sql",
                getDatabaseName(),
                getUsername(),
                getPassword()
            );

            String createDbSql =
                String.format(
                    "echo \"CREATE DATABASE '/firebird/data/%s' user '%s' password '%s' page_size = 8192;\" " +
                        "> createDb.sql",
                    getDatabaseName(),
                    getUsername(),
                    getPassword()
                );

            String execSqlTemplate = "./usr/local/firebird/bin/isql -input %s";

            List<ExecResult> execResults = new ArrayList<>();

            execResults.add(execInContainer("bash", "-c", dropDbSql));
            execResults.add(execInContainer("bash", "-c", createDbSql));
            execResults.add(execInContainer("bash", "-c", String.format(execSqlTemplate, "dropDb.sql")));
            execResults.add(execInContainer("bash", "-c", String.format(execSqlTemplate, "createDb.sql")));

            execResults.stream().filter(er -> er.getExitCode() != 0)
                .findFirst()
                .ifPresent(er -> {
                    throw new RuntimeException("Couldn't start container. " +
                        "Execution failed with status " + er.getExitCode() + " : " + er.getStderr());
                }
            );

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
