package ru.curs.celesta;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PasswordHiderTest {
    @ParameterizedTest
    @CsvSource(
            nullValues = "null",
            value = {"null,null",
                    // Oracle
                    "jdbc:oracle:thin:formserver/*****@localhost:1521:xe," +
                            "jdbc:oracle:thin:formserver/123@localhost:1521:xe",
                    "jdBc:oracle:thin:scott/*****@myhost:1521:orcl," +
                            "jdBc:oracle:thin:scott/tiger@myhost:1521:orcl",
                    // SQL Server + various cases of semicolon escaping
                    "jdbc:sqlserveR://kapustina;databaseName=FS;user=sa;password=*****," +
                            "jdbc:sqlserveR://kapustina;databaseName=FS;user=sa;password=F7&08420Dx",
                    "jdbc:sqlserver://kapustina;databaseName=FS;user=sa;pAssword=*****," +
                            "jdbc:sqlserver://kapustina;databaseName=FS;user=sa;pAssword=F7084{;}20Dx",
                    "jdbc:sqlserver://localhost;databaseName=fs;password=*****;user=sa," +
                            "jdbc:sqlserver://localhost;databaseName=fs;password=1&2{;}3;user=sa",
                    "jdbc:sqlserver://localhost;databaseName=fs;password=*****;user=sa," +
                            "jdbc:sqlserver://localhost;databaseName=fs;password=aa{a{;user=sa",
                    "jdbc:sqlserver://localhost;databaseName=fs;user=sa;password=*****," +
                            "jdbc:sqlserver://localhost;databaseName=fs;user=sa;password=aa{a{",
                    // MySQL
                    "jdbc:mYsql://localhost/test?user=root&password=*****," +
                            "jdbc:mYsql://localhost/test?user=root&password=123",
                    // Imaginary driver: cut off everything after "password"
                    "jdbc:blahblah:test?paSsword=*****,jdbc:blahblah:test?paSsword=sdfwe&;sdf"
            }
    )
    public void passwordInJDBCURLIsMasked(String masked, String original) {
        assertEquals(masked, PasswordHider.maskPassword(original));
    }

}
