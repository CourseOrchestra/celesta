package ru.curs.celesta;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PasswordHiderTest {
    @Test
    public void passwordInJDBCURLIsMasked() {
        assertEquals(null, PasswordHider.maskPassword(null));

        // Oracle
        assertEquals(
                "jdbc:oracle:thin:formserver/*****@localhost:1521:xe",
                PasswordHider.maskPassword("jdbc:oracle:thin:formserver/123@localhost:1521:xe"));
        assertEquals(
                "jdBc:oracle:thin:scott/*****@myhost:1521:orcl",
                PasswordHider.maskPassword("jdBc:oracle:thin:scott/tiger@myhost:1521:orcl"));

        // SQL Server в разных позициях и с экранированием точки с запятой
        // (включая случаи не-экранирования)
        assertEquals(
                "jdbc:sqlserveR://kapustina;databaseName=FS;user=sa;password=*****",
                PasswordHider.maskPassword("jdbc:sqlserveR://kapustina;databaseName=FS;user=sa;password=F7&08420Dx"));
        assertEquals(
                "jdbc:sqlserver://kapustina;databaseName=FS;user=sa;pAssword=*****",
                PasswordHider.maskPassword("jdbc:sqlserver://kapustina;databaseName=FS;user=sa;pAssword=F7084{;}20Dx"));
        assertEquals(
                "jdbc:sqlserver://localhost;databaseName=fs;password=*****;user=sa",
                PasswordHider.maskPassword("jdbc:sqlserver://localhost;databaseName=fs;password=1&2{;}3;user=sa"));
        assertEquals(
                "jdbc:sqlserver://localhost;databaseName=fs;password=*****;user=sa",
                PasswordHider.maskPassword("jdbc:sqlserver://localhost;databaseName=fs;password=aa{a{;user=sa"));
        assertEquals(
                "jdbc:sqlserver://localhost;databaseName=fs;user=sa;password=*****",
                PasswordHider.maskPassword("jdbc:sqlserver://localhost;databaseName=fs;user=sa;password=aa{a{"));

        // MySQL в разных позициях
        assertEquals(
                "jdbc:mYsql://localhost/test?user=root&password=*****",
                PasswordHider.maskPassword("jdbc:mYsql://localhost/test?user=root&password=123"));
        // Выдуманный драйвер: всё, что за словом password -- скрывается
        assertEquals("jdbc:blahblah:test?paSsword=*****",
                PasswordHider.maskPassword("jdbc:blahblah:test?paSsword=sdfwe&;sdf"));
    }

}
