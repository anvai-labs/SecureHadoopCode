package com.cloudera.sa.querykerberosauthhs2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

class QueryKerberosAuthHS2Test {
  @TempDir Path tempDir;

  @Test
  void buildsTlsKerberosJdbcUrlWithoutEmbeddingSecrets() {
    assertEquals(
        "jdbc:hive2://hs2.example.com:10000/analytics;principal=hive/_HOST@EXAMPLE.COM;ssl=true",
        QueryKerberosAuthHS2.buildJdbcUrl(
            "hs2.example.com", 10000, "analytics", "EXAMPLE.COM", true));
  }

  @Test
  void rejectsJdbcParameterInjectionAndBadPorts() {
    assertThrows(IllegalArgumentException.class,
        () -> QueryKerberosAuthHS2.buildJdbcUrl("host;password=x", 10000, "db", "REALM", true));
    assertThrows(IllegalArgumentException.class,
        () -> QueryKerberosAuthHS2.buildJdbcUrl("host", 0, "db", "REALM", true));
    assertThrows(IllegalArgumentException.class,
        () -> QueryKerberosAuthHS2.buildJdbcUrl("host", 10000, "db/name", "REALM", true));
    assertThrows(IllegalArgumentException.class,
        () -> QueryKerberosAuthHS2.buildJdbcUrl("host", 10000, "db", "REALM;ssl=false", true));
  }

  @Test
  void delegatesOnlyValidatedUrlToConnector() throws Exception {
    AtomicReference<String> captured = new AtomicReference<>();
    Connection expected = mock(Connection.class);
    QueryKerberosAuthHS2 query = new QueryKerberosAuthHS2(url -> {
      captured.set(url);
      return expected;
    });

    assertEquals(expected, query.connect("host", 443, "db", "REALM", true));
    assertTrue(captured.get().endsWith(";ssl=true"));
  }

  @Test
  void statementsAreExecutedAndClosed() throws Exception {
    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    PreparedStatement prepared = mock(PreparedStatement.class);
    when(connection.createStatement()).thenReturn(statement);
    when(connection.prepareStatement("select 2")).thenReturn(prepared);
    when(statement.execute("select 1")).thenReturn(true);
    when(prepared.execute()).thenReturn(false);
    QueryKerberosAuthHS2 query = new QueryKerberosAuthHS2(url -> connection);

    assertTrue(query.executeQueryStatement(connection, "select 1"));
    assertFalse(query.executeQueryPreparedStatement(connection, "select 2"));
    verify(statement).close();
    verify(prepared).close();
  }

  @Test
  void rejectsBlankQueriesAndInvalidKeytabsBeforeExternalCalls() {
    QueryKerberosAuthHS2 query = new QueryKerberosAuthHS2(url -> mock(Connection.class));
    assertThrows(NullPointerException.class,
        () -> query.executeQueryStatement(null, "select 1"));
    assertThrows(IllegalArgumentException.class,
        () -> query.executeQueryStatement(mock(Connection.class), " "));
    assertThrows(IllegalArgumentException.class,
        () -> QueryKerberosAuthHS2.loginFromKeytab("user@REALM", Path.of("relative.keytab")));
    assertThrows(IllegalArgumentException.class,
        () -> QueryKerberosAuthHS2.loginFromKeytab("bad;principal", Path.of("/missing.keytab")));
  }

  @Test
  void validatesRealKeytabAndRejectsSymlink() throws Exception {
    Path keytab = Files.writeString(tempDir.resolve("user.keytab"), "test-only");
    Path symlink = tempDir.resolve("linked.keytab");
    Files.createSymbolicLink(symlink, keytab);

    try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
      QueryKerberosAuthHS2.loginFromKeytab("svc/hive@EXAMPLE.COM", keytab);
      ugi.verify(() -> UserGroupInformation.loginUserFromKeytab(
          "svc/hive@EXAMPLE.COM", keytab.toString()));
    }
    assertThrows(IllegalArgumentException.class,
        () -> QueryKerberosAuthHS2.loginFromKeytab("user@EXAMPLE.COM", symlink));
  }

  @Test
  void defaultConnectorIsAvailableAndNullConnectorIsRejected() {
    new QueryKerberosAuthHS2();
    assertThrows(NullPointerException.class, () -> new QueryKerberosAuthHS2(null));
  }
}
