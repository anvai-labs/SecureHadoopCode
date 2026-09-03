package com.cloudera.sa.querykerberosauthhs2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.hadoop.security.UserGroupInformation;

public final class QueryKerberosAuthHS2 {
  private static final Pattern HOST = Pattern.compile("[A-Za-z0-9.-]+");
  private static final Pattern DATABASE = Pattern.compile("[A-Za-z0-9_]+");
  private static final Pattern PRINCIPAL = Pattern.compile("[A-Za-z0-9/._-]+@[A-Za-z0-9._-]+");
  private static final Pattern REALM = Pattern.compile("[A-Za-z0-9._-]+");
  private final JdbcConnector connector;

  public QueryKerberosAuthHS2() {
    this(QueryKerberosAuthHS2::openConnection);
  }

  QueryKerberosAuthHS2(JdbcConnector connector) {
    this.connector = Objects.requireNonNull(connector, "connector");
  }

  public static void loginFromKeytab(String principal, Path keytab) throws IOException {
    requireMatch(principal, PRINCIPAL, "Kerberos principal");
    if (keytab == null || !keytab.isAbsolute() || Files.isSymbolicLink(keytab)
        || !Files.isRegularFile(keytab)) {
      throw new IllegalArgumentException("keytab must be an existing absolute regular file, not a symlink");
    }
    UserGroupInformation.loginUserFromKeytab(principal, keytab.toString());
  }

  public Connection connect(
      String hostName, int port, String database, String realm, boolean ssl)
      throws ClassNotFoundException, SQLException {
    return connector.connect(buildJdbcUrl(hostName, port, database, realm, ssl));
  }

  static String buildJdbcUrl(
      String hostName, int port, String database, String realm, boolean ssl) {
    requireMatch(hostName, HOST, "host");
    requireMatch(database, DATABASE, "database");
    requireMatch(realm, REALM, "Kerberos realm");
    if (port < 1 || port > 65535) {
      throw new IllegalArgumentException("port must be between 1 and 65535");
    }
    return "jdbc:hive2://" + hostName + ":" + port + "/" + database
        + ";principal=hive/_HOST@" + realm + ";ssl=" + ssl;
  }

  public boolean executeQueryStatement(Connection connection, String query)
      throws SQLException {
    requireQuery(connection, query);
    try (Statement statement = connection.createStatement()) {
      return statement.execute(query);
    }
  }

  public boolean executeQueryPreparedStatement(Connection connection, String query)
      throws SQLException {
    requireQuery(connection, query);
    try (PreparedStatement statement = connection.prepareStatement(query)) {
      return statement.execute();
    }
  }

  private static Connection openConnection(String jdbcUrl)
      throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    return DriverManager.getConnection(jdbcUrl);
  }

  private static void requireQuery(Connection connection, String query) {
    Objects.requireNonNull(connection, "connection");
    if (query == null || query.isBlank()) {
      throw new IllegalArgumentException("query must not be blank");
    }
  }

  private static String requireMatch(String value, Pattern pattern, String label) {
    if (value == null || !pattern.matcher(value).matches()) {
      throw new IllegalArgumentException(label + " contains unsupported characters");
    }
    return value;
  }

  @FunctionalInterface
  interface JdbcConnector {
    Connection connect(String jdbcUrl) throws ClassNotFoundException, SQLException;
  }
}
