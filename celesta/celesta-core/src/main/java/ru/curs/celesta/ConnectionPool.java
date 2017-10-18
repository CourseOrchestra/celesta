package ru.curs.celesta;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ru.curs.celesta.dbutils.adaptors.DBAdaptor;

/**
 * Пул соединений с базой данных.
 * 
 */
public final class ConnectionPool implements AutoCloseable {

	private final ConcurrentLinkedQueue<CelestaConnection> pool = new ConcurrentLinkedQueue<>();
	private final String jdbcConnectionUrl;
	private final String driverClassName;
	private final String login;
	private final String password;
	private DBAdaptor dbAdaptor;
	private volatile boolean isClosed;

	public static ConnectionPool create(ConnectionPoolConfiguration configuration) throws CelestaException {
		return new ConnectionPool(configuration.getJdbcConnectionUrl(), configuration.getDriverClassName(),
				configuration.getLogin(), configuration.getPassword());
	}

	private ConnectionPool(String jdbcConnectionUrl, String driverClassName, String login, String password) {
		this.driverClassName = driverClassName;
		this.login = login;
		this.password = password;
		this.jdbcConnectionUrl = jdbcConnectionUrl;

		Runtime.getRuntime().addShutdownHook(new Thread(this::close));
	}

	public void setDbAdaptor(DBAdaptor dbAdaptor) {
		this.dbAdaptor = dbAdaptor;
	}

	/**
	 * Извлекает соединение из пула.
	 * 
	 * @throws CelestaException
	 *             В случае, если новое соединение не удалось создать.
	 */
	public Connection get() throws CelestaException {

		if (isClosed) {
			throw new CelestaException("ConnectionPool is closed");
		}

		// First, we are trying to provide a connection from pool
		Connection c = pool.poll();
		while (c != null) {
			try {
				if (dbAdaptor.isValidConnection(c, 1))
					return c;
			} catch (CelestaException e) {
				// do something to make CheckStyle happy ))
				c = null;
			}
			c = pool.poll();
		}

		try {
			Class.forName(driverClassName);
			if (login.isEmpty()) {
				c = DriverManager.getConnection(jdbcConnectionUrl);
			} else {
				c = DriverManager.getConnection(jdbcConnectionUrl, login, password);
			}
			c.setAutoCommit(false);

			CelestaConnection celestaConnection = new CelestaConnection(c) {
				@Override
				public void close() {
					try {
						if (isClosed) {
							getConnection().close();
						} else {
							commit();
							pool.add(this);
						}
					} catch (SQLException e){
						//ignore everything
						e.printStackTrace();
					}
				}
			};
			return celestaConnection;

		} catch (SQLException | ClassNotFoundException e) {
			throw new CelestaException("Could not connect to %s with error: %s",
					PasswordHider.maskPassword(jdbcConnectionUrl), e.getMessage());
		}

	}

	/**
	 * Выполняет команду commit на коннекшне, не выдавая исключения.
	 * 
	 * @param conn
	 *            соединение для выполнения коммита.
	 */
	public void commit(Connection conn) {
		try {
			if (conn != null)
				conn.commit();
		} catch (SQLException e) {
			// do something to make CheckStyle happy ))
			return;
		}
	}

	/**
	 * Закрывает пул, все его соединения и делает пул недоступным.
	 */
	@Override
	public void close() {
		if (!isClosed) {
			isClosed = true;
			CelestaConnection c;
			while ((c = pool.poll()) != null) {
				c.close();
			}
		}
	}

	
	/**
	 * Возвращает число доступных соединений в пуле.
	 */
	public Object poolSize() {
		return pool.size();
	}
	
	/**
	 * Закрыт ли пул.
	 */
	public boolean isClosed(){
		return isClosed;
	}
}

/**
 * Класс-утилита для сокрытия пароля в строке JDBC-подключения. Скопировано из
 * проекта FormsServer.
 * 
 */
final class PasswordHider {
	// Пароль Oracle всегда между / и @ (и не может содержать @).
	private static final Pattern ORA_PATTERN = Pattern.compile("/[^@]+@");
	// В MS SQL всё продумано и если пароль содержит ;, она меняется на {;}
	private static final Pattern MSSQL_PATTERN = Pattern.compile("(password)=([^{;]|(\\{(;\\})|[^;]?))+(;|$)",
			Pattern.CASE_INSENSITIVE);
	// В POSTGRESQL JDBC-URL не сработает правильно, если пароль содержит &
	private static final Pattern POSTGRESQL_PATTERN = Pattern.compile("(password)=[^&]+(&|$)",
			Pattern.CASE_INSENSITIVE);
	// А это на случай неизвестного науке JDBC-драйвера
	private static final Pattern GENERIC_PATTERN = Pattern.compile("(password)=.+$", Pattern.CASE_INSENSITIVE);

	private PasswordHider() {

	}

	/**
	 * 
	 * Метод, маскирующий пароль в строке JDBC-подключения.
	 * 
	 * @param url
	 *            Строка, содержащая URL JDBC-подключения
	 * 
	 */
	public static String maskPassword(String url) {
		if (url == null)
			return null;
		Matcher m;
		StringBuffer sb = new StringBuffer();
		if (url.toLowerCase().startsWith("jdbc:oracle")) {
			m = ORA_PATTERN.matcher(url);
			while (m.find()) {
				m.appendReplacement(sb, "/*****@");
			}
		} else if (url.toLowerCase().startsWith("jdbc:sqlserver")) {
			m = MSSQL_PATTERN.matcher(url);
			while (m.find()) {
				m.appendReplacement(sb, m.group(1) + "=*****" + m.group(5));
			}
		} else if (url.toLowerCase().startsWith("jdbc:postgresql")) {
			m = POSTGRESQL_PATTERN.matcher(url);
			while (m.find()) {
				m.appendReplacement(sb, m.group(1) + "=*****" + m.group(2));
			}
		} else {
			m = GENERIC_PATTERN.matcher(url);
			while (m.find()) {
				m.appendReplacement(sb, m.group(1) + "=*****");
			}
		}
		m.appendTail(sb);
		return sb.toString();
	}
}
