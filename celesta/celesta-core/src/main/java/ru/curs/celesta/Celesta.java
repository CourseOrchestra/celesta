/*
   (с) 2013 ООО "КУРС-ИТ"  

   Этот файл — часть КУРС:Celesta.
   
   КУРС:Celesta — свободная программа: вы можете перераспространять ее и/или изменять
   ее на условиях Стандартной общественной лицензии GNU в том виде, в каком
   она была опубликована Фондом свободного программного обеспечения; либо
   версии 3 лицензии, либо (по вашему выбору) любой более поздней версии.

   Эта программа распространяется в надежде, что она будет полезной,
   но БЕЗО ВСЯКИХ ГАРАНТИЙ; даже без неявной гарантии ТОВАРНОГО ВИДА
   или ПРИГОДНОСТИ ДЛЯ ОПРЕДЕЛЕННЫХ ЦЕЛЕЙ. Подробнее см. в Стандартной
   общественной лицензии GNU.

   Вы должны были получить копию Стандартной общественной лицензии GNU
   вместе с этой программой. Если это не так, см. http://www.gnu.org/licenses/.

   
   Copyright 2013, COURSE-IT Ltd.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see http://www.gnu.org/licenses/.

 */
package ru.curs.celesta;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.python.core.PyException;
import org.python.core.PyObject;
import org.python.util.PythonInterpreter;

import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.adaptors.configuration.DbAdaptorBuilder;
import ru.curs.celesta.event.TriggerDispatcher;
import ru.curs.celesta.ormcompiler.ORMCompiler;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;

/**
 * Корневой класс приложения.
 */
public final class Celesta implements AutoCloseable {
	private static final String FILE_PROPERTIES = "celesta.properties";
	private static final Pattern PROCNAME = Pattern
			.compile("\\s*([A-Za-z][A-Za-z0-9]*)((\\.[A-Za-z_]\\w*)+)\\.([A-Za-z_]\\w*)\\s*");

	private static final TriggerDispatcher triggerDispatcher = new TriggerDispatcher();

	private final Score score;
	private final DBAdaptor dbAdaptor;
	private final AppSettings appSettings;
	final ConnectionPool connectionPool;
	private final PythonInterpreterPool interpreterPool;
	private final Optional<SessionLogManager> sessionLogManager;
	private final LoggingManager loggingManager;
	private final PermissionManager permissionManager;
	private final ConcurrentHashMap<String, SessionContext> sessions = new ConcurrentHashMap<>();
	private final Set<CallContext> contexts = Collections.synchronizedSet(new LinkedHashSet<CallContext>());

	private final ProfilingManager profiler;

	private Celesta(AppSettings appSettings, boolean initInterpeterPool) throws CelestaException {
		CurrentCelesta.set(this);

		this.appSettings = appSettings;
		// CELESTA STARTUP SEQUENCE
		// 1. Разбор описания гранул.
		System.out.print("Celesta initialization: phase 1/4 score parsing...");
		score = new Score(appSettings.getScorePath());
		System.out.println("done.");

		// 2. Перекомпиляция ORM-модулей, где это необходимо.
		System.out.print("Celesta initialization: phase 2/4 data access classes compiling...");
		ORMCompiler.compile(score);
		System.out.println("done.");

		// 3. Обновление структуры базы данных.
		// Т. к. на данном этапе уже используется метаинформация, то theCelesta и ConnectionPool
		// необходимо проинициализировать.

		ConnectionPoolConfiguration cpc = new ConnectionPoolConfiguration();
		cpc.setJdbcConnectionUrl(appSettings.getDatabaseConnection());
		cpc.setDriverClassName(appSettings.getDbClassName());
		cpc.setLogin(appSettings.getDBLogin());
		cpc.setPassword(appSettings.getDBPassword());
		connectionPool = ConnectionPool.create(cpc);

		DbAdaptorBuilder dac = new DbAdaptorBuilder()
				.setDbType(appSettings.getDBType())
				.setConnectionPool(connectionPool)
				.setH2ReferentialIntegrity(appSettings.isH2ReferentialIntegrity());

		dbAdaptor = dac.createDbAdaptor();

		this.sessionLogManager = appSettings.getLogLogins()
				? Optional.of(new SessionLogManager(this, appSettings.getLogLogins()))
				: Optional.empty();
		this.loggingManager = new LoggingManager(dbAdaptor);
		this.permissionManager = new PermissionManager(dbAdaptor);
		this.profiler = new ProfilingManager(dbAdaptor);

		if (!appSettings.getSkipDBUpdate()) {
			System.out.print("Celesta initialization: phase 3/4 database upgrade...");

			DbUpdater dbUpdater = new DbUpdaterBuilder()
					.dbAdaptor(dbAdaptor)
					.connectionPool(connectionPool)
					.score(score)
					.forceDdInitialize(appSettings.getForceDBInitialize())
					.setPermissionManager(permissionManager)
					.setLoggingManager(loggingManager)
					.build();

			dbUpdater.updateDb();
			System.out.println("done.");
		} else {
			System.out.println("Celesta initialization: phase 3/4 database upgrade...skipped.");
		}

		if (initInterpeterPool) {
			System.out.print("Celesta initialization: phase 4/4 Jython interpreters pool initialization...");

			InterpreterPoolConfiguration ipc = new InterpreterPoolConfiguration()
					.setCelesta(this)
					.setScore(score)
					.setJavaLibPath(appSettings.getJavalibPath())
					.setScriptLibPath(appSettings.getPylibPath());
			interpreterPool = InterpreterPoolFactory.create(ipc);

			System.out.println("done.");
		} else {
			interpreterPool = null;
			System.out.println("Celesta initialization: phase 4/4 Jython interpreters pool initialization...skipped.");
		}
	}

	/**
	 * Метод для запуска Celesta из командной строки (для выполнения
	 * перекомпиляции и обновления базы данных).
	 * 
	 * @param args
	 *            аргументы командной строки.
	 */
	public static void main(String[] args) {
		System.out.println();
		Celesta celesta = null;
		try {
			Properties properties = loadPropertiesDynamically();
			celesta = createInstance(properties);
		} catch (CelestaException e) {
			System.out.println("The following problems occured during initialization process:");
			System.out.println(e.getMessage());
			System.exit(1);
		}
		System.out.println("Celesta initialized successfully.");

		if (args.length > 1)
			try {
				Object[] params = new String[args.length - 2];
				for (int i = 2; i < args.length; i++)
					params[i - 2] = args[i];

				String userId = args[0];
				String sesId = String.format("TEMP%08X", (new Random()).nextInt());
				// getInstance().setProfilemode(true);
				celesta.login(sesId, userId);
				celesta.runPython(sesId, args[1], params);
				celesta.logout(sesId, false);
				celesta.interpreterPool.cancelSourceMonitor();
			} catch (CelestaException e) {
				System.out.println("The following problems occured while trying to execute " + args[1] + ":");
				System.out.println(e.getMessage());
			}
	}

	/**
	 * Returns the set of active (running) call contexts (for monitoring/debug
	 * purposes).
	 */
	public Collection<CallContext> getActiveContexts() {
		return Collections.unmodifiableCollection(contexts);
	}

	/**
	 * Связывает идентификатор сессии и идентификатор пользователя.
	 * 
	 * @param sessionId
	 *            Имя сессии.
	 * @param userId
	 *            Имя пользователя.
	 */
	public void login(String sessionId, String userId) {
		if (sessionId == null)
			throw new IllegalArgumentException("Session id is null.");
		if (userId == null)
			throw new IllegalArgumentException("User id is null.");
		// Создавать новый SessionContext имеет смысл лишь в случае, когда
		// нет старого.
		SessionContext oldSession = sessions.get(sessionId);
		if (oldSession == null || !userId.equals(oldSession.getUserId())) {
			SessionContext session = new SessionContext(userId, sessionId);
			sessions.put(sessionId, session);

			sessionLogManager.ifPresent(s -> s.logLogin(session));
		}
	}

	/**
	 * Фиксирует (при наличии включённой настройки log.logins) неудачный логин.
	 * 
	 * @param userId
	 *            Имя пользователя, под которым производился вход.
	 */
	public void failedLogin(String userId) {
		sessionLogManager.ifPresent(s -> s.logFailedLogin(userId));
	}

	/**
	 * Завершает сессию (удаляет связанные с ней данные).
	 * 
	 * @param sessionId
	 *            имя сессии.
	 * @param timeout
	 *            признак разлогинивания по таймауту.
	 * @throws CelestaException
	 *             Ошибка взаимодействия с БД.
	 * 
	 */
	public void logout(String sessionId, boolean timeout) {
		SessionContext sc = sessions.remove(sessionId);
		if (sc != null) {
			// Очищаем сессионные данные, дабы облегчить работу сборщика мусора.
			sc.getData().clear();
			sessionLogManager.ifPresent(s -> s.logLogout(sc, timeout));
		}
	}

	/**
	 * Запуск питоновской процедуры.
	 * 
	 * @param sesId
	 *            идентификатор пользователя, от имени которого производится
	 *            изменение
	 * @param proc
	 *            Имя процедуры в формате <grain>.<module>.<proc>
	 * @param param
	 *            Параметры для передачи процедуры.
	 * @return PyObject
	 * @throws CelestaException
	 *             В случае, если процедура не найдена или в случае ошибки
	 *             выполненения процедуры.
	 */
	public PyObject runPython(String sesId, String proc, Object... param) throws CelestaException {
		return runPython(sesId, null, null, proc, param);
	}

	/**
	 * Запуск питоновской процедуры.
	 * 
	 * @param sesId
	 *            идентификатор пользователя, от имени которого производится
	 *            изменение
	 * @param rec
	 *            приёмник сообщений.
	 * @param sc
	 *            Контекст Showcase.
	 * @param proc
	 *            Имя процедуры в формате <grain>.<module>.<proc>
	 * @param param
	 *            Параметры для передачи процедуры.
	 * @return PyObject Результат вызова питон-процедуры.
	 * @throws CelestaException
	 *             В случае, если процедура не найдена или в случае ошибки
	 *             выполненения процедуры.
	 */
	public PyObject runPython(String sesId, CelestaMessage.MessageReceiver rec, ShowcaseContext sc, String proc,
			Object... param) throws CelestaException {
		if (interpreterPool == null)
			throw new CelestaException("Interperter pool not initialized. Running in debug mode?");

		Matcher m = PROCNAME.matcher(proc);

		if (m.matches()) {
			String grainName = m.group(1);
			String unitName = m.group(2);
			String procName = m.group(4);

			Grain grain;
			try {
				grain = getScore().getGrain(grainName);
			} catch (ParseException e) {
				throw new CelestaException("Invalid procedure name: %s, grain %s is unknown for the system.", proc,
						grainName);
			}

			StringBuilder sb = new StringBuilder("context");
			for (int i = 0; i < param.length; i++)
				sb.append(String.format(", arg%d", i));

			SessionContext sesContext = sessions.get(sesId);
			if (sesContext == null)
				throw new CelestaException("Session ID=%s is not logged in", sesId);

			sesContext.setMessageReceiver(rec);



			try (PythonInterpreter interp = interpreterPool.getPythonInterpreter();
					 CallContext context = new CallContextBuilder()
							 .setCelesta(this)
							 .setConnectionPool(connectionPool)
							 .setSesContext(sesContext)
							 .setShowcaseContext(sc)
							 .setScore(score)
							 .setCurGrain(grain)
							 .setProcName(proc)
							 .setDbAdaptor(dbAdaptor)
							 .setPermissionManager(permissionManager)
							 .setLoggingManager(loggingManager)
							 .createCallContext()) {
				contexts.add(context);

				try {
					interp.set("context", context);
					for (int i = 0; i < param.length; i++)
						interp.set(String.format("arg%d", i), param[i]);

					String lastPyCmd = "";
					try {
						String line = String.format("import %s%s", grainName, unitName);
						lastPyCmd = line;
						interp.exec(line);
						line = String.format("%s%s.%s(%s)", grainName, unitName, procName, sb.toString());
						lastPyCmd = line;
						PyObject pyObj = interp.eval(line);
						profiler.logCall(context);
						return pyObj;
					} catch (PyException e) {
						String sqlErr = "";
						try {
							// Ошибка базы данных!
							context.rollback();
						} catch (SQLException e1) {
							// Если связь с базой развалилась, об этом тоже сообщим
							// пользователю.
							sqlErr = ". SQL error:" + e1.getMessage();
						}
						StringWriter sw = new StringWriter();
						e.fillInStackTrace().printStackTrace(new PrintWriter(sw));
						throw new CelestaException(String.format("Python error while executing '%s': %s:%s%n%s%n%s",
								lastPyCmd, e.type, e.value, sw.toString(), sqlErr));
					}
				} finally {
					contexts.remove(context);
				}
			} finally {
				sessions.putIfAbsent(sesId, sesContext);
			}

		} else {
			throw new CelestaException("Invalid procedure name: %s, should match pattern <grain>.(<module>.)...<proc>, "
					+ "note that grain name should not contain underscores.", proc);
		}
	}

	public Future<PyObject> runPythonAsync(String sesId, String proc, long delay, Object... param) {

		final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

		SessionContext sessionContext = sessions.get(sesId);

		Callable<PyObject> callable = () -> {
			String currentSesId = null;
			try {

				if (sessions.containsKey(sesId)) {
					currentSesId = sesId;
				} else {
					currentSesId = String.format("TEMP%08X", ThreadLocalRandom.current().nextInt());
					login(currentSesId, sessionContext.getUserId());
				}

				return runPython(currentSesId, proc, param);
			} catch (Exception e) {
				System.out.println("Exception while executing async task:" + e.getMessage());
				throw e;
			} finally {
				scheduledExecutor.shutdown();

				if (currentSesId != null && sessions.containsKey(currentSesId))
					logout(currentSesId, false);
			}
		};

		return scheduledExecutor.schedule(callable, delay, TimeUnit.MILLISECONDS);
	}

	private static void initCL() {
		Set<URL> urlSet = new LinkedHashSet<URL>();

		File lib = new File(getMyPath() + "lib");
		addLibEntry(lib, urlSet);

		// Construct the class loader itself
		if (urlSet.size() > 0) {
			final URL[] array = urlSet.toArray(new URL[urlSet.size()]);
			ClassLoader classLoader = AccessController.doPrivileged(new PrivilegedAction<URLClassLoader>() {
				@Override
				public URLClassLoader run() {
					return new URLClassLoader(array, Thread.currentThread().getContextClassLoader());
				}
			});
			Thread.currentThread().setContextClassLoader(classLoader);
		}
		Properties postProperties = new Properties();
		postProperties.setProperty("python.packages.directories", "java.ext.dirs,celesta.lib");
		postProperties.setProperty("python.console.encoding", "UTF-8");
		PythonInterpreter.initialize(System.getProperties(), postProperties, null);
		// codecs.setDefaultEncoding("UTF-8");
	}

	private static void addLibEntry(File lib, Set<URL> urlSet) {
		if (lib.exists() && lib.isDirectory() && lib.canRead()) {
			// Construct the "class path" for this class loader
			String[] filenames = lib.list();
			for (String filename : filenames) {
				if (!filename.toLowerCase().endsWith(".jar"))
					continue;
				File file = new File(lib, filename);
				URL url;
				try {
					url = file.toURI().toURL();
					urlSet.add(url);
				} catch (MalformedURLException e) {
					// This can't happen
					e.printStackTrace();
				}
			}
		}
	}


	/**
	 * Инициализирует и возвращает новый экземпляр Celesta с проиницилизированным интерпретатором скриптового языка.
	 * Конфигурация извлекается из файла celesta.properties рядом с jar
	 * @return Celesta
	 * @throws CelestaException
	 */
	public static Celesta createInstance() throws CelestaException {
		Properties properties = loadPropertiesDynamically();
		return createInstance(properties);
	}


	/**
	 * Инициализирует и возвращает новый экземпляр Celesta без проиницилизированного интерпретатора скриптового языка
	 * Конфигурация извлекается из файла celesta.properties рядом с jar
	 * Использовать для отладочных целей
	 * @return Celesta
	 * @throws CelestaException
	 */
	public static Celesta createDebugInstance() throws CelestaException {
		Properties properties = loadPropertiesDynamically();
		return createDebugInstance(properties);
	}

	/**
	 * Инициализирует и возвращает новый экземпляр Celesta с проиницилизированным интерпретатором скриптового языка
	 * @param properties настройки
	 * @return Celesta
	 * @throws CelestaException
	 */
	public static Celesta createInstance(Properties properties) throws CelestaException {
		AppSettings appSettings = preInit(properties);
		return new Celesta(appSettings, true);
	}

	/**
	 * Инициализирует и возвращает новый экземпляр Celesta без проиницилизированного интерпретатора скриптового языка
	 * Использовать для отладочных целей
	 * @param properties настройки
	 * @return Celesta
	 * @throws CelestaException
	 */
	public static Celesta createDebugInstance(Properties properties) throws CelestaException {
		AppSettings appSettings = preInit(properties);
		return new Celesta(appSettings, false);
	}

	private static AppSettings preInit(Properties properties) throws CelestaException {
		System.out.print("Celesta pre-initialization: phase 1/2 system settings reading...");
		AppSettings appSettings = new AppSettings(properties);
		System.out.println("done.");

		// Инициализация ClassLoader для нужд Jython-интерпретатора
		System.out.print("Celesta pre-initialization: phase 2/2 Jython initialization...");
		initCL();
		System.out.println("done.");

		return appSettings;
	}

	private static Properties loadPropertiesDynamically() throws CelestaException {
		// Разбираемся с настроечным файлом: читаем его и превращаем в
		// Properties.
		Properties properties = new Properties();
		try {
			ClassLoader loader = Thread.currentThread().getContextClassLoader();
			InputStream in = loader.getResourceAsStream(FILE_PROPERTIES);
			if (in == null) {
				String path = getMyPath();
				File f = new File(path + FILE_PROPERTIES);
				if (!f.exists())
					throw new CelestaException(String.format("File %s cannot be found.", f.toString()));
				in = new FileInputStream(f);
			}
			try {
				properties.load(in);
			} finally {
				in.close();
			}
		} catch (IOException e) {
			throw new CelestaException(String.format("IOException while reading settings file: %s", e.getMessage()));
		}

		return properties;
	}

	static String getMyPath() {

		final String result;

		String path = Celesta.class.getResource(Celesta.class.getSimpleName() + ".class").getPath();
		path = path.replace("%20", " ");

		if (path.contains(".jar")) {
			if (path.startsWith("file:")) {
				path = path.replace("file:", "");
			}
			path = path.substring(0, path.indexOf("jar!"));

			File f = new File(path).getParentFile();
			result = f.getPath() + File.separator;
		} else {
			File f = new File(path).getParentFile();
			result = f.getParent() + File.separator;
		}

		return result;
	}

	/**
	 * Возвращает метаданные Celesta (описание таблиц).
	 */
	public Score getScore() {
		return score;
	}

	/**
	 * Возвращает свойства, с которыми была инициализирована Челеста. Внимание:
	 * данный объект имеет смысл использовать только на чтение, динамическое
	 * изменение этих свойств не приводит ни к чему.
	 */
	public Properties getSetupProperties() {
		return appSettings.getSetupProperties();
	}

	/**
	 * Режим профилирования (записывается ли в таблицу calllog время вызовов
	 * процедур).
	 */
	public boolean isProfilemode() {
		return profiler.isProfilemode();
	}

	/**
	 * Возвращает поведение NULLS FIRST текущей базы данных.
	 * 
	 * @throws CelestaException
	 *             unknown database
	 */
	public boolean nullsFirst() throws CelestaException {
		return dbAdaptor.nullsFirst();
	}

	/**
	 * Устанавливает режим профилирования.
	 * 
	 * @param profilemode
	 *            режим профилирования.
	 */
	public void setProfilemode(boolean profilemode) {
		profiler.setProfilemode(profilemode);
	}

	public static TriggerDispatcher getTriggerDispatcher() {
		return Celesta.triggerDispatcher;
	}


	/**
	 * Инициализирует и возвращает новый CallContext для указанного SessionContext
	 * @param sessionContext
	 * @return CallContext
	 * @throws CelestaException
	 */
	public CallContext callContext(SessionContext sessionContext) throws CelestaException {
		return new CallContextBuilder()
				.setCelesta(this)
				.setConnectionPool(connectionPool)
				.setSesContext(sessionContext)
				.setScore(score)
				.setDbAdaptor(dbAdaptor)
				.setPermissionManager(permissionManager)
				.setLoggingManager(loggingManager)
				.createCallContext();
	}


	/**
	 * Останавливает работу Celesta. После вызова экземпляр Celesta становится непригодным для использования.
	 */
	@Override
	public void close() {
		connectionPool.close();
	}
}
