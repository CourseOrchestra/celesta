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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.python.core.PyException;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PySystemState;
import org.python.core.codecs;
import org.python.util.PythonInterpreter;

import ru.curs.celesta.dbutils.DBUpdator;
import ru.curs.celesta.dbutils.SessionLogManager;
import ru.curs.celesta.ormcompiler.ORMCompiler;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;

/**
 * Корневой класс приложения.
 */
public final class Celesta {

	private static final String CELESTA_IS_ALREADY_INITIALIZED = "Celesta is already initialized.";
	private static final String CELESTA_IS_NOT_INITIALIZED = "Celesta is not initialized, use "
			+ "one of 'initialize' methods instead.";
	private static final String FILE_PROPERTIES = "celesta.properties";
	private static final Pattern PROCNAME = Pattern
			.compile("\\s*([A-Za-z][A-Za-z0-9]*)((\\.[A-Za-z_]\\w*)+)\\.([A-Za-z_]\\w*)\\s*");

	private static Celesta theCelesta;
	private final Score score;
	private List<String> pyPathList;
	private final Queue<PythonInterpreter> interpreterPool = new LinkedList<>();
	private final Map<String, SessionContext> sessions = Collections
			.synchronizedMap(new HashMap<String, SessionContext>());

	private Celesta() throws CelestaException {
		// CELESTA STARTUP SEQUENCE
		// 1. Разбор описания гранул.
		System.out.print("Celesta initialization: phase 1/3 score parsing...");
		score = new Score(AppSettings.getScorePath());
		System.out.println("done.");

		// 2. Перекомпиляция ORM-модулей, где это необходимо.
		System.out
				.print("Celesta initialization: phase 2/3 data access classes compiling...");
		ORMCompiler.compile(score);
		System.out.println("done.");

		// 3. Обновление структуры базы данных.
		// Т. к. на данном этапе уже используется метаинформация, то theCelesta
		// необходимо проинициализировать.
		theCelesta = this;

		if (!AppSettings.getSkipDBUpdate()) {
			System.out
					.print("Celesta initialization: phase 3/3 database upgrade...");
			DBUpdator.updateDB(score);
			System.out.println("done.");
		} else {
			System.out
					.println("Celesta initialization: phase 3/3 database upgrade...skipped.");
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
		try {
			initialize();
		} catch (CelestaException e) {
			System.out
					.println("The following problems occured during initialization process:");
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
				String sesId = String.format("TEMP%08X",
						(new Random()).nextInt());
				getInstance().login(sesId, userId);
				getInstance().runPython(sesId, args[1], params);
				getInstance().logout(sesId, false);

			} catch (CelestaException e) {
				System.out
						.println("The following problems occured while trying to execute "
								+ args[1] + ":");
				System.out.println(e.getMessage());
			}
	}

	private synchronized PythonInterpreter getPythonInterpreter()
			throws CelestaException {
		// Для начала, пытаемся достать готовый интерпретатор из пула.
		PythonInterpreter interp = interpreterPool.poll();
		if (interp != null)
			return interp;

		initPyPathList();

		PySystemState state = new PySystemState();
		for (String path : pyPathList)
			state.path.append(new PyString(path));
		interp = new PythonInterpreter(null, state);
		codecs.setDefaultEncoding("UTF-8");

		SessionContext scontext = new SessionContext("super", "celesta_init");
		Connection conn = ConnectionPool.get();
		CallContext context = new CallContext(conn, scontext);
		try {
			interp.set("_ic", context);
			interp.exec("import sys");
			interp.exec("sys.modules['initcontext'] = lambda: _ic");
			// Инициализация пакетов гранул
			for (Grain g : theCelesta.getScore().getGrains().values())
				if (!"celesta".equals(g.getName())) {
					String line = String.format("import %s", g.getName());
					interp.exec(line);
				}

		} catch (Throwable e) {
			System.out.println("Python interpreter initialization error:");
			e.printStackTrace(System.out);
			throw e;
		} finally {
			context.closeCursors();
			ConnectionPool.putBack(conn);
		}
		interp.set("_ic",
				"You can't use initcontext() outside the initialization code!");
		return interp;
	}

	private synchronized void returnPythonInterpreter(PythonInterpreter interp) {
		interpreterPool.add(interp);
	}

	/**
	 * Очищает пул интерпретаторов Python.
	 */
	public synchronized void clearInterpretersPool() {
		interpreterPool.clear();
	}

	private void initPyPathList() {
		if (pyPathList == null) {
			pyPathList = new ArrayList<String>();
			File pathEntry = new File(getMyPath() + "pylib");
			if (pathEntry.exists() && pathEntry.isDirectory()) {
				pyPathList.add(pathEntry.getAbsolutePath());
			}
			pathEntry = new File(AppSettings.getPylibPath());
			if (pathEntry.exists() && pathEntry.isDirectory()) {
				pyPathList.add(pathEntry.getAbsolutePath());
			}
			for (String entry : AppSettings.getScorePath().split(File.pathSeparator)) {
				pathEntry = new File(entry.trim());
				if (pathEntry.exists() && pathEntry.isDirectory()) {
					pyPathList.add(pathEntry.getAbsolutePath());
				}
			}
		}
	}

	/**
	 * Связывает идентификатор сессии и идентификатор пользователя.
	 * 
	 * @param sessionId
	 *            Имя сессии.
	 * @param userId
	 *            Имя пользователя.
	 * @throws CelestaException
	 *             Сбой взаимодействия с базой данных.
	 */
	public void login(String sessionId, String userId) throws CelestaException {
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
			SessionLogManager.logLogin(session);
		}
	}

	/**
	 * Фиксирует (при наличии включённой настройки log.logins) неудачный логин.
	 * 
	 * @param userId
	 *            Имя пользователя, под которым производился вход.
	 * @throws CelestaException
	 *             Ошибка работы с базой данных.
	 */
	public void failedLogin(String userId) throws CelestaException {
		SessionLogManager.logFailedLogin(userId);
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
	public void logout(String sessionId, boolean timeout)
			throws CelestaException {
		SessionContext sc = sessions.remove(sessionId);
		if (sc != null) {
			// Очищаем сессионные данные, дабы облегчить работу сборщика мусора.
			sc.getData().clear();
			SessionLogManager.logLogout(sc, timeout);
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
	public PyObject runPython(String sesId, String proc, Object... param)
			throws CelestaException {
		return runPython(sesId, null, proc, param);

	}

	/**
	 * Запуск питоновской процедуры.
	 * 
	 * @param sesId
	 *            идентификатор пользователя, от имени которого производится
	 *            изменение
	 * @param rec
	 *            приёмник сообщений.
	 * @param proc
	 *            Имя процедуры в формате <grain>.<module>.<proc>
	 * @param param
	 *            Параметры для передачи процедуры.
	 * @return PyObject
	 * @throws CelestaException
	 *             В случае, если процедура не найдена или в случае ошибки
	 *             выполненения процедуры.
	 */
	public PyObject runPython(String sesId, CelestaMessage.MessageReceiver rec,
			String proc, Object... param) throws CelestaException {
		Matcher m = PROCNAME.matcher(proc);

		if (m.matches()) {
			String grainName = m.group(1);
			String unitName = m.group(2);
			String procName = m.group(4);

			Grain grain;
			try {
				grain = getScore().getGrain(grainName);
			} catch (ParseException e) {
				throw new CelestaException(
						"Invalid procedure name: %s, grain %s is unknown for the system.",
						proc, grainName);
			}

			StringBuilder sb = new StringBuilder("context");
			for (int i = 0; i < param.length; i++)
				sb.append(String.format(", arg%d", i));

			SessionContext sesContext = sessions.get(sesId);
			if (sesContext == null)
				throw new CelestaException("Session ID=%s is not logged in",
						sesId);
			sesContext.setMessageReceiver(rec);

			Connection conn = ConnectionPool.get();
			CallContext context = new CallContext(conn, sesContext, grain);
			PythonInterpreter interp = getPythonInterpreter();
			try {
				interp.set("context", context);
				for (int i = 0; i < param.length; i++)
					interp.set(String.format("arg%d", i), param[i]);

				try {
					String line = String.format("import %s%s", grainName,
							unitName);
					interp.exec(line);
					line = String.format("%s%s.%s(%s)", grainName, unitName,
							procName, sb.toString());
					PyObject pyObj = interp.eval(line);
					return pyObj;
				} catch (PyException e) {
					String sqlErr = "";
					try {
						// Ошибка базы данных!
						conn.rollback();
					} catch (SQLException e1) {
						// Если связь с базой развалилась, об этом тоже сообщим
						// пользователю.
						sqlErr = ". SQL error:" + e1.getMessage();
					}
					StringWriter sw = new StringWriter();
					e.fillInStackTrace().printStackTrace(new PrintWriter(sw));
					throw new CelestaException(String.format(
							"Python error: %s:%s%n%s%n%s", e.type, e.value,
							sw.toString(), sqlErr));
				}
			} finally {
				context.closeCursors();
				returnPythonInterpreter(interp);
				ConnectionPool.putBack(conn);
			}

		} else {
			throw new CelestaException(
					"Invalid procedure name: %s, should match pattern <grain>.(<module>.)...<proc>, "
							+ "note that grain name should not contain underscores.",
					proc);
		}
	}

	/**
	 * Инициализация с использованием явно передаваемых свойств (метод необходим
	 * для автоматизации взаимодействия с другими системами, прежде всего, с
	 * ShowCase). Вызывать один из перегруженных вариантов метода initialize
	 * можно не более одного раза.
	 * 
	 * @param settings
	 *            свойства, которые следует применить при инициализации --
	 *            эквивалент файла celesta.properties.
	 * 
	 * @throws CelestaException
	 *             в случае ошибки при инициализации.
	 */
	public static synchronized void initialize(Properties settings)
			throws CelestaException {
		initialize(settings, true);
	}

	private static synchronized void initialize(Properties settings,
			boolean initPython) throws CelestaException {
		if (theCelesta != null)
			throw new CelestaException(CELESTA_IS_ALREADY_INITIALIZED);

		System.out
				.print("Celesta pre-initialization: phase 1/2 system settings reading...");
		AppSettings.init(settings);
		System.out.println("done.");

		// Инициализация ClassLoader для нужд Jython-интерпретатора
		System.out
				.print("Celesta pre-initialization: phase 2/2 Jython initialization...");
		initCL();
		System.out.println("done.");

		new Celesta();

		if (initPython) {
			System.out
					.print("Celesta post-initialization: phase 1/1 first Jython interpreter initialization...");
			theCelesta.returnPythonInterpreter(theCelesta
					.getPythonInterpreter());
			System.out.println("done.");
		}
	}

	private static void initCL() {
		File lib = new File(getMyPath() + "lib");
		if (lib.exists() && lib.isDirectory() && lib.canRead()) {
			// Construct the "class path" for this class loader
			Set<URL> set = new LinkedHashSet<URL>();

			String[] filenames = lib.list();
			for (String filename : filenames) {
				if (!filename.toLowerCase().endsWith(".jar"))
					continue;
				File file = new File(lib, filename);
				URL url;
				try {
					url = file.toURI().toURL();
					set.add(url);
				} catch (MalformedURLException e) {
					// This can't happen
					e.printStackTrace();
				}
			}
			// Construct the class loader itself
			final URL[] array = set.toArray(new URL[set.size()]);
			ClassLoader classLoader = AccessController
					.doPrivileged(new PrivilegedAction<URLClassLoader>() {
						@Override
						public URLClassLoader run() {
							return new URLClassLoader(array);
						}
					});
			Thread.currentThread().setContextClassLoader(classLoader);
		}

		Properties postProperties = new Properties();
		postProperties.setProperty("python.packages.directories",
				"java.ext.dirs,celesta.lib");
		PythonInterpreter.initialize(System.getProperties(), postProperties,
				null);
		// codecs.setDefaultEncoding("UTF-8");
	}

	/**
	 * Инициализация с использованием свойств, находящихся в файле
	 * celesta.properties, лежащем в одной папке с celesta.jar. Вызывать один из
	 * перегруженных вариантов метода initialize можно не более одного раза.
	 * 
	 * @throws CelestaException
	 *             в случае ошибки при инициализации, а также в случае, если
	 *             Celesta уже была проинициализирована.
	 */

	public static synchronized void initialize() throws CelestaException {
		initialize(true);
	}

	private static synchronized void initialize(boolean initPython)
			throws CelestaException {
		if (theCelesta != null)
			throw new CelestaException(CELESTA_IS_ALREADY_INITIALIZED);

		// Разбираемся с настроечным файлом: читаем его и превращаем в
		// Properties.
		Properties settings = new Properties();
		try {
			ClassLoader loader = Thread.currentThread().getContextClassLoader();
			InputStream in = loader.getResourceAsStream(FILE_PROPERTIES);
			if (in == null) {
				String path = getMyPath();
				File f = new File(path + FILE_PROPERTIES);
				if (!f.exists())
					throw new CelestaException(String.format(
							"File %s cannot be found.", f.toString()));
				in = new FileInputStream(f);
			}
			try {
				settings.load(in);
			} finally {
				in.close();
			}
		} catch (IOException e) {
			throw new CelestaException(String.format(
					"IOException while reading settings file: %s",
					e.getMessage()));
		}

		initialize(settings, initPython);
	}

	/**
	 * Производит повторную инициализацию Celesta. Метод необходим для системы
	 * динамического изменения структуры базы данных.
	 * 
	 * @throws CelestaException
	 *             если Celesta не была иницилизирована или в случае ошибки
	 *             инициализации.
	 */
	public static synchronized void reInitialize() throws CelestaException {
		if (theCelesta == null)
			throw new CelestaException(CELESTA_IS_NOT_INITIALIZED);
		Map<String, SessionContext> sessions = theCelesta.sessions;
		theCelesta = null;
		new Celesta();
		theCelesta.sessions.putAll(sessions);
	}

	/**
	 * Возвращает объект-синглетон Celesta. Если до этого объект не был создан,
	 * то сначала инициализирует его.
	 * 
	 * @throws CelestaException
	 *             в случае ошибки при инициализации.
	 */
	public static synchronized Celesta getInstance() throws CelestaException {
		if (theCelesta == null)
			initialize();
		return theCelesta;
	}

	/**
	 * Возвращает объект-синглетон Celesta, при этом не инициализируя один из
	 * питоновских интерпретаторов в пуле. Метод предназначен для использования
	 * в пошаговой отладке.
	 * 
	 * @throws CelestaException
	 *             в случае ошибки при инициализации.
	 */
	public static synchronized Celesta getDebugInstance()
			throws CelestaException {
		if (theCelesta == null)
			initialize(false);
		return theCelesta;
	}

	private static String getMyPath() {
		String path = Celesta.class.getProtectionDomain().getCodeSource()
				.getLocation().getPath();
		File f = new File(path.replace("%20", " "));
		if (f.getAbsolutePath().toLowerCase().endsWith(".jar"))
			return f.getParent() + File.separator;
		else {
			// NB второй вариант возвращает папку выше папки bin, если имеем
			// дело с исходниками, построенными и отлаживаемыми в Eclipse
			// return f.getAbsolutePath() + File.separator;
			return f.getParent() + File.separator;
		}
	}

	/**
	 * Возвращает метаданные Celesta (описание таблиц).
	 */
	public Score getScore() {
		return score;
	}
}
