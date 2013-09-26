package ru.curs.celesta;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.python.core.PyException;
import org.python.core.codecs;
import org.python.util.PythonInterpreter;

import ru.curs.celesta.dbutils.DBUpdator;
import ru.curs.celesta.ormcompiler.ORMCompiler;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;

/**
 * Корневой класс приложения.
 */
public final class Celesta {

	private static final String CELESTA_IS_ALREADY_INITIALIZED = "Celesta is already initialized.";
	private static final Pattern PROCNAME = Pattern
			.compile("([A-Za-z_][A-Za-z_0-9]*)\\.([A-Za-z_][A-Za-z_0-9]*)\\.([A-Za-z_][A-Za-z_0-9]*)");

	private static Celesta theCelesta;
	private Score score;

	private Celesta() throws CelestaException {
		// CELESTA STARTUP SEQUENCE
		// 1. Разбор описания гранул.
		score = new Score(AppSettings.getScorePath());

		// 2. Перекомпиляция ORM-модулей, где это необходимо.
		ORMCompiler.compile(score);

		// 3. Обновление структуры базы данных.
		// Т. к. на данном этапе уже используется метаинформация, то theCelesta
		// необходимо проинициализировать.
		theCelesta = this;
		DBUpdator.updateDB(score);
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

				getInstance().runPython(args[0], args[1], params);
			} catch (CelestaException e) {
				System.out
						.println("The following problems occured while trying to execute "
								+ args[1] + ":");
				System.out.println(e.getMessage());
			}
	}

	/**
	 * Запуск питоновской процедуры.
	 * 
	 * @param userId
	 *            идентификатор пользователя, от имени которого производится
	 *            изменение
	 * 
	 * @param proc
	 *            Имя процедуры в формате <grain>.<module>.<proc>
	 * @param param
	 *            Параметры для передачи процедуры.
	 * @throws CelestaException
	 *             В случае, если процедура не найдена или в случае ошибки
	 *             выполненения процедуры.
	 */
	public void runPython(String userId, String proc, Object... param)
			throws CelestaException {
		Matcher m = PROCNAME.matcher(proc);

		if (m.find()) {
			String grainName = m.group(1);
			String unitName = m.group(2);
			String procName = m.group(3);

			try {
				getScore().getGrain(grainName);
			} catch (ParseException e) {
				throw new CelestaException(
						"Invalid procedure name: %s, grain %s is unknown for the system.",
						proc, grainName);
			}

			StringBuilder sb = new StringBuilder("context");
			for (int i = 0; i < param.length; i++)
				sb.append(String.format(", arg%d", i));

			PythonInterpreter interp = new PythonInterpreter();
			Connection conn = ConnectionPool.get();
			CallContext context = new CallContext(conn, userId);
			try {
				interp.set("context", context);
				for (int i = 0; i < param.length; i++)
					interp.set(String.format("arg%d", i), param[i]);

				try {
					String line = String.format("from %s import %s as %s",
							grainName, unitName, unitName);
					interp.exec(line);
					line = String.format("%s.%s(%s)", unitName, procName,
							sb.toString());
					interp.exec(line);
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
					throw new CelestaException(String.format(
							"Python error: %s:%s%s", e.type, e.value, sqlErr));
				}
			} finally {
				ConnectionPool.putBack(conn);
			}

		} else {
			throw new CelestaException(
					"Invalid procedure name: %s, should match pattern <grain>.<module>.<proc>.",
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
		if (theCelesta != null)
			throw new CelestaException(CELESTA_IS_ALREADY_INITIALIZED);

		AppSettings.init(settings);

		// Инициализация Jython
		initCL(getMyPath());

		new Celesta();
	}

	private static void initCL(String path) {

		File lib = new File(path + "lib");

		// Construct the "class path" for this class loader
		Set<URL> set = new LinkedHashSet<URL>();
		if (lib.isDirectory() && lib.exists() && lib.canRead()) {
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

		String libfolder = lib.toString();
		Properties postProperties = new Properties();
		postProperties.setProperty("python.packages.directories",
				"java.ext.dirs,celesta.lib");
		postProperties.setProperty("celesta.lib", libfolder);
		StringBuilder sb = new StringBuilder(path + "pylib");
		for (String entry : AppSettings.getScorePath().split(";")) {
			File pathEntry = new File(entry);
			if (pathEntry.exists() && pathEntry.isDirectory()) {
				sb.append(";");
				sb.append(pathEntry.getAbsolutePath());
			}
		}
		postProperties.setProperty("python.path", sb.toString());
		PythonInterpreter.initialize(System.getProperties(), postProperties,
				null);

		codecs.setDefaultEncoding("UTF-8");
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
		if (theCelesta != null)
			throw new CelestaException(CELESTA_IS_ALREADY_INITIALIZED);

		// Разбираемся с настроечным файлом: читаем его и превращаем в
		// Properties.
		String path = getMyPath();
		File f = new File(path + "celesta.properties");
		if (!f.exists())
			throw new CelestaException(String.format(
					"File %s cannot be found.", f.toString()));
		Properties settings = new Properties();
		try {
			FileInputStream in = new FileInputStream(f);
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

		initialize(settings);
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
