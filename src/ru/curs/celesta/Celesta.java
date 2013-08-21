package ru.curs.celesta;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import ru.curs.celesta.dbutils.DBUpdator;
import ru.curs.celesta.ormcompiler.ORMCompiler;
import ru.curs.celesta.score.Score;

/**
 * Корневой класс приложения.
 */
public final class Celesta {

	private static final String CELESTA_IS_ALREADY_INITIALIZED = "Celesta is already initialized.";
	private static Celesta theCelesta;
	private Score score;

	private Celesta() throws CelestaCritical {
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
		} catch (CelestaCritical e) {
			System.out
					.println("The following problems occured during initialization process:");
			System.out.println(e.getMessage());
			System.exit(1);
		}
		System.out.println("Celesta initialized successfully.");
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
	 * @throws CelestaCritical
	 *             в случае ошибки при инициализации.
	 */

	public static synchronized void initialize(Properties settings)
			throws CelestaCritical {
		if (theCelesta != null)
			throw new CelestaCritical(CELESTA_IS_ALREADY_INITIALIZED);

		AppSettings.init(settings);

		new Celesta();
	}

	/**
	 * Инициализация с использованием свойств, находящихся в файле
	 * celesta.properties, лежащем в одной папке с celesta.jar. Вызывать один из
	 * перегруженных вариантов метода initialize можно не более одного раза.
	 * 
	 * @throws CelestaCritical
	 *             в случае ошибки при инициализации, а также в случае, если
	 *             Celesta уже была проинициализирована.
	 */
	public static synchronized void initialize() throws CelestaCritical {
		if (theCelesta != null)
			throw new CelestaCritical(CELESTA_IS_ALREADY_INITIALIZED);

		// Разбираемся с настроечным файлом: читаем его и превращаем в
		// Properties.
		String path = getMyPath();
		File f = new File(path + "celesta.properties");
		if (!f.exists())
			throw new CelestaCritical(String.format("File %s cannot be found.",
					f.toString()));
		Properties settings = new Properties();
		try {
			FileInputStream in = new FileInputStream(f);
			try {
				settings.load(in);
			} finally {
				in.close();
			}
		} catch (IOException e) {
			throw new CelestaCritical(String.format(
					"IOException while reading settings file: %s",
					e.getMessage()));
		}

		initialize(settings);
	}

	/**
	 * Возвращает объект-синглетон Celesta. Если до этого объект не был создан,
	 * то сначала инициализирует его.
	 * 
	 * @throws CelestaCritical
	 *             в случае ошибки при инициализации.
	 */
	public static synchronized Celesta getInstance() throws CelestaCritical {
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
			return f.getAbsolutePath() + File.separator;
		}
	}

	/**
	 * Возвращает метаданные Celesta (описание таблиц).
	 */
	public Score getScore() {
		return score;
	}
}
