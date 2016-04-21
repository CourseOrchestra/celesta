package ru.curs.celesta.dbschemasync;

import java.io.File;
import java.util.Properties;

import ru.curs.celesta.AppSettings;
import ru.curs.celesta.score.Score;

/**
 * Класс синхронизации с проектом DBSchema.
 * 
 */
public final class DBSchemaSync {

	private DBSchemaSync() {

	}

	/**
	 * Главный метод.
	 * 
	 * @param args
	 *            аргументы.
	 * @throws Exception
	 *             Если что-то пошло не так.
	 */
	public static void main(String[] args) throws Exception {
		// "c:/Users/Ivan/workspace/celesta/score/"
		// "c:/Users/Ivan/Desktop/test2.dbs"
		//[TODO: remove this stub
		Properties settings = new Properties();
		settings.setProperty("score.path", ".");
		settings.setProperty("pylib.path", ".");
		settings.setProperty("rdbms.connection.url", "jdbc:sqlserver://");
		//this is why all this stuff is here
		settings.setProperty("allow.indexed.nulls", "true");
		AppSettings.init(settings);
		//TODO]
		
		if (args.length < 2)
			throw new Exception(
					"There should be two arguments: score path and .dbs-file.");

		if (args[0].trim().toLowerCase().endsWith(".dbs")) {
			System.out.println("DBS-->SCORE");
			File dbsFile = new File(args[0]);
			System.out.println("parsing score...");
			Score s = new Score(args[1]);
			System.out.println("processing...");
			DBSchema2Celesta.dBSToScore(dbsFile, s);
			System.out.println("done.");
		} else {
			File dbsFile = new File(args[1]);
			System.out.println("SCORE-->DBS");
			System.out.println("parsing score...");
			Score s = new Score(args[0]);
			System.out.println("processing...");
			Celesta2DBSchema.scoreToDBS(s, dbsFile);
			System.out.println("done.");
		}
	}
}
