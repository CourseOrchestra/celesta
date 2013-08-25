package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.VersionString;

/**
 * Класс, выполняющий процедуру обновления базы данных.
 * 
 */
public final class DBUpdator {

	private static DBAdaptor dba;
	private static GrainsCursor c;

	private static final Comparator<Grain> GRAIN_COMPARATOR = new Comparator<Grain>() {
		@Override
		public int compare(Grain o1, Grain o2) {
			return o1.getDependencyOrder() - o2.getDependencyOrder();
		}
	};

	private DBUpdator() {
	}

	/**
	 * Буфер для хранения информации о грануле.
	 */
	private static class GrainInfo {
		private boolean recover;
		private int length;
		private int checksum;
		private VersionString version;
	}

	/**
	 * Выполняет обновление структуры БД на основе разобранной объектной модели.
	 * 
	 * @param score
	 *            модель
	 * @throws CelestaException
	 *             в случае ошибки обновления.
	 */
	public static void updateDB(Score score) throws CelestaException {
		if (dba == null)
			dba = DBAdaptor.getAdaptor();
		Connection conn = ConnectionPool.get();
		try {
			c = new GrainsCursor(conn);

			// Проверяем наличие главной системной таблицы.
			if (!dba.tableExists("celesta", "grains")) {
				// Если главной таблицы нет, а другие таблицы есть -- ошибка.
				if (dba.userTablesExist())
					throw new CelestaException(
							"No celesta.grains table found in non-empty database.");
				// Если база вообще пустая, то создаём системные таблицы.
				try {
					Grain sys = score.getGrain("celesta");
					dba.createSchemaIfNotExists("celesta");
					dba.createTable(sys.getTable("grains"));
					insertGrainRec(sys);
					updateGrain(sys);
				} catch (ParseException e) {
					throw new CelestaException(
							"No 'celesta' grain definition found.");
				}
			}

			// Теперь собираем в память информацию о гранулах на основании того,
			// что
			// хранится в таблице grains.
			Map<String, GrainInfo> dbGrains = new HashMap<>();
			while (c.next()) {
				if (!(c.getState() == GrainsCursor.READY || c.getState() == GrainsCursor.RECOVER))
					throw new CelestaException(
							"Cannot proceed with database upgrade: there are grains "
									+ "not in 'ready' or 'recover' state.");
				GrainInfo gi = new GrainInfo();
				gi.checksum = c.getChecksum();
				gi.length = c.getLength();
				gi.recover = c.getState() == GrainsCursor.RECOVER;
				try {
					gi.version = new VersionString(c.getVersion());
				} catch (ParseException e) {
					throw new CelestaException(String.format(
							"Error while scanning celesta.grains table: %s",
							e.getMessage()));
				}
				dbGrains.put(c.getId(), gi);
			}

			// Получаем список гранул на основе метамодели и сортируем его по
			// порядку зависимости.
			List<Grain> grains = new ArrayList<>(score.getGrains().values());
			Collections.sort(grains, GRAIN_COMPARATOR);

			// Выполняем итерацию по гранулам.
			for (Grain g : grains) {
				// Запись о грануле есть?
				GrainInfo gi = dbGrains.get(g.getName());
				if (gi == null) {
					insertGrainRec(g);
					updateGrain(g);
				} else {
					// Запись есть -- решение об апгрейде принимается на основе
					// версии и контрольной суммы.
					decideToUpgrade(g, gi);
				}
			}
		} finally {
			ConnectionPool.putBack(conn);
		}
	}

	private static void insertGrainRec(Grain g) throws CelestaException {
		c.init();
		c.setId(g.getName());
		c.setVersion(g.getVersion().toString());
		c.setLength(g.getLength());
		c.setChecksum(g.getChecksum());
		c.setState(GrainsCursor.RECOVER);
		c.setLastmodified(new Date());
		c.setMessage("");
		c.insert();
	}

	private static void decideToUpgrade(Grain g, GrainInfo gi)
			throws CelestaException {
		if (gi.recover) {
			updateGrain(g);
			return;
		}

		// Как соотносятся версии?
		switch (g.getVersion().compareTo(gi.version)) {
		case LOWER:
			// Старая версия -- не апгрейдим, ошибка.
			throw new CelestaException(
					"Grain '%s' version '%s' is lower than database "
							+ "grain version '%s'. Will not proceed with auto-upgrade.",
					g.getName(), g.getVersion().toString(), gi.version
							.toString());
		case INCONSISTENT:
			// Непонятная (несовместимая) версия -- не апгрейдим,
			// ошибка.
			throw new CelestaException(
					"Grain '%s' version '%s' is inconsistent with database "
							+ "grain version '%s'. Will not proceed with auto-upgrade.",
					g.getName(), g.getVersion().toString(), gi.version
							.toString());
		case GREATER:
			// Версия выросла -- апгрейдим.
			updateGrain(g);
			break;
		case EQUALS:
			// Версия не изменилась: апгрейдим лишь в том случае, если
			// изменилась контрольная сумма.
			if (gi.length != g.getLength() || gi.checksum != g.getChecksum())
				updateGrain(g);
			break;
		default:
			break;
		}
	}

	/**
	 * Выполняет обновление на уровне отдельной гранулы.
	 * 
	 * @param g
	 *            Гранула.
	 * @throws CelestaException
	 *             в случае ошибки обновления.
	 */
	private static void updateGrain(Grain g) throws CelestaException {
		// выставление в статус updating
		c.get(g.getName());
		c.setState(GrainsCursor.UPGRADING);
		c.update();
		ConnectionPool.commit(c.getConnection());
		// теперь собственно обновление гранулы
		try {
			// Схему создаём, если ещё не создана.
			dba.createSchemaIfNotExists(g.getName());
			// Обновляем все таблицы.
			for (Table t : g.getTables().values())
				updateTable(t);

			// Обновляем все индексы.
			Set<String> dbIndices = dba.getIndices(c.getConnection(), g);
			Map<String, Index> myIndices = g.getIndices();
			// Начинаем с удаления ненужных
			for (String indexName : dbIndices)
				if (!myIndices.containsKey(indexName))
					dba.dropIndex(g, indexName);
			for (Entry<String, Index> e : myIndices.entrySet()) {
				if (dbIndices.contains(e.getKey())) {
					// TODO БД содержит индекс с таким именем, надо проверить
					// поля и пересоздавать индекс лишь в случае необходимости!!
					System.out.println("Implement index check here.");
				} else {
					// Создаём не существовавший ранее индекс.
					dba.createIndex(e.getValue());
				}
			}

			// По завершении -- обновление номера версии, контрольной суммы
			// и выставление в статус ready
			c.setState(GrainsCursor.READY);
			c.setChecksum(g.getChecksum());
			c.setLength(g.getLength());
			c.setLastmodified(new Date());
			c.setMessage("");
			c.setVersion(g.getVersion().toString());
			c.update();
		} catch (CelestaException e) {
			// Если что-то пошло не так
			c.setState(GrainsCursor.ERROR);
			c.setMessage(String.format(
					"Error while trying to update to version %s: %s", g
							.getVersion().toString(), e.getMessage()));
			c.update();
		}
	}

	private static void updateTable(Table t) throws CelestaException {
		if (dba.tableExists(t.getGrain().getName(), t.getName())) {
			Set<String> dbColumns = dba.getColumns(c.getConnection(), t);

			for (Entry<String, Column> e : t.getColumns().entrySet()) {
				if (dbColumns.contains(e.getKey())) {
					// TODO БД содержит колонку с таким именем, надо проверить
					// все её атрибуты и при необходимости и возможности --
					// обновить.
					System.out.println("Implement column check here.");
				} else {
					dba.createColumn(c.getConnection(), e.getValue());
				}
			}
		} else {
			dba.createTable(t);
		}
		// TODO обновление внешних ключей
	}

}
