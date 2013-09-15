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

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.VersionString;
import ru.curs.celesta.syscursors.GrainsCursor;
import ru.curs.celesta.syscursors.TablesCursor;

/**
 * Класс, выполняющий процедуру обновления базы данных.
 * 
 */
public final class DBUpdator {

	private static DBAdaptor dba;
	private static GrainsCursor grain;
	private static TablesCursor table;

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
		CallContext context = new CallContext(conn, Cursor.SYSTEMUSERID);
		try {
			grain = new GrainsCursor(context);
			table = new TablesCursor(context);

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
					dba.createTable(sys.getTable("tables"));
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
			while (grain.next()) {
				if (!(grain.getState() == GrainsCursor.READY || grain
						.getState() == GrainsCursor.RECOVER))
					throw new CelestaException(
							"Cannot proceed with database upgrade: there are grains "
									+ "not in 'ready' or 'recover' state.");
				GrainInfo gi = new GrainInfo();
				gi.checksum = (int) Long.parseLong(grain.getChecksum(), 16);
				gi.length = grain.getLength();
				gi.recover = grain.getState() == GrainsCursor.RECOVER;
				try {
					gi.version = new VersionString(grain.getVersion());
				} catch (ParseException e) {
					throw new CelestaException(String.format(
							"Error while scanning celesta.grains table: %s",
							e.getMessage()));
				}
				dbGrains.put(grain.getId(), gi);
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
		grain.init();
		grain.setId(g.getName());
		grain.setVersion(g.getVersion().toString());
		grain.setLength(g.getLength());
		grain.setChecksum(String.format("%08X", g.getChecksum()));
		grain.setState(GrainsCursor.RECOVER);
		grain.setLastmodified(new Date());
		grain.setMessage("");
		grain.insert();
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
		grain.get(g.getName());
		grain.setState(GrainsCursor.UPGRADING);
		grain.update();
		ConnectionPool.commit(grain.getConnection());

		// теперь собственно обновление гранулы
		try {
			// Схему создаём, если ещё не создана.
			dba.createSchemaIfNotExists(g.getName());
			// Обновляем все таблицы.
			table.setRange("grainid", g.getName());
			while (table.next()) {
				table.setOrphaned(!g.getTables().containsKey(
						table.getTablename()));
				table.update();
			}
			for (Table t : g.getTables().values()) {
				updateTable(t);
				table.setGrainid(g.getName());
				table.setTablename(t.getName());
				table.setOrphaned(false);
				table.tryInsert();
			}

			// Обновляем все индексы.
			Set<String> dbIndices = dba.getIndices(grain.getConnection(), g);
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
			grain.setState(GrainsCursor.READY);
			grain.setChecksum(String.format("%08X", g.getChecksum()));
			grain.setLength(g.getLength());
			grain.setLastmodified(new Date());
			grain.setMessage("");
			grain.setVersion(g.getVersion().toString());
			grain.update();
		} catch (CelestaException e) {
			// Если что-то пошло не так
			grain.setState(GrainsCursor.ERROR);
			grain.setMessage(String.format(
					"Error while trying to update to version %s: %s", g
							.getVersion().toString(), e.getMessage()));
			grain.update();
		}
	}

	private static void updateTable(Table t) throws CelestaException {
		if (dba.tableExists(t.getGrain().getName(), t.getName())) {
			Set<String> dbColumns = dba.getColumns(grain.getConnection(), t);

			for (Entry<String, Column> e : t.getColumns().entrySet()) {
				if (dbColumns.contains(e.getKey())) {
					// TODO БД содержит колонку с таким именем, надо проверить
					// все её атрибуты и при необходимости и возможности --
					// обновить.
					System.out.println("Implement column check here.");
				} else {
					dba.createColumn(grain.getConnection(), e.getValue());
				}
			}
		} else {
			dba.createTable(t);
		}
		// TODO обновление внешних ключей
	}

}
