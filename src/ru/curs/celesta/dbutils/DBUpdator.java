package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.ForeignKey;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.Index;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.VersionString;
import ru.curs.celesta.score.View;
import ru.curs.celesta.syscursors.GrainsCursor;
import ru.curs.celesta.syscursors.TablesCursor;
import ru.curs.celesta.syscursors.TablesCursor.TableType;

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

	private static final Set<Integer> EXPECTED_STATUSES;
	static {
		EXPECTED_STATUSES = new HashSet<>();
		EXPECTED_STATUSES.add(GrainsCursor.READY);
		EXPECTED_STATUSES.add(GrainsCursor.RECOVER);
		EXPECTED_STATUSES.add(GrainsCursor.LOCK);
	}

	private DBUpdator() {
	}

	/**
	 * Буфер для хранения информации о грануле.
	 */
	private static class GrainInfo {
		private boolean recover;
		private boolean lock;
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
		CallContext context = new CallContext(conn, BasicCursor.SYSTEMSESSION);
		try {
			grain = new GrainsCursor(context);
			table = new TablesCursor(context);

			// Проверяем наличие главной системной таблицы.
			if (!dba.tableExists(conn, "celesta", "grains")) {
				// Если главной таблицы нет, а другие таблицы есть -- ошибка.
				if (dba.userTablesExist())
					throw new CelestaException(
							"No celesta.grains table found in non-empty database.");
				// Если база вообще пустая, то создаём системные таблицы.
				try {
					Grain sys = score.getGrain("celesta");
					dba.createSchemaIfNotExists("celesta");
					dba.createTable(conn, sys.getTable("grains"));
					dba.createTable(conn, sys.getTable("tables"));
					dba.createTable(conn, sys.getTable("logsetup"));
					dba.createTable(conn, sys.getTable("sequences"));
					dba.createSysObjects(conn);
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

				if (!(EXPECTED_STATUSES.contains(grain.getState())))
					throw new CelestaException(
							"Cannot proceed with database upgrade: there are grains "
									+ "not in 'ready', 'recover' or 'lock' state.");
				GrainInfo gi = new GrainInfo();
				gi.checksum = (int) Long.parseLong(grain.getChecksum(), 16);
				gi.length = grain.getLength();
				gi.recover = grain.getState() == GrainsCursor.RECOVER;
				gi.lock = grain.getState() == GrainsCursor.LOCK;
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
			boolean success = true;
			for (Grain g : grains) {
				// Запись о грануле есть?
				GrainInfo gi = dbGrains.get(g.getName());
				if (gi == null) {
					insertGrainRec(g);
					success = updateGrain(g) & success;
				} else {
					// Запись есть -- решение об апгрейде принимается на основе
					// версии и контрольной суммы.
					success = decideToUpgrade(g, gi) & success;
				}
			}
			if (!success)
				throw new CelestaException(
						"Not all grains were updated successfully, see celesta.grains table data for details.");
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

	private static boolean decideToUpgrade(Grain g, GrainInfo gi)
			throws CelestaException {
		if (gi.lock)
			return true;

		if (gi.recover)
			return updateGrain(g);

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
			return updateGrain(g);
		case EQUALS:
			// Версия не изменилась: апгрейдим лишь в том случае, если
			// изменилась контрольная сумма.
			if (gi.length != g.getLength() || gi.checksum != g.getChecksum())
				return updateGrain(g);
		default:
			return true;
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
	private static boolean updateGrain(Grain g) throws CelestaException {
		// выставление в статус updating
		grain.get(g.getName());
		grain.setState(GrainsCursor.UPGRADING);
		grain.update();
		ConnectionPool.commit(grain.callContext().getConn());

		// теперь собственно обновление гранулы
		try {
			// Схему создаём, если ещё не создана.
			dba.createSchemaIfNotExists(g.getName());

			// Удаляем все представления
			dropAllViews(g);

			// Выполняем удаление ненужных индексов, чтобы облегчить задачу
			// обновления столбцов на таблицах.
			dropOrphanedGrainIndices(g);

			// Сбрасываем внешние ключи, более не включённые в метаданные
			dropOrphanedGrainFKeys(g);

			// Обновляем все таблицы.
			for (Table t : g.getTables().values())
				updateTable(t);

			// Обновляем все индексы.
			updateGrainIndices(g);

			// Обновляем внешние ключи
			updateGrainFKeys(g);

			// Создаём представления заново
			createViews(g);

			// Обновляем справочник celesta.tables.
			table.setRange("grainid", g.getName());
			while (table.next()) {
				switch (table.getTabletype()) {
				case TABLE:
					table.setOrphaned(!g.getTables().containsKey(
							table.getTablename()));
					break;
				case VIEW:
					table.setOrphaned(!g.getViews().containsKey(
							table.getTablename()));
				default:
					break;
				}
				table.update();
			}
			for (Table t : g.getTables().values()) {
				table.setGrainid(g.getName());
				table.setTablename(t.getName());
				table.setTabletype(TableType.TABLE);
				table.setOrphaned(false);
				table.tryInsert();
			}
			for (View v : g.getViews().values()) {
				table.setGrainid(g.getName());
				table.setTablename(v.getName());
				table.setTabletype(TableType.VIEW);
				table.setOrphaned(false);
				table.tryInsert();
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
			ConnectionPool.commit(grain.callContext().getConn());
			return true;
		} catch (CelestaException e) {
			String newMsg = "";
			try {
				grain.callContext().getConn().rollback();
			} catch (SQLException e1) {
				newMsg = ", " + e1.getMessage();
			}
			// Если что-то пошло не так
			grain.setState(GrainsCursor.ERROR);
			grain.setMessage(String.format("%s/%d/%08X: %s", g.getVersion()
					.toString(), g.getLength(), g.getChecksum(), e.getMessage()
					+ newMsg));
			grain.update();
			ConnectionPool.commit(grain.callContext().getConn());
			return false;
		}
	}

	private static void createViews(Grain g) throws CelestaException {
		Connection conn = grain.callContext().getConn();
		for (View v : g.getViews().values())
			dba.createView(conn, v);
	}

	private static void dropAllViews(Grain g) throws CelestaException {
		Connection conn = grain.callContext().getConn();
		for (String viewName : dba.getViewList(conn, g))
			dba.dropView(conn, g.getName(), viewName);
	}

	private static void updateGrainFKeys(Grain g) throws CelestaException {
		Connection conn = grain.callContext().getConn();
		Map<String, DBFKInfo> dbFKeys = new HashMap<>();
		for (DBFKInfo dbi : dba.getFKInfo(conn, g))
			dbFKeys.put(dbi.getName(), dbi);
		for (Table t : g.getTables().values())
			for (ForeignKey fk : t.getForeignKeys()) {
				if (dbFKeys.containsKey(fk.getConstraintName())) {
					// FK обнаружен в базе, апдейтим при необходимости.
					DBFKInfo dbi = dbFKeys.get(fk.getConstraintName());
					if (!dbi.reflects(fk)) {
						dba.dropFK(conn, g.getName(), dbi.getTableName(),
								dbi.getName());
						dba.createFK(conn, fk);
					}
				} else {
					// FK не обнаружен в базе, создаём с нуля
					dba.createFK(conn, fk);
				}
			}
	}

	private static void dropOrphanedGrainFKeys(Grain g) throws CelestaException {
		Connection conn = grain.callContext().getConn();
		List<DBFKInfo> dbFKeys = dba.getFKInfo(conn, g);
		Map<String, ForeignKey> fKeys = new HashMap<>();
		for (Table t : g.getTables().values())
			for (ForeignKey fk : t.getForeignKeys())
				fKeys.put(fk.getConstraintName(), fk);
		for (DBFKInfo dbFKey : dbFKeys) {
			ForeignKey fKey = fKeys.get(dbFKey.getName());
			if (fKey == null || !dbFKey.reflects(fKey)) {
				dba.dropFK(conn, g.getName(), dbFKey.getTableName(),
						dbFKey.getName());
			}
		}
	}

	private static void dropOrphanedGrainIndices(Grain g)
			throws CelestaException {
		/*
		 * В целом метод повторяет код updateGrainIndices, но только в части
		 * удаления индексов. Зачистить все индексы, подвергшиеся удалению или
		 * изменению необходимо перед тем, как будет выполняться обновление
		 * структуры таблиц, чтобы увеличить вероятность успешного результата:
		 * висящие на полях индексы могут помешать процессу.
		 */
		final Connection conn = grain.callContext().getConn();
		Map<String, DBIndexInfo> dbIndices = dba.getIndices(conn, g);
		Map<String, Index> myIndices = g.getIndices();
		// Удаление несуществующих в метаданных индексов.
		for (DBIndexInfo dBIndexInfo : dbIndices.values())
			if (!myIndices.containsKey(dBIndexInfo.getIndexName()))
				dba.dropIndex(g, dBIndexInfo, true);

		// Удаление индексов, которые будут в дальнейшем изменены, перед
		// обновлением таблиц.
		for (Entry<String, Index> e : myIndices.entrySet()) {
			DBIndexInfo dBIndexInfo = dbIndices.get(e.getKey());
			if (dBIndexInfo != null) {
				boolean reflects = dBIndexInfo.reflects(e.getValue());
				if (!reflects)
					dba.dropIndex(g, dBIndexInfo, true);
			}
		}
	}

	private static void updateGrainIndices(Grain g) throws CelestaException {
		final Connection conn = grain.callContext().getConn();
		Map<String, DBIndexInfo> dbIndices = dba.getIndices(conn, g);
		Map<String, Index> myIndices = g.getIndices();
		// Начинаем с удаления ненужных индексов (ещё раз)
		for (DBIndexInfo dBIndexInfo : dbIndices.values())
			if (!myIndices.containsKey(dBIndexInfo.getIndexName()))
				dba.dropIndex(g, dBIndexInfo, true);

		// Обновление и создание нужных индексов
		for (Entry<String, Index> e : myIndices.entrySet()) {
			DBIndexInfo dBIndexInfo = dbIndices.get(e.getKey());
			if (dBIndexInfo != null) {
				// БД содержит индекс с таким именем, надо проверить
				// поля и пересоздать индекс в случае необходимости.
				boolean reflects = dBIndexInfo.reflects(e.getValue());
				if (!reflects) {
					dba.dropIndex(g, dBIndexInfo, false);
					dba.createIndex(conn, e.getValue());
				}
			} else {
				// Создаём не существовавший ранее индекс.
				dba.createIndex(conn, e.getValue());
			}
		}
	}

	private static void updateTable(Table t) throws CelestaException {
		final Connection conn = grain.callContext().getConn();

		if (!dba.tableExists(conn, t.getGrain().getName(), t.getName())) {
			// Таблицы не существует в базе данных, создаём с нуля.
			dba.createTable(conn, t);
			return;
		}

		// Таблица существует в базе данных, определяем: надо ли удалить
		// первичный ключ
		DBPKInfo pkInfo = dba.getPKInfo(conn, t);
		if (!(pkInfo.isEmpty() || pkInfo.reflects(t)))
			dba.dropPK(conn, t, pkInfo.getName());

		Set<String> dbColumns = dba.getColumns(conn, t);
		for (Entry<String, Column> e : t.getColumns().entrySet()) {
			if (dbColumns.contains(e.getKey())) {
				// Таблица содержит колонку с таким именем, надо проверить
				// все её атрибуты и при необходимости -- попытаться
				// обновить.
				DBColumnInfo ci = dba.getColumnInfo(conn, e.getValue());
				if (!ci.reflects(e.getValue())) {
					// Если колонка, требующая обновления, входит в первичный
					// ключ -- сбрасываем первичный ключ.
					if (t.getPrimaryKey().containsKey(e.getKey()))
						dba.dropPK(conn, t, pkInfo.getName());
					dba.updateColumn(conn, e.getValue(), ci);
				}
			} else {
				// Таблица не содержит колонку с таким именем, добавляем
				dba.createColumn(conn, e.getValue());
			}
		}

		// Для версионированных таблиц синхронизируем поле recversion
		if (t.isVersioned())
			if (dbColumns.contains(Table.RECVERSION)) {
				DBColumnInfo ci = dba.getColumnInfo(conn,
						t.getRecVersionField());
				if (!ci.reflects(t.getRecVersionField()))
					dba.updateColumn(conn, t.getRecVersionField(), ci);
			} else {
				dba.createColumn(conn, t.getRecVersionField());
			}

		// Ещё раз проверяем первичный ключ и при необходимости создаём.
		pkInfo = dba.getPKInfo(conn, t);
		if (pkInfo.isEmpty())
			dba.createPK(conn, t);

		dba.updateVersioningTrigger(conn, t);
	}

}
