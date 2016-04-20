package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import ru.curs.celesta.AppSettings;
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
import ru.curs.celesta.syscursors.RolesCursor;
import ru.curs.celesta.syscursors.TablesCursor;
import ru.curs.celesta.syscursors.TablesCursor.TableType;
import ru.curs.celesta.syscursors.UserRolesCursor;

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
				if (dba.userTablesExist() && !AppSettings.getForceDBInitialize())
					throw new CelestaException("No celesta.grains table found in non-empty database.");
				// Если база вообще пустая, то создаём системные таблицы.
				try {
					Grain sys = score.getGrain("celesta");
					dba.createSchemaIfNotExists("celesta");
					dba.createTable(conn, sys.getTable("grains"));
					dba.createTable(conn, sys.getTable("tables"));
					dba.createTable(conn, sys.getTable("sequences"));
					dba.createSysObjects(conn);
					// logsetup -- версионированная таблица, поэтому для её
					// создания уже могут понадобиться системные объекты
					dba.createTable(conn, sys.getTable("logsetup"));
					insertGrainRec(sys);
					updateGrain(sys);
					initSecurity(context);
				} catch (ParseException e) {
					throw new CelestaException("No 'celesta' grain definition found.");
				}

			}

			// Теперь собираем в память информацию о гранулах на основании того,
			// что
			// хранится в таблице grains.
			Map<String, GrainInfo> dbGrains = new HashMap<>();
			while (grain.nextInSet()) {

				if (!(EXPECTED_STATUSES.contains(grain.getState())))
					throw new CelestaException("Cannot proceed with database upgrade: there are grains "
							+ "not in 'ready', 'recover' or 'lock' state.");
				GrainInfo gi = new GrainInfo();
				gi.checksum = (int) Long.parseLong(grain.getChecksum(), 16);
				gi.length = grain.getLength();
				gi.recover = grain.getState() == GrainsCursor.RECOVER;
				gi.lock = grain.getState() == GrainsCursor.LOCK;
				try {
					gi.version = new VersionString(grain.getVersion());
				} catch (ParseException e) {
					throw new CelestaException(
							String.format("Error while scanning celesta.grains table: %s", e.getMessage()));
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
			context.closeCursors();
			ConnectionPool.putBack(conn);
		}
	}

	/**
	 * Инициализация записей в security-таблицах. Производится один раз при
	 * создании системной гранулы.
	 * 
	 * @throws CelestaException
	 */
	private static void initSecurity(CallContext context) throws CelestaException {
		RolesCursor roles = new RolesCursor(context);
		roles.clear();
		roles.setId("editor");
		roles.setDescription("full read-write access");
		roles.tryInsert();

		roles.clear();
		roles.setId("reader");
		roles.setDescription("full read-only access");
		roles.tryInsert();

		UserRolesCursor userRoles = new UserRolesCursor(context);
		userRoles.clear();
		userRoles.setRoleid("editor");
		userRoles.setUserid("super");
		userRoles.tryInsert();
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

	private static boolean decideToUpgrade(Grain g, GrainInfo gi) throws CelestaException {
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
					g.getName(), g.getVersion().toString(), gi.version.toString());
		case INCONSISTENT:
			// Непонятная (несовместимая) версия -- не апгрейдим,
			// ошибка.
			throw new CelestaException(
					"Grain '%s' version '%s' is inconsistent with database "
							+ "grain version '%s'. Will not proceed with auto-upgrade.",
					g.getName(), g.getVersion().toString(), gi.version.toString());
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
			List<DBFKInfo> dbFKeys = dropOrphanedGrainFKeys(g);

			// Обновляем все таблицы.
			for (Table t : g.getTables().values())
				updateTable(t, dbFKeys);

			// Обновляем все индексы.
			updateGrainIndices(g);

			// Обновляем внешние ключи
			updateGrainFKeys(g);

			// Создаём представления заново
			createViews(g);

			// Обновляем справочник celesta.tables.
			table.setRange("grainid", g.getName());
			while (table.nextInSet()) {
				switch (table.getTabletype()) {
				case TABLE:
					table.setOrphaned(!g.getTables().containsKey(table.getTablename()));
					break;
				case VIEW:
					table.setOrphaned(!g.getViews().containsKey(table.getTablename()));
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
			grain.setMessage(String.format("%s/%d/%08X: %s", g.getVersion().toString(), g.getLength(), g.getChecksum(),
					e.getMessage() + newMsg));
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
			if (t.isAutoUpdate())
				for (ForeignKey fk : t.getForeignKeys()) {
					if (dbFKeys.containsKey(fk.getConstraintName())) {
						// FK обнаружен в базе, апдейтим при необходимости.
						DBFKInfo dbi = dbFKeys.get(fk.getConstraintName());
						if (!dbi.reflects(fk)) {
							dba.dropFK(conn, g.getName(), dbi.getTableName(), dbi.getName());
							dba.createFK(conn, fk);
						}
					} else {
						// FK не обнаружен в базе, создаём с нуля
						dba.createFK(conn, fk);
					}
				}
	}

	private static List<DBFKInfo> dropOrphanedGrainFKeys(Grain g) throws CelestaException {
		Connection conn = grain.callContext().getConn();
		List<DBFKInfo> dbFKeys = dba.getFKInfo(conn, g);
		Map<String, ForeignKey> fKeys = new HashMap<>();
		for (Table t : g.getTables().values())
			for (ForeignKey fk : t.getForeignKeys())
				fKeys.put(fk.getConstraintName(), fk);
		Iterator<DBFKInfo> i = dbFKeys.iterator();
		while (i.hasNext()) {
			DBFKInfo dbFKey = i.next();
			ForeignKey fKey = fKeys.get(dbFKey.getName());
			if (fKey == null || !dbFKey.reflects(fKey)) {
				dba.dropFK(conn, g.getName(), dbFKey.getTableName(), dbFKey.getName());
				i.remove();
			}
		}
		return dbFKeys;
	}

	private static void dropOrphanedGrainIndices(Grain g) throws CelestaException {
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

				// Удаление индексов на тех полях, которые подвергнутся
				// изменению
				for (Entry<String, Column> ee : e.getValue().getColumns().entrySet()) {
					DBColumnInfo ci = dba.getColumnInfo(conn, ee.getValue());
					if (ci == null || !ci.reflects(ee.getValue())) {
						dba.dropIndex(g, dBIndexInfo, true);
						break;
					}
				}
			}
		}
	}

	private static void updateGrainIndices(Grain g) throws CelestaException {
		final Connection conn = grain.callContext().getConn();
		Map<String, DBIndexInfo> dbIndices = dba.getIndices(conn, g);
		Map<String, Index> myIndices = g.getIndices();
		// Начинаем с удаления ненужных индексов (ещё раз, в MySQL могло
		// остаться из-за ошибок)
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

	private static void updateTable(Table t, List<DBFKInfo> dbFKeys) throws CelestaException {
		// Если таблица скомпилирована с опцией NO AUTOUPDATE, то ничего не
		// делаем с ней
		if (!t.isAutoUpdate())
			return;

		final Connection conn = grain.callContext().getConn();

		if (!dba.tableExists(conn, t.getGrain().getName(), t.getName())) {
			// Таблицы не существует в базе данных, создаём с нуля.
			dba.createTable(conn, t);
			return;
		}

		DBPKInfo pkInfo;
		Set<String> dbColumns = dba.getColumns(conn, t);
		boolean modified = updateColumns(t, conn, dbColumns, dbFKeys);

		// Для версионированных таблиц синхронизируем поле recversion
		if (t.isVersioned())
			if (dbColumns.contains(Table.RECVERSION)) {
				DBColumnInfo ci = dba.getColumnInfo(conn, t.getRecVersionField());
				if (!ci.reflects(t.getRecVersionField())) {
					dba.updateColumn(conn, t.getRecVersionField(), ci);
					modified = true;
				}
			} else {
				dba.createColumn(conn, t.getRecVersionField());
				modified = true;
			}

		// Ещё раз проверяем первичный ключ и при необходимости (если его нет
		// или он был сброшен) создаём.
		pkInfo = dba.getPKInfo(conn, t);
		if (pkInfo.isEmpty())
			dba.createPK(conn, t);

		if (modified)
			try {
				dba.manageAutoIncrement(conn, t);
			} catch (SQLException e) {
				throw new CelestaException("Updating table %s.%s failed: %s.", t.getGrain().getName(), t.getName(),
						e.getMessage());
			}

		dba.updateVersioningTrigger(conn, t);
	}

	private static void dropReferencedFKs(Table t, Connection conn, List<DBFKInfo> dbFKeys) throws CelestaException {
		Iterator<DBFKInfo> i = dbFKeys.iterator();
		while (i.hasNext()) {
			DBFKInfo dbFKey = i.next();
			if (t.getGrain().getName().equals(dbFKey.getRefGrainName())
					&& t.getName().equals(dbFKey.getRefTableName())) {
				dba.dropFK(conn, t.getGrain().getName(), dbFKey.getTableName(), dbFKey.getName());
				i.remove();
			}
		}
	}

	private static boolean updateColumns(Table t, final Connection conn, Set<String> dbColumns, List<DBFKInfo> dbFKeys)
			throws CelestaException {
		// Таблица существует в базе данных, определяем: надо ли удалить
		// первичный ключ
		DBPKInfo pkInfo = dba.getPKInfo(conn, t);
		boolean result = false;
		boolean keyDropped = pkInfo.isEmpty();
		if (!(pkInfo.reflects(t) || keyDropped)) {
			dropReferencedFKs(t, conn, dbFKeys);
			dba.dropPK(conn, t, pkInfo.getName());
			keyDropped = true;
		}

		for (Entry<String, Column> e : t.getColumns().entrySet()) {
			if (dbColumns.contains(e.getKey())) {
				// Таблица содержит колонку с таким именем, надо проверить
				// все её атрибуты и при необходимости -- попытаться
				// обновить.
				DBColumnInfo ci = dba.getColumnInfo(conn, e.getValue());
				if (!ci.reflects(e.getValue())) {
					// Если колонка, требующая обновления, входит в первичный
					// ключ -- сбрасываем первичный ключ.
					if (t.getPrimaryKey().containsKey(e.getKey()) && !keyDropped) {
						dropReferencedFKs(t, conn, dbFKeys);
						dba.dropPK(conn, t, pkInfo.getName());
						keyDropped = true;
					}
					dba.updateColumn(conn, e.getValue(), ci);
					result = true;
				}
			} else {
				// Таблица не содержит колонку с таким именем, добавляем
				dba.createColumn(conn, e.getValue());
				result = true;
			}
		}
		return result;
	}

}
