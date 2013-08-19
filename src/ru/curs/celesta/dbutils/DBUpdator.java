package ru.curs.celesta.dbutils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ru.curs.celesta.CelestaCritical;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.ParseException;
import ru.curs.celesta.score.Score;
import ru.curs.celesta.score.VersionString;

/**
 * Класс, выполняющий процедуру обновления базы данных.
 * 
 */
public final class DBUpdator {

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
	 * @throws CelestaCritical
	 *             в случае ошибки обновления.
	 */
	public static void updateDB(Score score) throws CelestaCritical {
		DBAdaptor dba = DBAdaptor.getAdaptor();
		Connection conn = ConnectionPool.get();
		try {
			GrainsCursor c = new GrainsCursor(conn);

			// Проверяем наличие главной системной таблицы.
			if (!dba.tableExists("celesta", "grains")) {
				// Если главной таблицы нет, а другие таблицы есть -- ошибка.
				if (dba.userTablesExist())
					throw new CelestaCritical(
							"No celesta.grains table found in non-empty database.");
				// Если база вообще пустая, то создаём системные таблицы.
				try {
					Grain sys = score.getGrain("celesta");
					dba.createSchemaIfNotExists("celesta");
					dba.createTable(sys.getTable("grains"));
					c.init();
					c.setId(sys.getName());
					c.setLength(sys.getLength());
					c.setChecksum(sys.getChecksum());
					c.setState(GrainsCursor.RECOVER);
					c.insert();
					updateGrain(sys);
				} catch (ParseException e) {
					throw new CelestaCritical(
							"No 'celesta' grain definition found.");
				}
			}

			// Теперь собираем в память информацию о гранулах на основании того,
			// что
			// хранится в таблице grains.
			Map<String, GrainInfo> dbGrains = new HashMap<>();
			while (c.next()) {
				if (!(c.getState() == GrainsCursor.READY || c.getState() == GrainsCursor.RECOVER))
					throw new CelestaCritical(
							"Cannot proceed with database upgrade: there are grains "
									+ "not in 'ready' or 'recover' state.");
				GrainInfo gi = new GrainInfo();
				gi.checksum = c.getChecksum();
				gi.length = c.getLength();
				gi.recover = c.getState() == GrainsCursor.RECOVER;
				try {
					gi.version = new VersionString(c.getVersion());
				} catch (ParseException e) {
					throw new CelestaCritical(String.format(
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
					// Записи нет --- создаём и апгрейдим.
					c.init();
					c.setId(g.getName());
					c.setLength(g.getLength());
					c.setChecksum(g.getChecksum());
					c.setState(GrainsCursor.RECOVER);
					c.insert();
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

	private static void decideToUpgrade(Grain g, GrainInfo gi)
			throws CelestaCritical {
		if (gi.recover) {
			updateGrain(g);
			return;
		}

		// Как соотносятся версии?
		switch (g.getVersion().compareTo(gi.version)) {
		case LOWER:
			// Старая версия -- не апгрейдим, ошибка.
			throw new CelestaCritical(
					String.format(
							"Grain '%s' version '%s' is lower than database "
									+ "grain version '%s'. Will not proceed with auto-upgrade.",
							g.getName(), g.getVersion().toString(),
							gi.version.toString()));
		case INCONSISTENT:
			// Непонятная (несовместимая) версия -- не апгрейдим,
			// ошибка.
			throw new CelestaCritical(
					String.format(
							"Grain '%s' version '%s' is inconsistent than database "
									+ "grain version '%s'. Will not proceed with auto-upgrade.",
							g.getName(), g.getVersion().toString(),
							gi.version.toString()));
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
	 * @throws CelestaCritical
	 *             в случае ошибки обновления.
	 */
	static void updateGrain(Grain g) throws CelestaCritical {
		// выставление в статус updating
		Connection conn = ConnectionPool.get();
		try {
			GrainsCursor c = new GrainsCursor(conn);
			c.get(g.getName());
			c.setState(GrainsCursor.UPGRADING);
			c.update();
			ConnectionPool.commit(conn);
			// теперь собственно обновление гранулы
			try {
				// TODO

				// по завершении -- обновление номера версии, контрольной суммы
				// и
				// выставление в статус ready
				c.setState(GrainsCursor.READY);
				c.setChecksum(g.getChecksum());
				c.setLength(g.getLength());
				c.setLastmodified(new Date());
				c.setMessage("");
				c.setVersion(g.getVersion().toString());
				c.update();

				// TODO DELETETHIS
				throw new CelestaCritical("!!");
			} catch (CelestaCritical e) {
				// Если что-то пошло не так
				c.setState(GrainsCursor.ERROR);
				c.setMessage(String.format(
						"Error while trying to update to version %s: %s", g
								.getVersion().toString(), e.getMessage()));
				c.update();
			}
		} finally {
			ConnectionPool.putBack(conn);
		}
	}
}
