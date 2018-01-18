package ru.curs.celesta.dbutils;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.ICallContext;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.meta.*;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.*;
import ru.curs.celesta.syscursors.ISchemaCursor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public abstract class DbUpdater<T extends ICallContext> {

    private static final Comparator<Grain> GRAIN_COMPARATOR = Comparator.comparingInt(Grain::getDependencyOrder);

    private static final Set<Integer> EXPECTED_STATUSES;

    static {
        EXPECTED_STATUSES = new HashSet<>();
        EXPECTED_STATUSES.add(ISchemaCursor.READY);
        EXPECTED_STATUSES.add(ISchemaCursor.RECOVER);
        EXPECTED_STATUSES.add(ISchemaCursor.LOCK);
    }


    protected final DBAdaptor dbAdaptor;
    protected final AbstractScore score;
    private final boolean forceDdInitialize;
    protected final ConnectionPool connectionPool;
    protected ISchemaCursor schemaCursor;

    public DbUpdater(ConnectionPool connectionPool, AbstractScore score, boolean forceDdInitialize, DBAdaptor dbAdaptor) {
        this.connectionPool = connectionPool;
        this.score = score;
        this.forceDdInitialize = forceDdInitialize;
        this.dbAdaptor = dbAdaptor;
    }


    protected abstract T createContext() throws CelestaException;

    protected abstract void initDataAccessors(T context) throws CelestaException;

    protected abstract String getSchemasTableName();

    /**
     * Выполняет обновление структуры БД на основе разобранной объектной модели.
     *
     * @throws CelestaException в случае ошибки обновления.
     */
    public void updateDb() throws CelestaException {
        try (T context = createContext()) {
            Connection conn = context.getConn();

            initDataAccessors(context);

            // Проверяем наличие главной системной таблицы.
            if (!dbAdaptor.tableExists(conn, "celesta", getSchemasTableName())) {
                // Если главной таблицы нет, а другие таблицы есть -- ошибка.
                if (dbAdaptor.userTablesExist() && !forceDdInitialize)
                    throw new CelestaException("No celesta.grains table found in non-empty database.");
                // Если база вообще пустая, то создаём системные таблицы.
                updateSysGrain(context);
            }

            // Теперь собираем в память информацию о гранулах на основании того,
            // что
            // хранится в таблице grains.
            Map<String, GrainInfo> dbGrains = new HashMap<>();
            while (schemaCursor.nextInSet()) {

                if (!(EXPECTED_STATUSES.contains(schemaCursor.getState())))
                    throw new CelestaException("Cannot proceed with database upgrade: there are grains "
                            + "not in 'ready', 'recover' or 'lock' state.");
                GrainInfo gi = new GrainInfo();
                gi.checksum = (int) Long.parseLong(schemaCursor.getChecksum(), 16);
                gi.length = schemaCursor.getLength();
                gi.recover = schemaCursor.getState() == ISchemaCursor.RECOVER;
                gi.lock = schemaCursor.getState() == ISchemaCursor.LOCK;
                try {
                    gi.version = new VersionString(schemaCursor.getVersion());
                } catch (ParseException e) {
                    throw new CelestaException(
                            String.format("Error while scanning celesta.grains table: %s", e.getMessage()));
                }
                dbGrains.put(schemaCursor.getId(), gi);
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
                    success = updateGrain(g, connectionPool) & success;
                } else {
                    // Запись есть -- решение об апгрейде принимается на основе
                    // версии и контрольной суммы.
                    success = decideToUpgrade(g, gi, connectionPool) & success;
                }
            }
            if (!success)
                throw new CelestaException(
                        "Not all grains were updated successfully, see celesta.grains table data for details.");
        }
    }


    void updateSysGrain(T context) throws CelestaException {
        try {
            Connection conn = context.getConn();
            Grain sys = score.getGrain("celesta");
            createSysObjects(conn, sys);
            insertGrainRec(sys);
            updateGrain(sys, connectionPool);
        } catch (ParseException e) {
            throw new CelestaException("No 'celesta' grain definition found.");
        }
    }

    void createSysObjects(Connection conn, Grain sys) throws CelestaException, ParseException {
        dbAdaptor.createSchemaIfNotExists("celesta");
        dbAdaptor.createTable(conn, sys.getElement(getSchemasTableName(), Table.class));
        dbAdaptor.createSysObjects(conn);
    }

    private void insertGrainRec(Grain g) throws CelestaException {
        schemaCursor.init();
        schemaCursor.setId(g.getName());
        schemaCursor.setVersion(g.getVersion().toString());
        schemaCursor.setLength(g.getLength());
        schemaCursor.setChecksum(String.format("%08X", g.getChecksum()));
        schemaCursor.setState(ISchemaCursor.RECOVER);
        schemaCursor.setLastmodified(new Date());
        schemaCursor.setMessage("");
        schemaCursor.insert();
    }

    boolean decideToUpgrade(Grain g, GrainInfo gi, ConnectionPool connectionPool) throws CelestaException {
        if (gi.lock)
            return true;

        if (gi.recover)
            return updateGrain(g, connectionPool);

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
                return updateGrain(g, connectionPool);
            case EQUALS:
                // Версия не изменилась: апгрейдим лишь в том случае, если
                // изменилась контрольная сумма.
                if (gi.length != g.getLength() || gi.checksum != g.getChecksum())
                    return updateGrain(g, connectionPool);
            default:
                return true;
        }
    }

    /**
     * Выполняет обновление на уровне отдельной гранулы.
     *
     * @param g Гранула.
     * @throws CelestaException в случае ошибки обновления.
     */
    boolean updateGrain(Grain g, ConnectionPool connectionPool) throws CelestaException {
        // выставление в статус updating
        schemaCursor.get(g.getName());
        schemaCursor.setState(ISchemaCursor.UPGRADING);
        schemaCursor.update();
        connectionPool.commit(schemaCursor.callContext().getConn());

        // теперь собственно обновление гранулы
        try {
            // Схему создаём, если ещё не создана.
            dbAdaptor.createSchemaIfNotExists(g.getName());

            // Удаляем все представления
            dropAllViews(g);
            // Удаляем все параметризованные представления
            dropAllParameterizedViews(g);

            // Выполняем удаление ненужных индексов, чтобы облегчить задачу
            // обновления столбцов на таблицах.
            dropOrphanedGrainIndices(g);

            // Сбрасываем внешние ключи, более не включённые в метаданные
            List<DbFkInfo> dbFKeys = dropOrphanedGrainFKeys(g);

            Set<String> modifiedTablesMap = new HashSet<>();

            updateSequences(g);

            // Обновляем все таблицы.
            for (Table t : g.getElements(Table.class).values())
                if (updateTable(t, dbFKeys))
                    modifiedTablesMap.add(t.getName());

            // Обновляем все индексы.
            updateGrainIndices(g);

            // Обновляем внешние ключи
            updateGrainFKeys(g);

            // Создаём представления заново
            createViews(g);

            // Создаём параметризованные представления заново
            createParameterizedViews(g);

            // Обновляем все материализованные представления.
            for (MaterializedView mv : g.getElements(MaterializedView.class).values()) {
                String tableName = mv.getRefTable().getTable().getName();
                updateMaterializedView(mv, modifiedTablesMap.contains(tableName));
            }

            //Для всех таблиц обновляем триггеры материализованных представлений
            for (Table t : g.getElements(Table.class).values()) {
                final Connection conn = schemaCursor.callContext().getConn();
                dbAdaptor.dropTableTriggersForMaterializedViews(conn, t);
                dbAdaptor.createTableTriggersForMaterializedViews(conn, t);
            }

            processGrainMeta(g);

            // По завершении -- обновление номера версии, контрольной суммы
            // и выставление в статус ready
            schemaCursor.setState(ISchemaCursor.READY);
            schemaCursor.setChecksum(String.format("%08X", g.getChecksum()));
            schemaCursor.setLength(g.getLength());
            schemaCursor.setLastmodified(new Date());
            schemaCursor.setMessage("");
            schemaCursor.setVersion(g.getVersion().toString());
            schemaCursor.update();
            connectionPool.commit(schemaCursor.callContext().getConn());
            return true;
        } catch (CelestaException e) {
            String newMsg = "";
            try {
                schemaCursor.callContext().getConn().rollback();
            } catch (SQLException e1) {
                newMsg = ", " + e1.getMessage();
            }
            // Если что-то пошло не так
            schemaCursor.setState(ISchemaCursor.ERROR);
            schemaCursor.setMessage(String.format("%s/%d/%08X: %s", g.getVersion().toString(), g.getLength(), g.getChecksum(),
                    e.getMessage() + newMsg));
            schemaCursor.update();
            connectionPool.commit(schemaCursor.callContext().getConn());
            return false;
        }
    }

    protected abstract void processGrainMeta(Grain g) throws CelestaException;

    void createViews(Grain g) throws CelestaException {
        Connection conn = schemaCursor.callContext().getConn();
        for (View v : g.getElements(View.class).values())
            dbAdaptor.createView(conn, v);
    }

    void dropAllViews(Grain g) throws CelestaException {
        Connection conn = schemaCursor.callContext().getConn();
        for (String viewName : dbAdaptor.getViewList(conn, g))
            dbAdaptor.dropView(conn, g.getName(), viewName);
    }

    void createParameterizedViews(Grain g) throws CelestaException {
        Connection conn = schemaCursor.callContext().getConn();
        for (ParameterizedView pv : g.getElements(ParameterizedView.class).values())
            dbAdaptor.createParameterizedView(conn, pv);
    }

    void updateSequences(Grain g) throws CelestaException {
        Connection conn = schemaCursor.callContext().getConn();

        for (SequenceElement s : g.getElements(SequenceElement.class).values()) {
            if (dbAdaptor.sequenceExists(conn, g.getName(), s.getName())) {
                DbSequenceInfo sequenceInfo = dbAdaptor.getSequenceInfo(conn, s);
                if (sequenceInfo.reflects(s))
                    dbAdaptor.alterSequence(conn, s);
            } else {
                dbAdaptor.createSequence(conn, s);
            }
        }

    }

    void dropAllParameterizedViews(Grain g) throws CelestaException {
        Connection conn = schemaCursor.callContext().getConn();
        for (String viewName : dbAdaptor.getParameterizedViewList(conn, g))
            dbAdaptor.dropParameterizedView(conn, g.getName(), viewName);
    }

    void updateGrainFKeys(Grain g) throws CelestaException {
        Connection conn = schemaCursor.callContext().getConn();
        Map<String, DbFkInfo> dbFKeys = new HashMap<>();
        for (DbFkInfo dbi : dbAdaptor.getFKInfo(conn, g))
            dbFKeys.put(dbi.getName(), dbi);
        for (Table t : g.getElements(Table.class).values())
            if (t.isAutoUpdate())
                for (ForeignKey fk : t.getForeignKeys()) {
                    if (dbFKeys.containsKey(fk.getConstraintName())) {
                        // FK обнаружен в базе, апдейтим при необходимости.
                        DbFkInfo dbi = dbFKeys.get(fk.getConstraintName());
                        if (!dbi.reflects(fk)) {
                            dbAdaptor.dropFK(conn, g.getName(), dbi.getTableName(), dbi.getName());
                            dbAdaptor.createFK(conn, fk);
                        }
                    } else {
                        // FK не обнаружен в базе, создаём с нуля
                        dbAdaptor.createFK(conn, fk);
                    }
                }
    }

    List<DbFkInfo> dropOrphanedGrainFKeys(Grain g) throws CelestaException {
        Connection conn = schemaCursor.callContext().getConn();
        List<DbFkInfo> dbFKeys = dbAdaptor.getFKInfo(conn, g);
        Map<String, ForeignKey> fKeys = new HashMap<>();
        for (Table t : g.getElements(Table.class).values())
            for (ForeignKey fk : t.getForeignKeys())
                fKeys.put(fk.getConstraintName(), fk);
        Iterator<DbFkInfo> i = dbFKeys.iterator();
        while (i.hasNext()) {
            DbFkInfo dbFKey = i.next();
            ForeignKey fKey = fKeys.get(dbFKey.getName());
            if (fKey == null || !dbFKey.reflects(fKey)) {
                dbAdaptor.dropFK(conn, g.getName(), dbFKey.getTableName(), dbFKey.getName());
                i.remove();
            }
        }
        return dbFKeys;
    }

    void dropOrphanedGrainIndices(Grain g) throws CelestaException {
        /*
         * В целом метод повторяет код updateGrainIndices, но только в части
         * удаления индексов. Зачистить все индексы, подвергшиеся удалению или
         * изменению необходимо перед тем, как будет выполняться обновление
         * структуры таблиц, чтобы увеличить вероятность успешного результата:
         * висящие на полях индексы могут помешать процессу.
         */
        final Connection conn = schemaCursor.callContext().getConn();
        Map<String, DbIndexInfo> dbIndices = dbAdaptor.getIndices(conn, g);
        Map<String, Index> myIndices = g.getIndices();
        // Удаление несуществующих в метаданных индексов.
        for (DbIndexInfo dBIndexInfo : dbIndices.values())
            if (!myIndices.containsKey(dBIndexInfo.getIndexName()))
                dbAdaptor.dropIndex(g, dBIndexInfo);

        // Удаление индексов, которые будут в дальнейшем изменены, перед
        // обновлением таблиц.
        for (Map.Entry<String, Index> e : myIndices.entrySet()) {
            DbIndexInfo dBIndexInfo = dbIndices.get(e.getKey());
            if (dBIndexInfo != null) {
                boolean reflects = dBIndexInfo.reflects(e.getValue());
                if (!reflects)
                    dbAdaptor.dropIndex(g, dBIndexInfo);

                // Удаление индексов на тех полях, которые подвергнутся
                // изменению
                for (Map.Entry<String, Column> ee : e.getValue().getColumns().entrySet()) {
                    DbColumnInfo ci = dbAdaptor.getColumnInfo(conn, ee.getValue());
                    if (ci == null || !ci.reflects(ee.getValue())) {
                        dbAdaptor.dropIndex(g, dBIndexInfo);
                        break;
                    }
                }
            }
        }
    }

    void updateGrainIndices(Grain g) throws CelestaException {
        final Connection conn = schemaCursor.callContext().getConn();
        Map<String, DbIndexInfo> dbIndices = dbAdaptor.getIndices(conn, g);
        Map<String, Index> myIndices = g.getIndices();

        // Обновление и создание нужных индексов
        for (Map.Entry<String, Index> e : myIndices.entrySet()) {
            DbIndexInfo dBIndexInfo = dbIndices.get(e.getKey());
            if (dBIndexInfo != null) {
                // БД содержит индекс с таким именем, надо проверить
                // поля и пересоздать индекс в случае необходимости.
                boolean reflects = dBIndexInfo.reflects(e.getValue());
                if (!reflects) {
                    dbAdaptor.dropIndex(g, dBIndexInfo);
                    dbAdaptor.createIndex(conn, e.getValue());
                }
            } else {
                // Создаём не существовавший ранее индекс.
                dbAdaptor.createIndex(conn, e.getValue());
            }
        }
    }

    boolean updateTable(Table t, List<DbFkInfo> dbFKeys) throws CelestaException {
        // Если таблица скомпилирована с опцией NO AUTOUPDATE, то ничего не
        // делаем с ней
        if (!t.isAutoUpdate())
            return false;

        final Connection conn = schemaCursor.callContext().getConn();

        if (!dbAdaptor.tableExists(conn, t.getGrain().getName(), t.getName())) {
            // Таблицы не существует в базе данных, создаём с нуля.
            dbAdaptor.createTable(conn, t);
            return true;
        }

        DbPkInfo pkInfo;
        Set<String> dbColumns = dbAdaptor.getColumns(conn, t);
        boolean modified = updateColumns(t, conn, dbColumns, dbFKeys);

        // Для версионированных таблиц синхронизируем поле recversion
        if (t.isVersioned())
            if (dbColumns.contains(VersionedElement.REC_VERSION)) {
                DbColumnInfo ci = dbAdaptor.getColumnInfo(conn, t.getRecVersionField());
                if (!ci.reflects(t.getRecVersionField())) {
                    dbAdaptor.updateColumn(conn, t.getRecVersionField(), ci);
                    modified = true;
                }
            } else {
                dbAdaptor.createColumn(conn, t.getRecVersionField());
                modified = true;
            }


        // Ещё раз проверяем первичный ключ и при необходимости (если его нет
        // или он был сброшен) создаём.
        pkInfo = dbAdaptor.getPKInfo(conn, t);
        if (pkInfo.isEmpty())
            dbAdaptor.createPK(conn, t);

        if (modified)
            try {
                dbAdaptor.manageAutoIncrement(conn, t);
            } catch (SQLException e) {
                throw new CelestaException("Updating table %s.%s failed: %s.", t.getGrain().getName(), t.getName(),
                        e.getMessage());
            }

        dbAdaptor.updateVersioningTrigger(conn, t);

        return modified;
    }

    void updateMaterializedView(MaterializedView mv, boolean refTableIsModified) throws CelestaException {
        final Connection conn = schemaCursor.callContext().getConn();

        boolean mViewExists = dbAdaptor.tableExists(conn, mv.getGrain().getName(), mv.getName());

        if (mViewExists) {

            if (!refTableIsModified) {

                //В теле insert-триггера должна храниться контрольная сумма.
                String insertTriggerName = mv.getTriggerName(TriggerType.POST_INSERT);
                TriggerQuery query = new TriggerQuery()
                        .withSchema(mv.getGrain().getName())
                        .withTableName(mv.getRefTable().getTable().getName())
                        .withName(insertTriggerName);

                Optional<String> insertTriggerBody = dbAdaptor.getTriggerBody(conn, query);
                boolean checksumIsMatched = insertTriggerBody.map(b -> b.contains(
                        String.format(MaterializedView.CHECKSUM_COMMENT_TEMPLATE, mv.getChecksum()))).orElse(false);
                if (checksumIsMatched) {
                    return;
                }
            }

            //Удаляем materialized view
            dbAdaptor.dropTable(conn, mv);
        }

        //1. Таблицы не существует в базе данных, создаём с нуля.
        dbAdaptor.createTable(conn, mv);
        //2. Проинициализировать данные материального представления
        dbAdaptor.initDataForMaterializedView(conn, mv);
    }

    private boolean updateColumns(TableElement t, final Connection conn, Set<String> dbColumns, List<DbFkInfo> dbFKeys)
            throws CelestaException {
        // Таблица существует в базе данных, определяем: надо ли удалить
        // первичный ключ
        DbPkInfo pkInfo = dbAdaptor.getPKInfo(conn, t);
        boolean result = false;
        boolean keyDropped = pkInfo.isEmpty();
        if (!(pkInfo.reflects(t) || keyDropped)) {
            dropReferencedFKs(t, conn, dbFKeys);
            dbAdaptor.dropPK(conn, t, pkInfo.getName());
            keyDropped = true;
        }

        for (Map.Entry<String, Column> e : t.getColumns().entrySet()) {
            if (dbColumns.contains(e.getKey())) {
                // Таблица содержит колонку с таким именем, надо проверить
                // все её атрибуты и при необходимости -- попытаться
                // обновить.
                DbColumnInfo ci = dbAdaptor.getColumnInfo(conn, e.getValue());
                if (!ci.reflects(e.getValue())) {
                    // Если колонка, требующая обновления, входит в первичный
                    // ключ -- сбрасываем первичный ключ.
                    if (t.getPrimaryKey().containsKey(e.getKey()) && !keyDropped) {
                        dropReferencedFKs(t, conn, dbFKeys);
                        dbAdaptor.dropPK(conn, t, pkInfo.getName());
                        keyDropped = true;
                    }
                    dbAdaptor.updateColumn(conn, e.getValue(), ci);
                    result = true;
                }
            } else {
                // Таблица не содержит колонку с таким именем, добавляем
                dbAdaptor.createColumn(conn, e.getValue());
                result = true;
            }
        }
        return result;
    }

    private void dropReferencedFKs(TableElement t, Connection conn, List<DbFkInfo> dbFKeys) throws CelestaException {
        Iterator<DbFkInfo> i = dbFKeys.iterator();
        while (i.hasNext()) {
            DbFkInfo dbFKey = i.next();
            if (t.getGrain().getName().equals(dbFKey.getRefGrainName())
                    && t.getName().equals(dbFKey.getRefTableName())) {
                dbAdaptor.dropFK(conn, t.getGrain().getName(), dbFKey.getTableName(), dbFKey.getName());
                i.remove();
            }
        }
    }

    /**
     * Буфер для хранения информации о грануле.
     */
    class GrainInfo {
        private boolean recover;
        private boolean lock;
        private int length;
        private int checksum;
        private VersionString version;
    }
}
