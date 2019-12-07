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
    protected final ConnectionPool connectionPool;
    protected ISchemaCursor schemaCursor;
    private final boolean forceDdInitialize;

    public DbUpdater(
            ConnectionPool connectionPool, AbstractScore score, boolean forceDdInitialize, DBAdaptor dbAdaptor) {

        this.connectionPool = connectionPool;
        this.score = score;
        this.forceDdInitialize = forceDdInitialize;
        this.dbAdaptor = dbAdaptor;
    }


    protected abstract T createContext();

    protected abstract void initDataAccessors(T context);

    protected abstract String getSchemasTableName();

    /**
     * Updates system schema.
     */
    public void updateSystemSchema() {
        try (T context = createContext()) {
            updateSystemSchema(context);
        }
    }

    private void updateSystemSchema(T context) {
        initDataAccessors(context);

        Connection conn = context.getConn();
        // Проверяем наличие главной системной таблицы.
        if (!dbAdaptor.tableExists(conn, score.getSysSchemaName(), getSchemasTableName())) {
            // Если главной таблицы нет, а другие таблицы есть -- ошибка.
            if (dbAdaptor.userTablesExist() && !forceDdInitialize) {
                throw new CelestaException("No %s.%s table found in non-empty database.",
                        score.getSysSchemaName(), getSchemasTableName());
            }
            // Если база вообще пустая, то создаём системные таблицы.
            updateSysGrain(context);
        }
    }

    /**
     * Performs update of DB structure based on the decomposed object model.
     */
    public void updateDb() {
        String sysSchemaName = score.getSysSchemaName();
        try (T context = createContext()) {
            updateSystemSchema(context);

            // Теперь собираем в память информацию о гранулах на основании того,
            // что
            // хранится в таблице grains.
            Map<String, GrainInfo> dbGrains = new HashMap<>();
            while (schemaCursor.nextInSet()) {

                if (!(EXPECTED_STATUSES.contains(schemaCursor.getState()))) {
                    throw new CelestaException("Cannot proceed with database upgrade: there are %s "
                            + "not in 'ready', 'recover' or 'lock' state.", getSchemasTableName());
                }
                GrainInfo gi = new GrainInfo();
                gi.checksum = (int) Long.parseLong(schemaCursor.getChecksum(), 16);
                gi.length = schemaCursor.getLength();
                gi.recover = schemaCursor.getState() == ISchemaCursor.RECOVER;
                gi.lock = schemaCursor.getState() == ISchemaCursor.LOCK;
                try {
                    gi.version = new VersionString(schemaCursor.getVersion());
                } catch (ParseException e) {
                    throw new CelestaException(
                            String.format(
                                    "Error while scanning %s.%s table: %s",
                                    sysSchemaName, getSchemasTableName(), e.getMessage()
                            )
                    );
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
                if (!g.isAutoupdate()) {
                    continue;
                }
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
            if (!success) {
                throw new CelestaException(
                        "Not all %s were updated successfully, see %s.%s table data for details.",
                        getSchemasTableName(), sysSchemaName, getSchemasTableName()
                );
            }
        }
    }


    void updateSysGrain(T context) {
        try {
            Connection conn = context.getConn();
            Grain sys = score.getGrain(score.getSysSchemaName());
            createSysObjects(conn, sys);
            insertGrainRec(sys);
            if (!updateGrain(sys, connectionPool)) {
                throw new CelestaException("System grain '%s' update failed.", score.getSysSchemaName());
            }
        } catch (ParseException e) {
            throw new CelestaException("No '%s' grain definition found.", score.getSysSchemaName());
        }
    }

    void createSysObjects(Connection conn, Grain sys) throws ParseException {
        dbAdaptor.createSchemaIfNotExists(score.getSysSchemaName());
        dbAdaptor.createTable(conn, sys.getElement(getSchemasTableName(), BasicTable.class));
        dbAdaptor.createSysObjects(conn, score.getSysSchemaName());
    }

    private void insertGrainRec(Grain g) {
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

    private boolean decideToUpgrade(Grain g, GrainInfo gi, ConnectionPool connectionPool) {
        if (gi.lock) {
            return true;
        }

        if (gi.recover) {
            return updateGrain(g, connectionPool);
        }

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
                if (gi.length != g.getLength() || gi.checksum != g.getChecksum()) {
                    return updateGrain(g, connectionPool);
                }
            default:
                return true;
        }
    }

    /**
     * Performs update at the level of individual grain.
     *
     * @param g  grain
     * @param connectionPool  connection pool
     * @return
     */
    boolean updateGrain(Grain g, ConnectionPool connectionPool) {
        // выставление в статус updating
        schemaCursor.get(g.getName());
        schemaCursor.setState(ISchemaCursor.UPGRADING);
        schemaCursor.update();
        connectionPool.commit(schemaCursor.callContext().getConn());

        // теперь собственно обновление гранулы
        try {
            // Схему создаём, если ещё не создана.
            dbAdaptor.createSchemaIfNotExists(g.getName());

            beforeGrainUpdating(g);

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
            for (BasicTable t : g.getElements(BasicTable.class).values()) {
                if (updateTable(t, dbFKeys)) {
                    modifiedTablesMap.add(t.getName());
                }
            }

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
            for (BasicTable t : g.getElements(BasicTable.class).values()) {
                final Connection conn = schemaCursor.callContext().getConn();
                dbAdaptor.dropTableTriggersForMaterializedViews(conn, t);
                dbAdaptor.createTableTriggersForMaterializedViews(conn, t);
            }

            processGrainMeta(g);

            afterGrainUpdating(g);
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
            schemaCursor.setMessage(String.format("%s/%d/%08X: %s",
                    g.getVersion().toString(), g.getLength(), g.getChecksum(), e.getMessage() + newMsg));
            schemaCursor.update();
            connectionPool.commit(schemaCursor.callContext().getConn());
            return false;
        }
    }

    protected void beforeGrainUpdating(Grain g) { }

    protected void afterGrainUpdating(Grain g) { }

    protected abstract void processGrainMeta(Grain g);

    void createViews(Grain g) {
        Connection conn = schemaCursor.callContext().getConn();
        for (View v : g.getElements(View.class).values()) {
            dbAdaptor.createView(conn, v);
        }
    }

    void dropAllViews(Grain g) {
        Connection conn = schemaCursor.callContext().getConn();
        for (String viewName : dbAdaptor.getViewList(conn, g)) {
            dbAdaptor.dropView(conn, g.getName(), viewName);
        }
    }

    void createParameterizedViews(Grain g) {
        Connection conn = schemaCursor.callContext().getConn();
        for (ParameterizedView pv : g.getElements(ParameterizedView.class).values()) {
            dbAdaptor.createParameterizedView(conn, pv);
        }
    }

    void updateSequences(Grain g) {
        Connection conn = schemaCursor.callContext().getConn();

        for (SequenceElement s : g.getElements(SequenceElement.class).values()) {
            if (dbAdaptor.sequenceExists(conn, g.getName(), s.getName())) {
                DbSequenceInfo sequenceInfo = dbAdaptor.getSequenceInfo(conn, s);
                if (sequenceInfo.reflects(s)) {
                    dbAdaptor.alterSequence(conn, s);
                }
            } else {
                dbAdaptor.createSequence(conn, s);
            }
        }

    }

    void dropAllParameterizedViews(Grain g) {
        Connection conn = schemaCursor.callContext().getConn();
        for (String viewName : dbAdaptor.getParameterizedViewList(conn, g)) {
            dbAdaptor.dropParameterizedView(conn, g.getName(), viewName);
        }
    }

    void updateGrainFKeys(Grain g) {
        Connection conn = schemaCursor.callContext().getConn();
        Map<String, DbFkInfo> dbFKeys = new HashMap<>();
        for (DbFkInfo dbi : dbAdaptor.getFKInfo(conn, g)) {
            dbFKeys.put(dbi.getName(), dbi);
        }
        for (BasicTable t : g.getElements(BasicTable.class).values()) {
            if (t.isAutoUpdate()) {
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
        }
    }

    List<DbFkInfo> dropOrphanedGrainFKeys(Grain g) {
        Connection conn = schemaCursor.callContext().getConn();
        List<DbFkInfo> dbFKeys = dbAdaptor.getFKInfo(conn, g);
        Map<String, ForeignKey> fKeys = new HashMap<>();
        for (BasicTable t : g.getElements(BasicTable.class).values()) {
            for (ForeignKey fk : t.getForeignKeys()) {
                fKeys.put(fk.getConstraintName(), fk);
            }
        }
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

    void dropOrphanedGrainIndices(Grain g) {
        /*
         * In general this method repeats the code from updateGrainIndices but only
         * in the part of deletion of indices. It is needed to clear up all indices
         * that were undergone a deletion or a change before an update of table
         * structure is performed. That raises the probability of a successful outcome:
         * hanging at fields indices may interfere with the process.
         */
        final Connection conn = schemaCursor.callContext().getConn();
        Map<String, DbIndexInfo> dbIndices = dbAdaptor.getIndices(conn, g);
        Map<String, Index> myIndices = g.getIndices();
        // Deletion of indices that don't exist in the metadata.
        for (DbIndexInfo dBIndexInfo : dbIndices.values()) {
            if (!myIndices.containsKey(dBIndexInfo.getIndexName())) {
                dbAdaptor.dropIndex(g, dBIndexInfo);
            }
        }

        // Deletion of indices that will be changed later before tables update.
        for (Map.Entry<String, Index> e : myIndices.entrySet()) {
            DbIndexInfo dBIndexInfo = dbIndices.get(e.getKey());
            if (dBIndexInfo != null) {
                boolean reflects = dBIndexInfo.reflects(e.getValue());
                if (!reflects) {
                    dbAdaptor.dropIndex(g, dBIndexInfo);
                }

                // Deletion of indices at those fields that will undergo a change
                for (Map.Entry<String, Column<?>> ee : e.getValue().getColumns().entrySet()) {
                    DbColumnInfo ci = dbAdaptor.getColumnInfo(conn, ee.getValue());
                    if (ci == null || !ci.reflects(ee.getValue())) {
                        dbAdaptor.dropIndex(g, dBIndexInfo);
                        break;
                    }
                }
            }
        }
    }

    void updateGrainIndices(Grain g) {
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

    boolean updateTable(BasicTable t, List<DbFkInfo> dbFKeys) {
        // If table was compiled with option NO AUTOUPDATE then nothing is to be done
        if (!t.isAutoUpdate()) {
            return false;
        }

        final Connection conn = schemaCursor.callContext().getConn();

        if (!dbAdaptor.tableExists(conn, t.getGrain().getName(), t.getName())) {
            // Table doesn't exist in the DB, create it from scratch.
            dbAdaptor.createTable(conn, t);
            return true;
        }

        DbPkInfo pkInfo;
        Set<String> dbColumns = dbAdaptor.getColumns(conn, t);
        boolean modified = updateColumns(t, conn, dbColumns, dbFKeys);

        // For versioned tables synchronize 'recversion' field
        if (t instanceof Table) {
            Table tab = (Table) t;
            if (tab.isVersioned()) {
                if (dbColumns.contains(VersionedElement.REC_VERSION)) {
                    DbColumnInfo ci = dbAdaptor.getColumnInfo(conn, tab.getRecVersionField());
                    if (!ci.reflects(tab.getRecVersionField())) {
                        dbAdaptor.updateColumn(conn, tab.getRecVersionField(), ci);
                        modified = true;
                    }
                } else {
                    dbAdaptor.createColumn(conn, tab.getRecVersionField());
                    modified = true;
                }
            }
        }

        // Once again check the primary key, and if needed (in case it doesn't exist or
        // had been dropped) create it.
        pkInfo = dbAdaptor.getPKInfo(conn, t);
        if (pkInfo.isEmpty()) {
            dbAdaptor.createPK(conn, t);
        }

        dbAdaptor.updateVersioningTrigger(conn, t);

        return modified;
    }

    void updateMaterializedView(MaterializedView mv, boolean refTableIsModified) {
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

    private boolean updateColumns(
            TableElement t, final Connection conn, Set<String> dbColumns, List<DbFkInfo> dbFKeys) {
        // Таблица существует в базе данных, определяем: надо ли удалить
        // первичный ключ
        DbPkInfo pkInfo = dbAdaptor.getPKInfo(conn, t);
        boolean result = false;
        boolean keyDropped = pkInfo.isEmpty();
        if (!(pkInfo.reflects(t) || keyDropped)) {
            dropReferencedFKs(t, conn, dbFKeys);
            dbAdaptor.dropPk(conn, t, pkInfo.getName());
            keyDropped = true;
        }

        for (Map.Entry<String, Column<?>> e : t.getColumns().entrySet()) {
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
                        dbAdaptor.dropPk(conn, t, pkInfo.getName());
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

    private void dropReferencedFKs(TableElement t, Connection conn, List<DbFkInfo> dbFKeys) {
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
     * Buffer for storing grain information.
     */
    class GrainInfo {
        private boolean recover;
        private boolean lock;
        private int length;
        private int checksum;
        private VersionString version;
    }

}
