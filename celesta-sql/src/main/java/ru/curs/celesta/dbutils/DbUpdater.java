package ru.curs.celesta.dbutils;

import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ConnectionPool;
import ru.curs.celesta.ICallContext;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.celesta.dbutils.meta.*;
import ru.curs.celesta.event.TriggerQuery;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.*;
import ru.curs.celesta.syscursors.ISchemaElementCursor;
import ru.curs.celesta.syscursors.ISchemaCursor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public abstract class DbUpdater<T extends ICallContext> {

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
    protected ISchemaElementCursor schemaElementCursor;

    public DbUpdater(ConnectionPool connectionPool, AbstractScore score, boolean forceDdInitialize, DBAdaptor dbAdaptor) {
        this.connectionPool = connectionPool;
        this.score = score;
        this.forceDdInitialize = forceDdInitialize;
        this.dbAdaptor = dbAdaptor;
    }


    protected abstract T createContext();

    protected abstract void initDataAccessors(T context);

    protected abstract String getSchemasTableName();
    protected abstract String getSchemaElementsTableName();

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
            if (dbAdaptor.userTablesExist() && !forceDdInitialize)
                throw new CelestaException("No %s.%s table found in non-empty database.",
                        score.getSysSchemaName(), getSchemasTableName());
            // Если база вообще пустая, то создаём системные таблицы.
            updateSysGrain(context);
        }
    }

    /**
     * Выполняет обновление структуры БД на основе разобранной объектной модели.
     *
     * @в случае ошибки обновления.
     */
    public void updateDb() {
        String sysSchemaName = this.score.getSysSchemaName();

        try (T context = createContext()) {
            updateSystemSchema(context);

            // Теперь собираем в память информацию о гранулах на основании того,
            // что
            // хранится в таблице grains.
            Map<String, GrainInfo> dbGrains = new HashMap<>();
            while (schemaCursor.nextInSet()) {

                if (!(EXPECTED_STATUSES.contains(schemaCursor.getState())))
                    throw new CelestaException("Cannot proceed with database upgrade: there are %s " +
                            "not in 'ready', 'recover' or 'lock' state.", getSchemasTableName());
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

            Set<Grain> notSystemGrains = score.getGrains().values().stream()
                    .filter(g -> !g.getName().equals(sysSchemaName))
                    .collect(Collectors.toSet());
            updateGrains(notSystemGrains, dbGrains, true);
        }
    }

    void updateGrains(Set<Grain> grains, Map<String, GrainInfo> dbGrains, boolean isNotSystem) {
        String sysSchemaName = this.score.getSysSchemaName();
        boolean success = true;

        final List<GrainElement> grainElementsToUpdate = new ArrayList<>();
        final Map<Grain, List<DbFkInfo>> grainToDbFkInfoList = new HashMap<>();
        final Map<Grain, Collection<? extends GrainElement>> notProcessedElements = new HashMap<>();
        final Set<Table> modifiedTables = new HashSet<>();

        for (Grain g : grains) {

            if (isNotSystem) {
                GrainInfo gi = dbGrains.get(g.getName());

                if (gi == null) {
                    insertGrainRec(g);
                } else {
                    // The record exists
                    // The decision to upgrade is based on the version and checksum.
                    if (!needToUpgrade(g, gi)) {
                        // No need for update.
                        continue;
                    }
                }
            }

            beforeGrainUpdating(g);

            // Delete all views
            dropAllViews(g);
            // Delete all parameterized views
            dropAllParameterizedViews(g);

            // Выполняем удаление ненужных индексов, чтобы облегчить задачу
            // обновления столбцов на таблицах.
            dropOrphanedGrainIndices(g);

            // Сбрасываем внешние ключи, более не включённые в метаданные
            grainToDbFkInfoList.put(g, dropOrphanedGrainForeignKeysAndGetActual(g));


            Collection<SequenceElement> sequenceElements = g.getElements(SequenceElement.class).values();
            Collection<Table> tables = g.getElements(Table.class).values();
            Collection<Index> indices = g.getElements(Index.class).values();
            Collection<View> views = g.getElements(View.class).values();
            Collection<ParameterizedView> parameterizedViews = g.getElements(ParameterizedView.class).values();
            Collection<MaterializedView> materializedViews = g.getElements(MaterializedView.class).values();

            grainElementsToUpdate.addAll(sequenceElements);
            grainElementsToUpdate.addAll(tables);
            grainElementsToUpdate.addAll(indices);
            grainElementsToUpdate.addAll(views);
            grainElementsToUpdate.addAll(parameterizedViews);
            grainElementsToUpdate.addAll(materializedViews);

            Set<GrainElement> allGrainElements = new HashSet<>();
            allGrainElements.addAll(sequenceElements);
            allGrainElements.addAll(tables);
            allGrainElements.addAll(indices);
            allGrainElements.addAll(views);
            allGrainElements.addAll(parameterizedViews);
            allGrainElements.addAll(materializedViews);


            notProcessedElements.put(g, allGrainElements);
        }

        GrainElementUpdatingComparator comparator = new GrainElementUpdatingComparator(this.score);
        grainElementsToUpdate.sort(comparator);

        final Map<Grain, Set<GrainElement>> failedMap = new HashMap<>();
        final Set<Grain> upgradingGrains = new HashSet<>();

        for (GrainElement ge : grainElementsToUpdate) {
            Grain g = ge.getGrain();

            if (!upgradingGrains.contains(g)) {
                upgradingGrains.add(g);
                // Create a schema, if not already created.
                schemaCursor.get(g.getName());
                schemaCursor.setState(ISchemaCursor.UPGRADING);
                schemaCursor.update();
                connectionPool.commit(schemaCursor.callContext().getConn());

                dbAdaptor.createSchemaIfNotExists(g.getName());
            }

            success = updateGrainElement(ge, grainToDbFkInfoList, modifiedTables) && success;

            if (!success) {
                failedMap.computeIfAbsent(g, lambdaG -> new LinkedHashSet<>(Collections.singletonList(ge)))
                        .add(ge);
            } else {
                notProcessedElements.get(g).remove(ge);

                if (notProcessedElements.get(g).isEmpty()) {
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
                }
            }

        }

        if (!success) {

            failedMap.keySet().forEach(g -> {

                String message = failedMap.get(g).stream()
                        .map(GrainElement::getName)
                        .collect(Collectors.joining(", "));

                this.schemaCursor.get(g.getName());
                this.schemaCursor.setState(ISchemaCursor.ERROR);
                this.schemaCursor.setMessage(
                        String.format(
                                "%s/%d/%08X: failed to update elements %s",
                                g.getVersion().toString(), g.getLength(), g.getChecksum(), message
                        )
                );
                this.schemaCursor.update();
                this.connectionPool.commit(this.schemaCursor.callContext().getConn());
            });

            throw new CelestaException(
                    "Not all %s were updated successfully, see %s.%s table data for details.",
                    getSchemasTableName(), sysSchemaName, getSchemaElementsTableName()
            );
        }
    }

    void updateSysGrain(T context) {
        try {
            Connection conn = context.getConn();
            Grain sys = score.getGrain(score.getSysSchemaName());
            createSysObjects(conn, sys);
            insertGrainRec(sys);
            this.updateGrains(Collections.singleton(sys), null, false);
        } catch (ParseException e) {
            throw new CelestaException("No '%s' grain definition found.", score.getSysSchemaName());
        }
    }

    void createSysObjects(Connection conn, Grain sys) throws ParseException {
        dbAdaptor.createSchemaIfNotExists(score.getSysSchemaName());
        dbAdaptor.createTable(conn, sys.getElement(getSchemasTableName(), Table.class));
        dbAdaptor.createTable(conn, sys.getElement(getSchemaElementsTableName(), Table.class));
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

    private void insertGrainElementRec(GrainElement ge) {
        schemaElementCursor.init();
        schemaElementCursor.setId(ge.getName());
        schemaElementCursor.setGrainId(ge.getGrain().getName());
        schemaElementCursor.setType(ge.getClass().getSimpleName());
        schemaElementCursor.setLastModified(new Date());
        schemaElementCursor.setState(ISchemaElementCursor.UPGRADING);
        schemaElementCursor.setMessage("");
        schemaElementCursor.insert();
    }

    boolean needToUpgrade(Grain g, GrainInfo gi) {
        if (gi.lock)
            return false;

        if (gi.recover)
            return true;

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
                return true;
            case EQUALS:
                // Версия не изменилась: апгрейдим лишь в том случае, если
                // изменилась контрольная сумма.
                return (gi.length != g.getLength() || gi.checksum != g.getChecksum());
            default:
                return true;
        }
    }

    private boolean updateGrainElement(GrainElement ge, Map<Grain, List<DbFkInfo>> dbFKeysMap,
                                       Set<Table> modifiedTablesSet) {

        Grain g = ge.getGrain();

        if (this.schemaElementCursor.tryGet(ge.getName(), g.getName())) {
            // Setting of status to UPDATING state
            this.schemaElementCursor.setState(ISchemaElementCursor.UPGRADING);
            this.schemaElementCursor.update();
        } else {
            this.insertGrainElementRec(ge);
        }
        this.connectionPool.commit(this.schemaCursor.callContext().getConn());

        try {
            final Connection conn = this.schemaCursor.callContext().getConn();
            // TODO: We need to create separated elementUpgrader
            if (ge instanceof SequenceElement) {
                SequenceElement s = (SequenceElement) ge;
                updateSequence(s);
            } else if (ge instanceof Table) {
                Table t = (Table) ge;
                List<DbFkInfo> dbFKeys = dbFKeysMap.get(g);
                boolean tableIsUpdated = updateTable(t, dbFKeys);

                if (tableIsUpdated) {
                    modifiedTablesSet.add(t);
                }

                if (t.isAutoUpdate()) {
                    for (ForeignKey fk : t.getForeignKeys()) {
                        updateFk(fk, dbFKeys, conn);
                    }
                }
            } else if (ge instanceof Index) {
                // TODO: Optimization is needed
                Map<String, DbIndexInfo> dbIndices = this.dbAdaptor.getIndices(conn, g);
                final Index index = (Index) ge;
                updateIndex(index, dbIndices);
            } else if (ge instanceof ParameterizedView) {
                ParameterizedView parameterizedView = (ParameterizedView)ge;
                this.dbAdaptor.createParameterizedView(conn, parameterizedView);
            } else if (ge instanceof View) {
                View view = (View)ge;
                this.dbAdaptor.createView(conn, view);
            } else if (ge instanceof MaterializedView) {
                MaterializedView materializedView = (MaterializedView)ge;
                Table table = materializedView.getRefTable().getTable();
                updateMaterializedView(materializedView, modifiedTablesSet.contains(table));
            }

            this.schemaElementCursor.setState(ISchemaElementCursor.READY);
            this.schemaElementCursor.setMessage("");
            this.schemaElementCursor.setLastModified(new Date());
            this.schemaElementCursor.update();
            this.connectionPool.commit(this.schemaCursor.callContext().getConn());

            return true;
        } catch (Exception e) {
            String newMsg = "";
            try {
                this.schemaElementCursor.callContext().getConn().rollback();
            } catch (SQLException e1) {
                newMsg = ", " + e1.getMessage();
            }
            // Если что-то пошло не так
            this.schemaElementCursor.setState(ISchemaElementCursor.ERROR);
            this.schemaElementCursor.setMessage(String.format(
                    "%s/%d/%08X: %s",
                    g.getVersion().toString(), g.getLength(), g.getChecksum(), e.getMessage() + newMsg)
            );
            this.schemaElementCursor.update();
            this.connectionPool.commit(this.schemaElementCursor.callContext().getConn());
            return false;
        }
    }

    protected void beforeGrainUpdating(Grain g) { }

    protected void afterGrainUpdating(Grain g) { }

    protected abstract void processGrainMeta(Grain g);

    void createViews(Grain g) {
        Connection conn = schemaCursor.callContext().getConn();
        for (View v : g.getElements(View.class).values())
            dbAdaptor.createView(conn, v);
    }

    void dropAllViews(Grain g) {
        Connection conn = schemaCursor.callContext().getConn();
        for (String viewName : dbAdaptor.getViewList(conn, g))
            dbAdaptor.dropView(conn, g.getName(), viewName);
    }

    void createParameterizedViews(Grain g) {
        Connection conn = schemaCursor.callContext().getConn();
        for (ParameterizedView pv : g.getElements(ParameterizedView.class).values())
            dbAdaptor.createParameterizedView(conn, pv);
    }

    void updateSequence(SequenceElement s) {
        Grain g = s.getGrain();
        Connection conn = schemaCursor.callContext().getConn();
        if (dbAdaptor.sequenceExists(conn, g.getName(), s.getName())) {
            DbSequenceInfo sequenceInfo = dbAdaptor.getSequenceInfo(conn, s);
            if (sequenceInfo.reflects(s))
                dbAdaptor.alterSequence(conn, s);
        } else {
            dbAdaptor.createSequence(conn, s);
        }
    }

    void updateSequences(Grain g) {
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

    void dropAllParameterizedViews(Grain g) {
        Connection conn = schemaCursor.callContext().getConn();
        for (String viewName : dbAdaptor.getParameterizedViewList(conn, g))
            dbAdaptor.dropParameterizedView(conn, g.getName(), viewName);
    }

    void updateFk(ForeignKey fk, List<DbFkInfo> dbFKeys, Connection conn) {
        Grain g = fk.getParentTable().getGrain();
        Optional<DbFkInfo> dbFkInfoOpt = dbFKeys.stream()
                .filter(dbFkInfo -> dbFkInfo.getName().equals(fk.getConstraintName()))
                .findFirst();

        if (dbFkInfoOpt.isPresent()) {
            // FK is found in the database, update if necessary.
            DbFkInfo dbi = dbFkInfoOpt.get();
            if (!dbi.reflects(fk)) {
                dbAdaptor.dropFK(conn, g.getName(), dbi.getTableName(), dbi.getName());
                dbAdaptor.createFK(conn, fk);
            }
        } else {
            // FK is not detected in the database, creation from scratch
            dbAdaptor.createFK(conn, fk);
        }
    }

    void updateGrainFKeys(Grain g) {
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

    List<DbFkInfo> dropOrphanedGrainForeignKeysAndGetActual(Grain g) {
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

    void dropOrphanedGrainIndices(Grain g) {
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

    void updateIndex(Index index, Map<String, DbIndexInfo> dbIndices) {
        final Connection conn = schemaCursor.callContext().getConn();

        DbIndexInfo dBIndexInfo = dbIndices.get(index.getName());
        if (dBIndexInfo != null) {
            // The database contains an index with this name,
            // it is necessary to check fields and re-create the index if necessary.
            boolean reflects = dBIndexInfo.reflects(index);
            if (!reflects) {
                dbAdaptor.dropIndex(index.getGrain(), dBIndexInfo);
                dbAdaptor.createIndex(conn, index);
            }
        } else {
            // TCreating an index that did not exist before.
            dbAdaptor.createIndex(conn, index);
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

    boolean updateTable(Table t, List<DbFkInfo> dbFKeys) {
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
            } catch (CelestaException e) {
                throw new CelestaException("Updating table %s.%s failed: %s.", t.getGrain().getName(), t.getName(),
                        e.getMessage());
            }

        dbAdaptor.updateVersioningTrigger(conn, t);

        return modified;
    }

    void updateMaterializedView(MaterializedView mv, boolean refTableIsModified) {
        final Connection conn = this.schemaCursor.callContext().getConn();

        boolean mViewExists = this.dbAdaptor.tableExists(conn, mv.getGrain().getName(), mv.getName());

        if (mViewExists) {

            if (!refTableIsModified) {

                //В теле insert-триггера должна храниться контрольная сумма.
                String insertTriggerName = mv.getTriggerName(TriggerType.POST_INSERT);
                TriggerQuery query = new TriggerQuery()
                        .withSchema(mv.getGrain().getName())
                        .withTableName(mv.getRefTable().getTable().getName())
                        .withName(insertTriggerName);

                Optional<String> insertTriggerBody = this.dbAdaptor.getTriggerBody(conn, query);
                boolean checksumIsMatched = insertTriggerBody.map(b -> b.contains(
                        String.format(MaterializedView.CHECKSUM_COMMENT_TEMPLATE, mv.getChecksum())
                        )).orElse(false);
                if (checksumIsMatched) {
                    return;
                }
            }

            //Удаляем materialized view
            this.dbAdaptor.dropTable(conn, mv);
        }

        //1. Таблицы не существует в базе данных, создаём с нуля.
        this.dbAdaptor.createTable(conn, mv);
        //2. Проинициализировать данные материального представления
        this.dbAdaptor.initDataForMaterializedView(conn, mv);

        this.dbAdaptor.dropTableTriggerForMaterializedView(conn, mv);
        this.dbAdaptor.createTableTriggerForMaterializedView(conn, mv);
    }

    private boolean updateColumns(TableElement t, final Connection conn, Set<String> dbColumns, List<DbFkInfo> dbFKeys)
            {
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
