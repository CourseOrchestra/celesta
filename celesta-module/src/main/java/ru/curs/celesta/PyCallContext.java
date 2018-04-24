package ru.curs.celesta;

import org.python.core.*;
import ru.curs.celesta.dbutils.BasicDataAccessor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public final class PyCallContext extends CallContext<PyCallContext, PySessionContext, PyType, PyObject> {

    private static final String ERROR = "ERROR: %s";

    private final HashMap<PyString, PyObject> dataAccessorsCache = new HashMap<>();


    public PyCallContext(PyCallContextBuilder contextBuilder) {
        super(contextBuilder);
    }


    /**
     * Duplicates callcontext with another JDBC connection.
     */
    public PyCallContext getCopy() {
        return new PyCallContextBuilder()
                .setCelesta(this.celesta)
                .setConnectionPool(this.connectionPool)
                .setSesContext(this.sesContext)
                .setShowcaseContext(this.showcaseContext)
                .setScore(this.score)
                .setCurGrain(this.grain)
                .setProcName(this.procName)
                .setDbAdaptor(this.dbAdaptor)
                .setPermissionManager(this.permissionManager)
                .setLoggingManager(this.loggingManager)
                .createCallContext();
    }


    /**
     * Данные сессии.
     */
    public PyDictionary getData() {
        return sesContext.getData();
    }

    /**
     * Инициирует информационное сообщение.
     *
     * @param msg
     *            текст сообщения
     */
    public void message(String msg) {
        sesContext.addMessage(new CelestaMessage(CelestaMessage.INFO, msg));
    }

    /**
     * Инициирует информационное сообщение.
     *
     * @param msg
     *            текст сообщения
     * @param caption
     *            Заголовок окна.
     */
    public void message(String msg, String caption) {
        sesContext.addMessage(new CelestaMessage(CelestaMessage.INFO, msg, caption));
    }

    /**
     * Инициирует информационное сообщение.
     *
     * @param msg
     *            текст сообщения
     *
     * @param caption
     *            Заголовок окна.
     *
     * @param subkind
     *            Субтип сообщения.
     *
     */
    public void message(String msg, String caption, String subkind) {
        sesContext.addMessage(new CelestaMessage(CelestaMessage.INFO, msg, caption, subkind));
    }

    /**
     * Инициирует предупреждение.
     *
     * @param msg
     *            текст сообщения
     */
    public void warning(String msg) {
        sesContext.addMessage(new CelestaMessage(CelestaMessage.WARNING, msg));
    }

    /**
     * Инициирует предупреждение.
     *
     * @param msg
     *            текст сообщения
     * @param caption
     *            Заголовок окна.
     */
    public void warning(String msg, String caption) {
        sesContext.addMessage(new CelestaMessage(CelestaMessage.WARNING, msg, caption));
    }

    /**
     * Инициирует предупреждение.
     *
     * @param msg
     *            текст сообщения
     *
     * @param caption
     *            Заголовок окна.
     *
     * @param subkind
     *            Субтип сообщения.
     *
     */
    public void warning(String msg, String caption, String subkind) {
        sesContext.addMessage(new CelestaMessage(CelestaMessage.WARNING, msg, caption, subkind));
    }

    /**
     * Инициирует ошибку и вызывает исключение.
     *
     * @param msg текст сообщения
     */
    public void error(String msg) {
        sesContext.addMessage(new CelestaMessage(CelestaMessage.ERROR, msg));
        throw new CelestaException(ERROR, msg);
    }

    /**
     * Инициирует ошибку и вызывает исключение.
     *
     * @param msg текст сообщения
     * @param caption Заголовок окна.
     */
    public void error(String msg, String caption) {
        sesContext.addMessage(new CelestaMessage(CelestaMessage.ERROR, msg, caption));
        throw new CelestaException(ERROR, msg);
    }

    /**
     * Инициирует ошибку и вызывает исключение.
     *
     * @param msg
     *            текст сообщения
     * @param caption
     *            Заголовок окна.
     * @param subkind
     *            Субтип сообщения.
     */
    public void error(String msg, String caption, String subkind) {
        sesContext.addMessage(new CelestaMessage(CelestaMessage.ERROR, msg, caption, subkind));
        throw new CelestaException(ERROR, msg);
    }


    @Override
    public PyObject create(final PyType dataAccessorClass) {
        PyString classId = dataAccessorClass.__str__();
        PyObject result = dataAccessorsCache.computeIfAbsent(classId, s -> dataAccessorClass.__call__(Py.java2py(this)));
        BasicDataAccessor basicDataAccessor = (BasicDataAccessor) result.__tojava__(BasicDataAccessor.class);
        basicDataAccessor.clear();
        return result;
    }


    @Override
    public void removeFromCache(final BasicDataAccessor dataAccessor) {
        Iterator<Map.Entry<PyString, PyObject>> i = dataAccessorsCache.entrySet().iterator();
        while (i.hasNext()) {
            Map.Entry<PyString, PyObject> e = i.next();
            BasicDataAccessor basicDataAccessor = (BasicDataAccessor) e.getValue().__tojava__(BasicDataAccessor.class);
            if (dataAccessor.equals(basicDataAccessor)) {
                i.remove();
            }
        }
    }

    public static PyCallContextBuilder builder() {
        return new PyCallContextBuilder();
    }

    public static final class PyCallContextBuilder
            extends CallContext.CallContextBuilder<PyCallContextBuilder, PyCallContext, PySessionContext> {

        @Override
        protected PyCallContextBuilder getThis() {
            return this;
        }

        @Override
        public PyCallContext createCallContext() {
            return new PyCallContext(this);
        }


    }
}
