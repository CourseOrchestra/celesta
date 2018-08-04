package ru.curs.celesta.vintage;

import org.python.core.PyObject;
import ru.curs.celesta.*;
import ru.curs.celesta.event.TriggerDispatcher;
import ru.curs.celesta.java.JSessionContext;
import ru.curs.celesta.score.Score;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * It's good to use celesta-java module instead of this.
 * Maybe celesta-jython will not be supported in the future.
 */
@Deprecated
public final class Celesta implements VintageCelesta, AutoCloseable {

    private final VintageAppSettings appSettings;
    private final ru.curs.celesta.Celesta jythonCelesta;
    private final ru.curs.celesta.java.Celesta javaCelesta;

    private Celesta(VintageAppSettings appSettings) {
        this.appSettings = appSettings;

        final Properties jythonProperties = appSettings.getSetupProperties();
        this.jythonCelesta = ru.curs.celesta.Celesta.createInstance(jythonProperties);

        final Properties javaProperties = new Properties(jythonProperties);
        final String javaScorePath = appSettings.getJavaScorePath();

        if (javaScorePath != null && !javaScorePath.trim().isEmpty()) {
            javaProperties.put("score.path", appSettings.getJavaScorePath());
            this.javaCelesta = ru.curs.celesta.java.Celesta.createInstance(javaProperties);
        } else {
            this.javaCelesta = null;
        }
    }

    public static Celesta createInstance(Properties properties) {
        VintageAppSettings appSettings = preInit(properties);
        return new Celesta(appSettings);
    }

    private static VintageAppSettings preInit(Properties properties) {
        System.out.print("Celesta pre-initialization: phase 1/2 system settings reading...");
        VintageAppSettings appSettings = new VintageAppSettings(properties);
        System.out.println("done.");
        return appSettings;
    }

    public static Celesta createInstance() {
        Properties properties = ru.curs.celesta.java.Celesta.loadPropertiesDynamically();
        return createInstance(properties);
    }

    @Override
    public Object runProc(String sessionId, String qualifier, Object... args) {
        return this.javaCelesta.runProc(sessionId, qualifier, args);
    }

    @Override
    public Future<Object> runProcAsync(String sessionId, String qualifier, long delay, Object... args) {
        return this.javaCelesta.runProcAsync(sessionId, qualifier, delay, args);
    }

    @Override
    public <T> T run(String sessionId, Function<CallContext, T> f) {
        return this.javaCelesta.run(sessionId, f);
    }

    @Override
    public <T> Future<T> runAsync(String sessionId, Function<CallContext, T> f, long delay) {
        return this.javaCelesta.runAsync(sessionId, f, delay);
    }

    @Override
    public PyObject runPython(String sesId, String proc, Object... param) {
        return this.jythonCelesta.runPython(sesId, proc, param);
    }

    @Override
    public PyObject runPython(String sesId, CelestaMessage.MessageReceiver rec, ShowcaseContext sc, String proc, Object... param) {
        return this.jythonCelesta.runPython(sesId, rec, sc, proc, param);
    }

    @Override
    public Future<PyObject> runPythonAsync(String sesId, String proc, long delay, Object... param) {
        return this.jythonCelesta.runPythonAsync(sesId, proc, delay, param);
    }

    @Override
    public TriggerDispatcher getTriggerDispatcher() {
        return this.jythonCelesta.getTriggerDispatcher();
    }

    @Override
    public TriggerDispatcher getJavaTriggerDispatcher() {
        return this.javaCelesta.getTriggerDispatcher();
    }

    @Override
    public Score getScore() {
        return this.jythonCelesta.getScore();
    }

    @Override
    public Score getJavaScore() {
        return this.javaCelesta.getScore();
    }

    @Override
    public Properties getSetupProperties() {
        return this.appSettings.getSetupProperties();
    }

    @Override
    public CallContext callContext() {
        return this.jythonCelesta.callContext();
    }

    @Override
    public CallContext getJavaCallContext() {
        return this.javaCelesta.callContext();
    }

    @Override
    public SessionContext getSystemSessionContext() {
        return this.jythonCelesta.getSystemSessionContext();
    }

    @Override
    public SessionContext getJavaSystemSessionContext() {
        return this.javaCelesta.getSystemSessionContext();
    }

    public void login(String sessionId, String userId) {
        this.jythonCelesta.login(sessionId, userId);
    }

    public void javaLogin(String sessionId, String userId) {
        this.javaCelesta.login(sessionId, userId);
    }

    public PySessionContext logout(String sessionId, boolean timeout) {
        return this.jythonCelesta.logout(sessionId, timeout);
    }

    public JSessionContext javaLogout(String sessionId, boolean timeout) {
        return this.javaCelesta.logout(sessionId, timeout);
    }

    public boolean isProfilemode() {
        return this.jythonCelesta.isProfilemode();
    }

    public boolean nullsFirst() {
        return this.jythonCelesta.nullsFirst();
    }

    public void setProfilemode(boolean profilemode) {
        this.jythonCelesta.setProfilemode(profilemode);
    }

    public void failedLogin(String userId) {
        this.jythonCelesta.failedLogin(userId);
    }

    public void javaFailedLogin(String userId) {
        this.javaCelesta.failedLogin(userId);
    }

    public Collection<CallContext> getActiveContexts() {
        return this.jythonCelesta.getActiveContexts();
    }

    public Collection<CallContext> getJavaActiveContexts() {
        return this.javaCelesta.getActiveContexts();
    }

    @Override
    public void close() {
        this.jythonCelesta.close();
        if (this.javaCelesta != null) {
            this.javaCelesta.close();
        }
    }

    public CallContext callContext(PySessionContext sessionContext) {
        return this.jythonCelesta.callContext(sessionContext);
    }

    public CallContext javaCallContext(JSessionContext sessionContext) {
        return this.javaCelesta.callContext(sessionContext);
    }
}
