package ru.curs.celesta;

import ru.curs.celesta.score.discovery.DefaultScoreDiscovery;
import ru.curs.celesta.score.discovery.ScoreDiscovery;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Function;

public class Celesta extends AbstractCelesta<JSessionContext> implements ICelesta {

    public static final JSessionContext SYSTEM_SESSION = new JSessionContext(
            SessionContext.SYSTEM_USER_ID, SessionContext.SYSTEM_SESSION_ID
    );

    private final ScoreDiscovery scoreDiscovery = new DefaultScoreDiscovery();
    private final CelestaProcExecutor celestaProcExecutor;

    private Celesta(AppSettings appSettings) {
        super(appSettings, 3);

        System.out.printf("Celesta initialization: phase 3/3 annotation scanning...");
        Set<Method> scannedMethods = AnnotationScanner.scan(appSettings.getCelestaScan());
        CelestaProcProvider procProvider = new CelestaProcProvider(scannedMethods);
        this.celestaProcExecutor = new CelestaProcExecutor(procProvider, this::callContext);
        System.out.println("done.");
    }


    public static Celesta createInstance(Properties properties) {
        AppSettings appSettings = preInit(properties);
        return new Celesta(appSettings);
    }

    public static Celesta createInstance() {
        Properties properties = loadPropertiesDynamically();
        return createInstance(properties);
    }

    private static AppSettings preInit(Properties properties) {
        System.out.print("Celesta pre-initialization: phase 1/2 system settings reading...");
        AppSettings appSettings = new AppSettings(properties);
        System.out.println("done.");
        return appSettings;
    }

    private static Properties loadPropertiesDynamically() {
        // Разбираемся с настроечным файлом: читаем его и превращаем в
        // Properties.
        Properties properties = new Properties();
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            InputStream in = loader.getResourceAsStream(FILE_PROPERTIES);
            if (in == null) {
                throw new CelestaException(
                        String.format("Couldn't find file %s on classpath.", FILE_PROPERTIES)
                );
            }
            try {
                properties.load(in);
            } finally {
                in.close();
            }
        } catch (IOException e) {
            throw new CelestaException(
                    String.format("IOException while reading %s file: %s", FILE_PROPERTIES, e.getMessage())
            );
        }

        return properties;
    }


    @Override
    ScoreDiscovery getScoreDiscovery() {
        return this.scoreDiscovery;
    }

    @Override
    public JSessionContext getSystemSessionContext() {
        return SYSTEM_SESSION;
    }

    @Override
    JSessionContext sessionContext(String userId, String sessionId) {
        return new JSessionContext(userId, sessionId);
    }

    public Object runProc(String sessionId, String qualifier, Object... args) {
        JSessionContext sessionContext = this.getSessionContext(sessionId);
        return this.celestaProcExecutor.runProc(sessionContext, qualifier, args);
    }

    public Future<Object> runProcAsync(String sessionId, String qualifier, long delay, Object... args) {
        return this.runAsyncInner(sessionId, delay, currentSessionId -> this.runProc(currentSessionId, qualifier, args));
    }

    public <T> T run(String sessionId, Function<CallContext, T> f) {
        JSessionContext sessionContext = this.getSessionContext(sessionId);
        try (CallContext cc = callContext(sessionContext)) {
            return f.apply(cc);
        }
    }

    public <T> Future<T> runAsync(String sessionId, Function<CallContext, T> f, long delay) {
        return this.runAsyncInner(sessionId, delay, currentSessionId -> this.run(currentSessionId, f));
    }

    private <T> Future<T> runAsyncInner(String sessionId, long delay, Function<String, T> f) {
        final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        JSessionContext sessionContext = this.sessions.get(sessionId);
        Thread shutDownHook = new Thread(scheduledExecutor::shutdown);
        Runtime.getRuntime().addShutdownHook(shutDownHook);

        Callable<T> callable = () -> {
            String currentSesId = null;
            try {

                if (sessions.containsKey(sessionId)) {
                    currentSesId = sessionId;
                } else {
                    currentSesId = String.format("TEMP%08X", ThreadLocalRandom.current().nextInt());
                    login(currentSesId, sessionContext.getUserId());
                }

                return f.apply(currentSesId);
            } catch (Exception e) {
                System.out.println("Exception while executing async task:" + e.getMessage());
                throw e;
            } finally {
                Runtime.getRuntime().removeShutdownHook(shutDownHook);
                scheduledExecutor.shutdown();

                if (currentSesId != null && sessions.containsKey(currentSesId))
                    logout(currentSesId, false);
            }
        };

        return scheduledExecutor.schedule(callable, delay, TimeUnit.MILLISECONDS);
    }

}
