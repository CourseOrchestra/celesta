package ru.curs.celesta;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

class CelestaProcExecutor {

    private final CelestaProcProvider celestaProcProvider;
    private final Function<JSessionContext, CallContext> callContextProvider;

    public CelestaProcExecutor(CelestaProcProvider celestaProcProvider, Function<JSessionContext, CallContext> callContextProvider) {
        this.celestaProcProvider = celestaProcProvider;
        this.callContextProvider = callContextProvider;
    }

    public Object runProc(JSessionContext sessionContext, String qualifier, Object... args) {
        CelestaProcMeta procMeta = this.celestaProcProvider.get(qualifier);

        if (procMeta == null)
            throw new CelestaException("No celesta procedure found for qualifier %s", qualifier);

        Method method = procMeta.getMethod();
        Class methodClass = method.getDeclaringClass();

        try {

            final Object underlyingObject;

            if (procMeta.isClassInstantiationNeeded()) {
                underlyingObject = methodClass.newInstance();
            } else {
                underlyingObject = methodClass;
            }

            if (procMeta.isCallContextInjectionNeeded()) {
                try (CallContext callContext = callContextProvider.apply(sessionContext)) {
                    LinkedList list = new LinkedList<>(Arrays.asList(args));
                    list.addFirst(callContext);
                    args = list.toArray();
                    return method.invoke(underlyingObject, args);
                }
            } else {
                return method.invoke(underlyingObject, args);
            }
        } catch (Exception e) {
            throw new CelestaException(String.format("Error of the executing celesta procedure %s", qualifier), e);
        }

    }
}

class CelestaProcMeta {
    private final boolean isCallContextInjectionNeeded;
    private final boolean isClassInstantiationNeeded;
    private final Method method;

    public CelestaProcMeta(boolean isCallContextInjectionNeeded, boolean isClassInstantiationNeeded, Method method) {
        this.isCallContextInjectionNeeded = isCallContextInjectionNeeded;
        this.isClassInstantiationNeeded = isClassInstantiationNeeded;
        this.method = method;
    }

    public boolean isCallContextInjectionNeeded() {
        return isCallContextInjectionNeeded;
    }

    public boolean isClassInstantiationNeeded() {
        return isClassInstantiationNeeded;
    }

    public Method getMethod() {
        return method;
    }

    @Override
    public int hashCode() {
        return method.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (this == obj)
            return true;

        if (obj instanceof CelestaProcMeta) {
            CelestaProcMeta that = (CelestaProcMeta) obj;
            return this.method.equals(that.method);
        }

        return false;
    }
}

class CelestaProcProvider {

    private final Map<String, CelestaProcMeta> celestaProcedures = new LinkedHashMap<>();

    public CelestaProcProvider(Set<Method> methods) {

        methods.forEach(
                method -> {
                    Class declaringClass = method.getDeclaringClass();
                    String qualifier = String.format("%s#%s", declaringClass.getName(), method.getName());

                    if (this.celestaProcedures.containsKey(qualifier))
                        throw new CelestaException("Duplicated celesta procedure detected - %s ", qualifier);

                    int classModifiers = declaringClass.getModifiers();
                    int methodModifiers = method.getModifiers();

                    if (!Modifier.isPublic(classModifiers))
                        throw new CelestaException(
                                "Declaring class of celesta procedure %s must have public access", qualifier
                        );
                    if (!Modifier.isPublic(methodModifiers))
                        throw new CelestaException(
                                "Method of celesta procedure %s must have public access", qualifier
                        );

                    final boolean needClassInstantiation = !Modifier.isStatic(methodModifiers);

                    if (needClassInstantiation && Modifier.isAbstract(classModifiers))
                        throw new CelestaException(
                                "Declaring class of celesta procedure %s can't be abstract", qualifier
                        );

                    Predicate<Class> isCallContextClass = CallContext.class::equals;

                    final boolean needInjectCallContext = Arrays.stream(method.getParameterTypes())
                            .anyMatch(isCallContextClass);

                    if (needInjectCallContext) {
                        if (!method.getParameterTypes()[0].equals(CallContext.class)) {
                            throw new CelestaException(
                                    "CallContext must be the first argument declared in procedure %s", qualifier
                            );
                        }

                        if (Arrays.stream(method.getParameterTypes()).filter(isCallContextClass).count() > 1) {
                            throw new CelestaException(
                                    "Procedure %s can't have more than one CallContext arguments ", qualifier
                            );
                        }

                    }

                    CelestaProcMeta meta = new CelestaProcMeta(needInjectCallContext, needClassInstantiation, method);

                    this.celestaProcedures.put(qualifier, meta);
                }
        );
    }

    public CelestaProcMeta get(String qualifier) {
        return celestaProcedures.get(qualifier);
    }
}
