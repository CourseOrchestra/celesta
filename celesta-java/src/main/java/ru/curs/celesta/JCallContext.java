package ru.curs.celesta;

import ru.curs.celesta.dbutils.BasicDataAccessor;

import java.util.HashMap;

public class JCallContext extends CallContext<
        JCallContext, JSessionContext, Class<? extends BasicDataAccessor>, BasicDataAccessor
        > {

    private final HashMap<Class<? extends BasicDataAccessor>, BasicDataAccessor> dataAccessorsCache = new HashMap<>();

    public JCallContext(JCallContextBuilder contextBuilder) {
        super(contextBuilder);
    }

    @Override
    public JCallContext getCopy() {
        return null;
    }

    @Override
    public BasicDataAccessor create(Class<? extends BasicDataAccessor> dataAccessorClass) {
        BasicDataAccessor result = dataAccessorsCache.computeIfAbsent(dataAccessorClass,
                aClass -> {
                    try {
                        return dataAccessorClass.getDeclaredConstructor(CallContext.class)
                                .newInstance(this);
                    } catch (Exception e) {
                        throw new CelestaException(e);
                    }
                });

        result.clear();
        return result;
    }

    @Override
    public void removeFromCache(BasicDataAccessor dataAccessor) {
        dataAccessorsCache.remove(dataAccessor.getClass());
    }

    @Override
    public CallContextBuilder getBuilder() {
        return null;
    }

    public static JCallContextBuilder builder() {
        return new JCallContextBuilder();
    }

    public static final class JCallContextBuilder extends CallContext.CallContextBuilder<JCallContextBuilder, JCallContext, JSessionContext> {
        @Override
        protected JCallContextBuilder getThis() {
            return this;
        }

        @Override
        public JCallContext createCallContext() {
            return new JCallContext(this);
        }
    }
}
