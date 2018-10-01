package ru.curs.celesta.plugin;

import com.squareup.javapoet.*;
import org.apache.commons.lang3.StringUtils;
import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ICelesta;
import ru.curs.celesta.dbutils.*;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.*;

import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class CursorGenerator {


    private static final HashMap<
            Class<? extends GrainElement>,
            Function<GrainElement, Class<? extends BasicDataAccessor>>
            >
            GRAIN_ELEMENTS_TO_DATA_ACCESSORS = new HashMap<>();

    static {
        GRAIN_ELEMENTS_TO_DATA_ACCESSORS.put(SequenceElement.class, ge -> Sequence.class);
        GRAIN_ELEMENTS_TO_DATA_ACCESSORS.put(Table.class, ge -> {
            Table t = (Table) ge;
            if (t.isReadOnly())
                return ReadOnlyTableCursor.class;
            else
                return Cursor.class;
        });
        GRAIN_ELEMENTS_TO_DATA_ACCESSORS.put(View.class, ge -> ViewCursor.class);
        GRAIN_ELEMENTS_TO_DATA_ACCESSORS.put(MaterializedView.class, ge -> MaterializedViewCursor.class);
        GRAIN_ELEMENTS_TO_DATA_ACCESSORS.put(ParameterizedView.class, ge -> ParameterizedViewCursor.class);
    }

    private static final Map<String, String> TRIGGER_REGISTRATION_METHOD_TO_TRIGGER_TYPE;

    static {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("onPreDelete", "PRE_DELETE");
        map.put("onPostDelete", "POST_DELETE");
        map.put("onPreInsert", "PRE_INSERT");
        map.put("onPostInsert", "POST_INSERT");
        map.put("onPreUpdate", "PRE_UPDATE");
        map.put("onPostUpdate", "POST_UPDATE");

        TRIGGER_REGISTRATION_METHOD_TO_TRIGGER_TYPE = Collections.unmodifiableMap(map);
    }

    private CursorGenerator() {
        throw new AssertionError();
    }

    protected static void generateCursor(GrainElement ge, File srcDir, String scorePath) {
        final String sourcePackage = calcSourcePackage(ge, scorePath);

        if (sourcePackage.isEmpty())
            throw new CelestaException(
                    "Couldn't generate class file for %s.%s without package",
                    ge.getGrain().getName(), ge.getName()
            );

        final String sourceFileNamePrefix = StringUtils.capitalize(ge.getName());

        boolean isVersionedGe = ge instanceof VersionedElement && ((VersionedElement) ge).isVersioned();

        String className = calcClassName(ge, sourceFileNamePrefix);

        TypeSpec.Builder cursorClass = buildClassDefinition(ge, className);

        cursorClass.addMethods(buildConstructors(ge));

        //FIELDS
        if (ge instanceof DataGrainElement) {
            List<FieldSpec> fieldSpecs = buildFields((DataGrainElement) ge);
            cursorClass.addFields(fieldSpecs);
            cursorClass.addMethods(generateGettersAndSetters(fieldSpecs));

            DataGrainElement dge = (DataGrainElement) ge;
            Map<String, ? extends ColumnMeta> columns = dge.getColumns();

            cursorClass.addMethod(buildSetFieldValue());

            StringBuilder parseResultOverridingMethodNameBuilder = new StringBuilder("_parseResult");

            final Set<Column> pk;
            if (dge instanceof TableElement) {
                TableElement te = (TableElement) dge;

                if (te instanceof Table && ((Table) te).isReadOnly()) {
                    pk = Collections.emptySet();
                } else {
                    pk = new LinkedHashSet<>(te.getPrimaryKey().values());
                    cursorClass.addMethod(buildCurrentKeyValues(pk));
                    if (te instanceof Table) {
                        parseResultOverridingMethodNameBuilder.append("Internal");
                    }
                }
            } else {
                pk = Collections.emptySet();
            }

            MethodSpec buildParseResultMethod = buildParseResult(
                    columns, parseResultOverridingMethodNameBuilder.toString(), isVersionedGe
            );
            cursorClass.addMethod(buildParseResultMethod);

            cursorClass.addMethod(buildClearBuffer(columns, pk));

            cursorClass.addMethod(buildCurrentValues(columns));

            if (dge instanceof Table) {
                Table t = (Table) dge;
                if (!t.isReadOnly()) {
                    cursorClass.addMethods(buildCalcBlobs(columns, className));
                    cursorClass.addMethod(buildSetAutoIncrement(columns));
                    cursorClass.addMethods(buildTriggerRegistration(className));
                }
                cursorClass.addTypes(
                        buildOptionFieldsAsInnerStaticClasses(t.getColumns().values())
                );
            }

            cursorClass.addMethods(buildCompileCopying(ge, className, columns.keySet(), isVersionedGe));
            cursorClass.addMethod(buildIterator(className));
        }

        cursorClass.addMethods(buildGrainNameAndObjectName(ge));

        JavaFile javaFile = JavaFile.builder(sourcePackage, cursorClass.build())
                .skipJavaLangImports(true)
                .indent("    ")
                .build();

        try {
            javaFile.writeTo(srcDir);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String calcSourcePackage(GrainElement ge, String scorePath) {
        String result;

        Grain g = ge.getGrain();
        if (g.getName().equals(g.getScore().getSysSchemaName())) {
            result = "ru.curs.celesta.syscursors";
        } else {
            final String grainPartPath = ge.getGrainPart().getSourceFile().getParentFile().getAbsolutePath();
            final String grainPartRelativePath = grainPartPath.replace(scorePath, "");
            result = grainPartRelativePath.replace(File.separator, ".");

            if (result.startsWith("."))
                result = result.substring(1);
        }

        return result;
    }

    private static String calcClassName(GrainElement ge, String sourceFileNamePrefix) {
        if (ge instanceof SequenceElement)
            return sourceFileNamePrefix + "Sequence";
        else
            return sourceFileNamePrefix + "Cursor";
    }

    private static TypeSpec.Builder buildClassDefinition(GrainElement ge, String className) {
        TypeSpec.Builder builder = TypeSpec.classBuilder(className)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .superclass(GRAIN_ELEMENTS_TO_DATA_ACCESSORS.get(ge.getClass()).apply(ge));

        if (ge instanceof DataGrainElement) {
            TypeName selfTypeName = ClassName.bestGuess(className);
            builder.addSuperinterface(
                    ParameterizedTypeName.get(ClassName.get(Iterable.class), selfTypeName)
            );
        }
        if (ge instanceof Table) {
            Table t = (Table) ge;
            if (!t.isReadOnly())
                t.getImplements().forEach(
                        i -> builder.addSuperinterface(ClassName.bestGuess(i))
                );

            if (ge.getGrain().getName().equals(ge.getGrain().getScore().getSysSchemaName()))
                builder.addField(
                        FieldSpec.builder(
                                String.class, "TABLE_NAME",
                                Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL
                        ).initializer("$S", ge.getName()).build()
                );
        }

        return builder;

    }

    private static List<TypeSpec> buildOptionFieldsAsInnerStaticClasses(Collection<Column> columns) {
        return columns.stream()
                .filter(c -> (c instanceof IntegerColumn || c instanceof StringColumn) && !c.getOptions().isEmpty())
                .map(
                        c -> {
                            TypeSpec.Builder builder = TypeSpec.classBuilder(StringUtils.capitalize(c.getName()))
                                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL);

                            MethodSpec constructor = MethodSpec.constructorBuilder()
                                    .addModifiers(Modifier.PRIVATE)
                                    .addStatement("throw new $T()", AssertionError.class)
                                    .build();

                            builder.addMethod(constructor);


                            List<String> options = c.getOptions();
                            builder.addFields(
                                    options.stream().map(s -> {
                                                FieldSpec.Builder fb = FieldSpec.builder(
                                                        c.getJavaClass(), s,
                                                        Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL
                                                );

                                                if (c instanceof IntegerColumn) {
                                                    fb.initializer("$L", options.indexOf(s));
                                                } else {
                                                    fb.initializer("$S", s);
                                                }

                                                return fb.build();
                                            }
                                    ).collect(Collectors.toList())
                            );


                            return builder.build();
                        }
                ).collect(Collectors.toList());
    }

    private static List<MethodSpec> buildConstructors(GrainElement ge) {
        List<MethodSpec> result = new ArrayList<>();

        ParameterSpec contextParam = ParameterSpec.builder(CallContext.class, "context")
                .build();
        ParameterSpec fieldsParam = ParameterSpec.builder(
                ParameterizedTypeName.get(Set.class, String.class), "fields")
                .build();
        ParameterSpec parametersParam = ParameterSpec.builder(
                ParameterizedTypeName.get(Map.class, String.class, Object.class), "parameters")
                .build();

        Supplier<MethodSpec.Builder> msp = () -> MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(contextParam);

        //Common constructor
        MethodSpec.Builder builder = msp.get();

        if (ge instanceof ParameterizedView) {
            builder.addParameter(parametersParam);
            builder.addStatement("super(context, parameters)");
        } else {
            builder.addStatement("super(context)");
        }

        result.add(builder.build());

        if (ge instanceof SequenceElement)
            return result;

        builder = msp.get();
        //Constructor with fields limitation
        if (ge instanceof ParameterizedView) {
            builder.addParameter(fieldsParam);
            builder.addParameter(parametersParam);
            builder.addStatement("super(context, fields, parameters)");
        } else {
            builder.addParameter(fieldsParam);
            builder.addStatement("super(context, fields)");
        }
        result.add(builder.build());

        return result;

    }

    private static List<MethodSpec> buildGrainNameAndObjectName(GrainElement ge) {
        MethodSpec grainName = MethodSpec.methodBuilder("_grainName")
                .addAnnotation(Override.class)
                .returns(String.class)
                .addModifiers(Modifier.PROTECTED)
                .addStatement("return $S", ge.getGrain().getName())
                .build();

        MethodSpec objectName = MethodSpec.methodBuilder("_objectName")
                .addAnnotation(Override.class)
                .returns(String.class)
                .addModifiers(Modifier.PROTECTED)
                .addStatement("return $S", ge.getName())
                .build();

        return Arrays.asList(grainName, objectName);
    }

    private static List<FieldSpec> buildFields(DataGrainElement ge) {
        Map<String, ? extends ColumnMeta> columns = ge.getColumns();

        return columns.entrySet().stream()
                .map(e -> FieldSpec.builder(e.getValue().getJavaClass(), e.getKey(), Modifier.PRIVATE))
                .map(FieldSpec.Builder::build)
                .collect(Collectors.toList());

    }

    private static List<MethodSpec> generateGettersAndSetters(List<FieldSpec> fieldSpecs) {
        List<MethodSpec> result = new ArrayList<>();

        fieldSpecs.forEach(
                fieldSpec -> {
                    String methodSuffix = String.valueOf(Character.toUpperCase(fieldSpec.name.charAt(0)));

                    if (fieldSpec.name.length() > 1)
                        methodSuffix = methodSuffix + fieldSpec.name.substring(1);

                    MethodSpec getter = MethodSpec.methodBuilder("get" + methodSuffix)
                            .addModifiers(Modifier.PUBLIC)
                            .returns(fieldSpec.type)
                            .addStatement("return this.$N", fieldSpec.name).build();
                    MethodSpec setter = MethodSpec.methodBuilder("set" + methodSuffix)
                            .addModifiers(Modifier.PUBLIC)
                            .addParameter(fieldSpec.type, fieldSpec.name)
                            .addStatement("this.$N = $N", fieldSpec.name, fieldSpec.name).build();

                    result.add(getter);
                    result.add(setter);
                }
        );

        return result;
    }

    private static MethodSpec buildParseResult(
            Map<String, ? extends ColumnMeta> columns, String methodName, boolean isVersionedObject
    ) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder(methodName)
                .addModifiers(Modifier.PROTECTED)
                .addAnnotation(Override.class)
                .addParameter(ResultSet.class, "rs")
                .addException(SQLException.class);

        columns.entrySet().forEach(entry -> {
            String name = entry.getKey();
            ColumnMeta meta = entry.getValue();

            if (BinaryColumn.CELESTA_TYPE.equals(meta.getCelestaType())) {
                builder.addStatement("this.$N = null", name);
            } else {
                builder.beginControlFlow("if (this.$N($S))", "inRec", name);
                if (ZonedDateTimeColumn.CELESTA_TYPE.equals(meta.getCelestaType())) {
                    builder.addStatement(
                            "$T ts = rs.$N($S, $T.getInstance($T.getTimeZone($S)))",
                            Timestamp.class, meta.jdbcGetterName(), name, Calendar.class, TimeZone.class, "UTC"
                    );
                    builder.beginControlFlow("if ($N != null)", "ts");
                    builder.addStatement("this.$N = $T.of(ts.toLocalDateTime(), $T.systemDefault())",
                            name, ZonedDateTime.class, ZoneOffset.class);
                    builder.endControlFlow();
                    builder.beginControlFlow("else");
                    builder.addStatement("this.$N = null", name);
                    builder.endControlFlow();
                } else {
                    builder.addStatement("this.$N = rs.$N($S)", name, meta.jdbcGetterName(), name);
                    builder.beginControlFlow("if (rs.$N())", "wasNull");
                    builder.addStatement("this.$N = null", name);
                    builder.endControlFlow();
                }
                builder.endControlFlow();
            }
        });

        if (isVersionedObject) {
            builder.addStatement("this.setRecversion(rs.getInt($S))", "recversion");
        }

        return builder.build();
    }

    private static MethodSpec buildSetFieldValue() {
        String nameParam = "name";
        String valueParam = "value";

        return MethodSpec.methodBuilder("_setFieldValue")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .addParameter(String.class, nameParam)
                .addParameter(Object.class, valueParam)
                .beginControlFlow("try")
                .addStatement("$T f = getClass().getDeclaredField($N)", Field.class, nameParam)
                .addStatement("f.setAccessible(true)")
                .addStatement("f.set(this, value)")
                .endControlFlow()
                .beginControlFlow("catch ($T e)", Exception.class)
                .addStatement("throw new $T(e)", RuntimeException.class)
                .endControlFlow()
                .build();
    }

    private static MethodSpec buildClearBuffer(Map<String, ? extends ColumnMeta> columns, Set<Column> pk) {

        ParameterSpec param = ParameterSpec.builder(boolean.class, "withKeys").build();

        MethodSpec.Builder builder = MethodSpec.methodBuilder("_clearBuffer")
                .addModifiers(Modifier.PROTECTED)
                .addAnnotation(Override.class)
                .addParameter(param);

        if (!pk.isEmpty()) {
            builder.beginControlFlow("if ($N)", param.name);
            pk.stream().forEach(c -> builder.addStatement("this.$N = null", c.getName()));
            builder.endControlFlow();
        }

        columns.entrySet().stream()
                .filter(e -> !pk.contains(e.getValue()))
                .forEach(e -> builder.addStatement("this.$N = null", e.getKey()));

        return builder.build();
    }

    private static MethodSpec buildCurrentKeyValues(Set<Column> pk) {

        ArrayTypeName resultType = ArrayTypeName.of(Object.class);

        MethodSpec.Builder builder = MethodSpec.methodBuilder("_currentKeyValues")
                .addModifiers(Modifier.PROTECTED)
                .addAnnotation(Override.class)
                .returns(resultType)
                .addStatement("$T result = new Object[$L]", resultType, pk.size());

        AtomicInteger counter = new AtomicInteger(0);
        pk.forEach(
                c -> builder.addStatement("result[$L] = this.$N", counter.getAndIncrement(), c.getName())
        );
        builder.addStatement("return result");
        return builder.build();
    }

    private static MethodSpec buildCurrentValues(Map<String, ? extends ColumnMeta> columns) {
        ArrayTypeName resultType = ArrayTypeName.of(Object.class);

        MethodSpec.Builder builder = MethodSpec.methodBuilder("_currentValues")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
                .returns(resultType)
                .addStatement("$T result = new Object[$L]", resultType, columns.size());

        AtomicInteger counter = new AtomicInteger(0);
        columns.forEach(
                (name, c) -> builder.addStatement("result[$L] = this.$N", counter.getAndIncrement(), name)
        );
        builder.addStatement("return result");
        return builder.build();
    }

    private static List<MethodSpec> buildCalcBlobs(Map<String, ? extends ColumnMeta> columns, String className) {
        return columns.entrySet().stream()
                .filter(e -> e.getValue() instanceof BinaryColumn)
                .map(e ->
                        MethodSpec.methodBuilder("calc" + StringUtils.capitalize(e.getKey()))
                                .addModifiers(Modifier.PUBLIC)
                                .addStatement("this.$N = this.calcBlob($S)", e.getKey(), e.getKey())
                                .addStatement(
                                        "(($N)this.getXRec()).$N = this.$N.clone()",
                                        className, e.getKey(), e.getKey()
                                ).build()
                ).collect(Collectors.toList());
    }

    private static MethodSpec buildSetAutoIncrement(Map<String, ? extends ColumnMeta> columns) {
        MethodSpec.Builder builder = MethodSpec
                .methodBuilder("_setAutoIncrement")
                .addModifiers(Modifier.PROTECTED)
                .addAnnotation(Override.class);

        ParameterSpec param = ParameterSpec.builder(int.class, "val").build();

        builder.addParameter(param);

        columns.entrySet().stream().filter(
                e -> e.getValue() instanceof IntegerColumn && (((IntegerColumn) e.getValue()).isIdentity()
                        || ((IntegerColumn)e.getValue()).getSequence() != null)
        ).findFirst()
                .ifPresent(e -> builder.addStatement("this.$N = $N", e.getKey(), param.name));

        return builder.build();
    }

    private static List<MethodSpec> buildTriggerRegistration(String className) {
        TypeName selfTypeName = ClassName.bestGuess(className);

        ParameterSpec celestaParam = ParameterSpec.builder(
                ICelesta.class, "celesta")
                .build();
        ParameterSpec consumerParam = ParameterSpec.builder(
                ParameterizedTypeName.get(ClassName.get(Consumer.class), selfTypeName),
                "cursorConsumer")
                .build();

        return TRIGGER_REGISTRATION_METHOD_TO_TRIGGER_TYPE.entrySet().stream()
                .map(e -> {
                            String methodName = e.getKey();
                            String triggerType = e.getValue();

                            return MethodSpec.methodBuilder(methodName)
                                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                                    .addParameter(celestaParam)
                                    .addParameter(consumerParam)
                                    .addStatement(
                                            "$N.getTriggerDispatcher().registerTrigger($T.$N, $T.class, $N)",
                                            celestaParam.name, TriggerType.class, triggerType, selfTypeName,
                                            consumerParam.name
                                    ).build();
                        }
                ).collect(Collectors.toList());
    }

    private static List<MethodSpec> buildCompileCopying(
            GrainElement ge, String className, Collection<String> columns, boolean isVersionedObject
    ) {
        final String copyFieldsFromMethodName = "copyFieldsFrom";

        final ParameterSpec context = ParameterSpec.builder(CallContext.class, "context").build();
        final ParameterSpec fields = ParameterSpec.builder(
                ParameterizedTypeName.get(List.class, String.class), "fields"
        ).build();

        TypeName selfTypeName = ClassName.bestGuess(className);

        MethodSpec.Builder getBufferCopyBuilder = MethodSpec.methodBuilder("_getBufferCopy")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
                .addParameters(Arrays.asList(context, fields))
                .returns(selfTypeName)
                .addStatement("final $T result", selfTypeName)
                .beginControlFlow("if ($T.isNull($N))", Objects.class, fields.name);


        if (ge instanceof ParameterizedView) {
            getBufferCopyBuilder.addStatement("result = new $T($N, this.parameters)", selfTypeName, context.name);
        } else {
            getBufferCopyBuilder.addStatement("result = new $T($N)", selfTypeName, context.name);
        }

        getBufferCopyBuilder.endControlFlow()
                .beginControlFlow("else");

        if (ge instanceof ParameterizedView) {
            getBufferCopyBuilder.addStatement(
                    "result = new $T($N, new $T<>($N), this.parameters)",
                    selfTypeName, context.name, LinkedHashSet.class, fields.name
            );
        } else {
            getBufferCopyBuilder.addStatement(
                    "result = new $T($N, new $T<>($N))",
                    selfTypeName, context.name, LinkedHashSet.class, fields.name
            );
        }

        getBufferCopyBuilder.endControlFlow()
                .addStatement("result.$N(this)", copyFieldsFromMethodName)
                .addStatement("return result");

        MethodSpec getBufferCopy = getBufferCopyBuilder.build();

        MethodSpec.Builder copyFieldsFromBuilder = MethodSpec.methodBuilder(copyFieldsFromMethodName)
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
                .addParameter(BasicCursor.class, "c");

        copyFieldsFromBuilder.addStatement("$T from = ($T)c", selfTypeName, selfTypeName);

        columns.forEach(c ->
                copyFieldsFromBuilder.addStatement("this.$N = from.$N", c, c)
        );

        if (isVersionedObject)
            copyFieldsFromBuilder.addStatement("this.setRecversion(from.getRecversion())");

        return Arrays.asList(getBufferCopy, copyFieldsFromBuilder.build());
    }

    private static MethodSpec buildIterator(String className) {
        TypeName selfTypeName = ClassName.bestGuess(className);

        TypeName iteratorTypeName = ParameterizedTypeName.get(ClassName.get(Iterator.class), selfTypeName);
        TypeName cursorIterator = ParameterizedTypeName.get(ClassName.get(CursorIterator.class), selfTypeName);

        return MethodSpec.methodBuilder("iterator")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
                .returns(iteratorTypeName)
                .addStatement("return new $T(this)", cursorIterator)
                .build();
    }

}
