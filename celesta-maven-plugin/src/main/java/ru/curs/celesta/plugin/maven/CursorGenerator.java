package ru.curs.celesta.plugin.maven;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.ArrayTypeName;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.WildcardTypeName;

import org.apache.commons.lang3.StringUtils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.CelestaException;
import ru.curs.celesta.ICelesta;

import ru.curs.celesta.dbutils.BasicCursor;
import ru.curs.celesta.dbutils.BasicDataAccessor;
import ru.curs.celesta.dbutils.CelestaGenerated;
import ru.curs.celesta.dbutils.Cursor;
import ru.curs.celesta.dbutils.CursorIterator;
import ru.curs.celesta.dbutils.MaterializedViewCursor;
import ru.curs.celesta.dbutils.ParameterizedViewCursor;
import ru.curs.celesta.dbutils.ReadOnlyTableCursor;
import ru.curs.celesta.dbutils.Sequence;
import ru.curs.celesta.dbutils.ViewCursor;
import ru.curs.celesta.event.TriggerType;
import ru.curs.celesta.score.BasicTable;
import ru.curs.celesta.score.BinaryColumn;
import ru.curs.celesta.score.Column;
import ru.curs.celesta.score.ColumnMeta;
import ru.curs.celesta.score.DataGrainElement;
import ru.curs.celesta.score.Grain;
import ru.curs.celesta.score.GrainElement;
import ru.curs.celesta.score.IntegerColumn;
import ru.curs.celesta.score.MaterializedView;
import ru.curs.celesta.score.NamedElement;
import ru.curs.celesta.score.Parameter;
import ru.curs.celesta.score.ParameterizedView;
import ru.curs.celesta.score.ReadOnlyTable;
import ru.curs.celesta.score.SequenceElement;
import ru.curs.celesta.score.StringColumn;
import ru.curs.celesta.score.Table;
import ru.curs.celesta.score.TableElement;
import ru.curs.celesta.score.VersionedElement;
import ru.curs.celesta.score.View;
import ru.curs.celesta.score.ZonedDateTimeColumn;
import ru.curs.celesta.score.io.FileResource;

import javax.annotation.Generated;
import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class CursorGenerator {

    private static final String GRAIN_FIELD_NAME = "GRAIN_NAME";
    private static final String OBJECT_FIELD_NAME = "OBJECT_NAME";
    private static final String COLUMNS_FIELD_NAME = "COLUMNS";

    private static final HashMap<
            Class<? extends GrainElement>,
            Function<GrainElement, Class<? extends BasicDataAccessor>>
            >
            GRAIN_ELEMENTS_TO_DATA_ACCESSORS = new HashMap<>();

    static {
        GRAIN_ELEMENTS_TO_DATA_ACCESSORS.put(SequenceElement.class, ge -> Sequence.class);
        GRAIN_ELEMENTS_TO_DATA_ACCESSORS.put(Table.class, ge -> Cursor.class);
        GRAIN_ELEMENTS_TO_DATA_ACCESSORS.put(ReadOnlyTable.class, ge -> ReadOnlyTableCursor.class);
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

        if (sourcePackage.isEmpty()) {
            throw new CelestaException(
                    "Couldn't generate class file for %s.%s without package",
                    ge.getGrain().getName(), ge.getName()
            );
        }

        final String sourceFileNamePrefix = StringUtils.capitalize(ge.getName());
        final String className = calcClassName(ge, sourceFileNamePrefix);
        final String columnsClassName = "Columns";

        boolean isVersionedGe = ge instanceof VersionedElement && ((VersionedElement) ge).isVersioned();

        ClassName classType = ClassName.bestGuess(className);

        TypeSpec.Builder cursorClass = buildClassDefinition(ge, classType);

        ClassName columnsClassType = classType.nestedClass(columnsClassName);

        cursorClass.addFields(buildMetaFields(ge));

        cursorClass.addMethods(buildConstructors(ge, columnsClassType));

        //FIELDS
        if (ge instanceof DataGrainElement) {
            DataGrainElement dge = (DataGrainElement) ge;

            FieldSpec columnsField = buildColumnsField(columnsClassType);
            cursorClass.addField(columnsField);
            cursorClass.addInitializerBlock(buildColumnsFiledInitializer(columnsField));

            List<FieldSpec> fieldSpecs = buildDataFields(dge);
            cursorClass.addFields(fieldSpecs);

            cursorClass.addMethods(generateGettersAndSetters(fieldSpecs));

            cursorClass.addMethod(buildGetFieldValue());
            cursorClass.addMethod(buildSetFieldValue());

            StringBuilder parseResultOverridingMethodNameBuilder = new StringBuilder("_parseResult");

            Set<Column<?>> pk = Collections.emptySet();
            if ((dge instanceof TableElement) && !(dge instanceof ReadOnlyTable)) {
                TableElement te = (TableElement) dge;
                pk = new LinkedHashSet<>(te.getPrimaryKey().values());
                cursorClass.addMethod(buildCurrentKeyValues(pk));
                if (te instanceof Table) {
                    parseResultOverridingMethodNameBuilder.append("Internal");
                }
            }

            final Map<String, ? extends ColumnMeta<?>> columns = dge.getColumns();

            MethodSpec buildParseResultMethod = buildParseResult(
                    columns, parseResultOverridingMethodNameBuilder.toString(), isVersionedGe);
            cursorClass.addMethod(buildParseResultMethod);

            cursorClass.addMethod(buildClearBuffer(columns, pk));

            cursorClass.addMethod(buildCurrentValues(columns));

            cursorClass.addType(buildCursorColumnsAsInnerStaticClass(dge, columnsClassType));

            if (dge instanceof BasicTable) {
                BasicTable t = (BasicTable) dge;
                if (t instanceof Table) {
                    cursorClass.addMethods(buildCalcBlobs(columns, className));
                    cursorClass.addMethod(buildSetAutoIncrement(columns));
                    cursorClass.addMethods(buildTriggerRegistration(className));
                }
                cursorClass.addTypes(
                        buildOptionFieldsAsInnerStaticClasses(t.getColumns().values()));
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

    static String calcSourcePackage(GrainElement ge, String scorePath) {
        String result;

        Grain g = ge.getGrain();
        if (g.getName().equals(g.getScore().getSysSchemaName())) {
            result = "ru.curs.celesta.syscursors";
        } else {
            String grainPartRelativePath =
                    new FileResource(new File(scorePath)).getRelativePath(ge.getGrainPart().getSource());
            result = Optional.of(grainPartRelativePath.lastIndexOf(File.separatorChar))
                    .filter(i -> i >= 0)
                    .map(i -> grainPartRelativePath.substring(0, i).replace(File.separator, "."))
                    .orElse("");
            if (result.startsWith(".")) {
                result = result.substring(1);
            }
        }

        return result;
    }

    private static String calcClassName(GrainElement ge, String sourceFileNamePrefix) {
        if (ge instanceof SequenceElement) {
            return sourceFileNamePrefix + "Sequence";
        } else {
            return sourceFileNamePrefix + "Cursor";
        }
    }

    private static String getCurrentDate() {
        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now());
    }

    private static AnnotationSpec buildGeneratedAnnotation() {
        return AnnotationSpec.builder(Generated.class)
                .addMember("value", "$S", CursorGenerator.class.getCanonicalName())
                .addMember("date", "$S", getCurrentDate())
                .build();
    }

    private static TypeSpec.Builder buildClassDefinition(GrainElement ge, ClassName classType) {
        TypeSpec.Builder builder = TypeSpec.classBuilder(classType)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .superclass(GRAIN_ELEMENTS_TO_DATA_ACCESSORS.get(ge.getClass()).apply(ge))
                .addAnnotation(buildGeneratedAnnotation())
                .addAnnotation(AnnotationSpec.builder(CelestaGenerated.class).build());

        if (ge instanceof DataGrainElement) {
            builder.addSuperinterface(
                    ParameterizedTypeName.get(ClassName.get(Iterable.class), classType)
            );
        }
        if (ge instanceof BasicTable) {
            BasicTable t = (BasicTable) ge;
            if (!(t instanceof ReadOnlyTable)) {
                t.getImplements().forEach(
                        i -> builder.addSuperinterface(ClassName.bestGuess(i))
                );
            }
        }

        return builder;

    }

    private static List<TypeSpec> buildOptionFieldsAsInnerStaticClasses(Collection<Column<?>> columns) {
        return columns.stream()
                .filter(c -> (c instanceof IntegerColumn || c instanceof StringColumn) && !c.getOptions().isEmpty())
                .map(
                        c -> {
                            TypeSpec.Builder builder = TypeSpec.classBuilder(StringUtils.capitalize(c.getName()))
                                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                                    .addAnnotation(buildGeneratedAnnotation())
                                    .addAnnotation(CelestaGenerated.class);

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

    private static List<MethodSpec> buildConstructors(GrainElement ge, TypeName columnsClassType) {

        List<MethodSpec> results = new ArrayList<>();

        ParameterSpec contextParam = ParameterSpec.builder(CallContext.class, "context")
                .build();

        ParameterSpec fieldsParam = ParameterSpec.builder(
                ParameterizedTypeName.get(Set.class, String.class), "fields")
                .build();

        ParameterSpec columnsParam = ParameterSpec.builder(
                ArrayTypeName.of(
                        ParameterizedTypeName.get(ClassName.get(ColumnMeta.class),
                                WildcardTypeName.subtypeOf(Object.class))),
                "columns")
                .build();

        ParameterSpec parametersParam = ParameterSpec.builder(
                ParameterizedTypeName.get(Map.class, String.class, Object.class), "parameters")
                .build();

        Supplier<MethodSpec.Builder> msp = () -> MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(contextParam);

        // Common constructor
        MethodSpec.Builder builder = msp.get();
        if (ge instanceof ParameterizedView) {
            builder.addParameter(parametersParam);
            builder.addStatement("super(context, parameters)");
        } else {
            builder.addStatement("super(context)");
        }
        results.add(builder.build());

        if (ge instanceof SequenceElement) {
            return results;
        }

        // Constructor with columns limitation
        builder = msp.get();
        if (ge instanceof ParameterizedView) {
            builder.addParameter(parametersParam);
            builder.addParameter(columnsParam).varargs();
            builder.addStatement("super(context, parameters, columns)");
        } else {
            builder.addParameter(columnsParam).varargs();
            builder.addStatement("super(context, columns)");
        }
        results.add(builder.build());

        // Deprecated constructor with fields limitation
        builder = msp.get();
        if (ge instanceof ParameterizedView) {
            builder.addParameter(fieldsParam);
            builder.addParameter(parametersParam);
            builder.addStatement("super(context, fields, parameters)");
        } else {
            builder.addParameter(fieldsParam);
            builder.addStatement("super(context, fields)");
        }
        results.add(builder.addAnnotation(Deprecated.class).build());

        //ParameterizedView constructors
        if (ge instanceof ParameterizedView) {
            ParameterizedView pv = (ParameterizedView) ge;

            builder = msp.get();
            for (Parameter parameter : pv.getParameters().values()) {
                builder.addParameter(ParameterSpec.builder(
                        parameter.getJavaClass(), parameter.getName()
                ).build());
            }

            String spec = "super (context, paramsMap("
                    + pv.getParameters().values().stream().map(c -> "$N").collect(Collectors.joining(", "))
                    + "))";
            builder.addStatement(spec, pv.getParameters().keySet().toArray());
            results.add(builder.build());

            builder = msp.get();
            for (Parameter parameter : pv.getParameters().values()) {
                builder.addParameter(ParameterSpec.builder(
                        parameter.getJavaClass(), parameter.getName()
                ).build());
            }
            builder.addParameter(columnsParam).varargs();
            spec = "super (context, paramsMap("
                    + pv.getParameters().values().stream().map(c -> "$N").collect(Collectors.joining(", "))
                    + "), columns)";
            builder.addStatement(spec, pv.getParameters().keySet().toArray());
            results.add(builder.build());

            results.add(getParameterizedViewTypedConstructorHelper(pv));
        }
        return results;
    }

    private static MethodSpec getParameterizedViewTypedConstructorHelper(ParameterizedView pv) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder("paramsMap")
                .addModifiers(Modifier.PRIVATE, Modifier.STATIC).returns(
                        ParameterizedTypeName.get(Map.class,
                                String.class, Object.class));
        builder.addStatement("$T<$T,$T> params = new $T<>()",
                Map.class, String.class, Object.class, HashMap.class);
        for (Parameter parameter : pv.getParameters().values()) {
            String paramName = parameter.getName();
            builder.addParameter(ParameterSpec.builder(
                    parameter.getJavaClass(), paramName
            ).build());
            builder.addStatement("params.put($S, $N)", paramName, paramName);
        }
        builder.addStatement("return params");
        return builder.build();
    }

    private static List<MethodSpec> buildGrainNameAndObjectName(GrainElement ge) {
        MethodSpec grainName = MethodSpec.methodBuilder("_grainName")
                .addAnnotation(Override.class)
                .returns(String.class)
                .addModifiers(Modifier.PROTECTED)
                .addStatement("return $L", GRAIN_FIELD_NAME)
                .build();

        MethodSpec objectName = MethodSpec.methodBuilder("_objectName")
                .addAnnotation(Override.class)
                .returns(String.class)
                .addModifiers(Modifier.PROTECTED)
                .addStatement("return $L", OBJECT_FIELD_NAME)
                .build();

        return Arrays.asList(grainName, objectName);
    }

    private static TypeSpec buildCursorColumnsAsInnerStaticClass(DataGrainElement dge, ClassName columnsClassType) {

        TypeSpec.Builder builder = TypeSpec.classBuilder(columnsClassType)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class)
                        .addMember("value", "$S", "unchecked")
                        .build())
                .addAnnotation(buildGeneratedAnnotation())
                .addAnnotation(CelestaGenerated.class);

        FieldSpec elementField = FieldSpec.builder(
                dge.getClass(), "element", Modifier.PRIVATE, Modifier.FINAL)
                .build();
        builder.addField(elementField);

        builder.addMethod(MethodSpec.constructorBuilder()
                .addModifiers(Modifier.PUBLIC)
                .addParameter(ICelesta.class, "celesta")
                .addStatement("this.$N = celesta.getScore().getGrains().get($L).getElements($T.class).get($L)",
                        elementField, GRAIN_FIELD_NAME, elementField.type, OBJECT_FIELD_NAME)
                .build());

        dge.getColumns().entrySet().stream()
                .filter(e -> !BinaryColumn.CELESTA_TYPE.equals(e.getValue().getCelestaType()))
                .map(e -> {
                    final String columnName = e.getKey();
                    final TypeName columnType =
                            ParameterizedTypeName.get(ColumnMeta.class, e.getValue().getJavaClass());
                    return MethodSpec.methodBuilder(columnName)
                            .addModifiers(Modifier.PUBLIC)
                            .returns(columnType)
                            .addStatement("return ($T) this.$N.getColumns().get($S)",
                                    columnType, elementField, columnName)
                            .build();
                })
                .forEach(builder::addMethod);

        return builder.build();
    }

    private static List<FieldSpec> buildMetaFields(GrainElement ge) {

        final String grainName = ge.getGrain().getName();

        FieldSpec grainField = FieldSpec.builder(String.class, GRAIN_FIELD_NAME,
                Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer("$S", grainName)
                .build();

        FieldSpec objectField = FieldSpec.builder(String.class, OBJECT_FIELD_NAME,
                Modifier.PRIVATE, Modifier.STATIC, Modifier.FINAL)
                .initializer("$S", ge.getName())
                .build();

        if (!grainName.equals(ge.getGrain().getScore().getSysSchemaName())) {
            return Arrays.asList(grainField, objectField);
        }

        FieldSpec tableField = FieldSpec.builder(String.class, "TABLE_NAME",
                Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
                .initializer("$N", objectField)
                .build();

        return Arrays.asList(grainField, objectField, tableField);
    }

    private static FieldSpec buildColumnsField(TypeName columnsClassType) {
        return FieldSpec.builder(columnsClassType, COLUMNS_FIELD_NAME, Modifier.PUBLIC, Modifier.FINAL)
                .build();
    }

    private static CodeBlock buildColumnsFiledInitializer(FieldSpec columnsField) {
        return CodeBlock.builder().addStatement(
                "this.$N = new $T(callContext().getCelesta())", columnsField, columnsField.type)
                .build();
    }

    private static List<FieldSpec> buildDataFields(DataGrainElement dge) {
        Map<String, ? extends ColumnMeta<?>> columns = dge.getColumns();

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

                    if (fieldSpec.name.length() > 1) {
                        methodSuffix = methodSuffix + fieldSpec.name.substring(1);
                    }

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
            Map<String, ? extends ColumnMeta<?>> columns, String methodName, boolean isVersionedObject
    ) {
        MethodSpec.Builder builder = MethodSpec.methodBuilder(methodName)
                .addModifiers(Modifier.PROTECTED)
                .addAnnotation(Override.class)
                .addParameter(ResultSet.class, "rs")
                .addException(SQLException.class);

        columns.forEach((name, meta) -> {

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

    /* NB In case the better performance is needed, this can be rewritten without using the reflection.
     * E.g. we can populate two maps: Map<String, Producer<?>> , Map<String, Consumer<?> in static Cursor
     * initializer, and then use these maps in getFieldValue and setFielValue. */
    private static MethodSpec buildGetFieldValue() {
        String nameParam = "name";

        return MethodSpec.methodBuilder("_getFieldValue")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(TypeName.OBJECT)
                .addParameter(String.class, nameParam)
                .beginControlFlow("try")
                .addStatement("$T f = getClass().getDeclaredField($N)", Field.class, nameParam)
                .addStatement("f.setAccessible(true)")
                .addStatement("return f.get(this)")
                .endControlFlow()
                .beginControlFlow("catch ($T e)", Exception.class)
                .addStatement("throw new $T(e)", RuntimeException.class)
                .endControlFlow()
                .build();
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

    private static MethodSpec buildClearBuffer(Map<String, ? extends ColumnMeta<?>> columns, Set<Column<?>> pk) {

        ParameterSpec param = ParameterSpec.builder(boolean.class, "withKeys").build();

        MethodSpec.Builder builder = MethodSpec.methodBuilder("_clearBuffer")
                .addModifiers(Modifier.PUBLIC)
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

    private static MethodSpec buildCurrentKeyValues(Set<Column<?>> pk) {

        ArrayTypeName resultType = ArrayTypeName.of(Object.class);

        MethodSpec.Builder builder = MethodSpec.methodBuilder("_currentKeyValues")
                .addModifiers(Modifier.PROTECTED)
                .addAnnotation(Override.class)
                .returns(resultType);
        String spec = "return new Object[] {"
                + pk.stream().map(c -> "$N").collect(Collectors.joining(", "))
                + "}";
        builder.addStatement(spec, pk.stream()
                .map(NamedElement::getName).toArray());
        return builder.build();
    }

    private static MethodSpec buildCurrentValues(Map<String, ? extends ColumnMeta<?>> columns) {
        ArrayTypeName resultType = ArrayTypeName.of(Object.class);

        MethodSpec.Builder builder = MethodSpec.methodBuilder("_currentValues")
                .addModifiers(Modifier.PUBLIC)
                .addAnnotation(Override.class)
                .returns(resultType);
        String spec = "return new Object[] {"
                + columns.values().stream().map(c -> "$N").collect(Collectors.joining(", "))
                + "}";
        builder.addStatement(spec, columns.values().stream()
                .map(ColumnMeta::getName).toArray());

        return builder.build();
    }

    private static List<MethodSpec> buildCalcBlobs(Map<String, ? extends ColumnMeta<?>> columns, String className) {
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

    private static MethodSpec buildSetAutoIncrement(Map<String, ? extends ColumnMeta<?>> columns) {
        MethodSpec.Builder builder = MethodSpec
                .methodBuilder("_setAutoIncrement")
                .addModifiers(Modifier.PROTECTED)
                .addAnnotation(Override.class);

        ParameterSpec param = ParameterSpec.builder(int.class, "val").build();

        builder.addParameter(param);

        columns.entrySet().stream()
                .filter(e -> e.getValue() instanceof IntegerColumn)
                .filter(e -> ((IntegerColumn) e.getValue()).getSequence() != null)
                .findAny()
                .ifPresent(e -> builder.addStatement("this.$N = $N", e.getKey(), param.name));

        return builder.build();
    }

    private static List<MethodSpec> buildTriggerRegistration(String className) {
        TypeName selfTypeName = ClassName.bestGuess(className);

        ParameterSpec celestaParam = ParameterSpec.builder(
                ICelesta.class, "celesta")
                .build();
        ParameterSpec consumerParam = ParameterSpec.builder(
                ParameterizedTypeName.get(ClassName.get(Consumer.class), WildcardTypeName.supertypeOf(selfTypeName)),
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

        if (isVersionedObject) {
            copyFieldsFromBuilder.addStatement("this.setRecversion(from.getRecversion())");
        }

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
