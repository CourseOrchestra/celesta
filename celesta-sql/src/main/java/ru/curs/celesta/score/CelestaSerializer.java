package ru.curs.celesta.score;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import ru.curs.celesta.score.SequenceElement.Argument;

/**
 * Serializes grain and its components to CelestaSQL.
 * <br/>
 * <i>The class name reflects its counterpart - {@link CelestaParser}</i>
 *
 * @author Pavel Perminov (packpaul@mail.ru)
 * @since 2019-03-07
 */
public final class CelestaSerializer {

    private final PrintWriter writer;

    public CelestaSerializer(PrintWriter writer) {
        this.writer = writer;
    }

    static String toString(MaterializedView mv) throws IOException {
        try (StringWriter sw = new StringWriter()) {
            new CelestaSerializer(new PrintWriter(sw)).save(mv);
            return sw.toString();
        }
    }

    /**
     * Returns a query based on which the view is going to be created.
     *
     * @param v  view
     * @return
     * @throws IOException  if query creation fails
     */
    public static String toQueryString(View v) throws IOException {
        try (StringWriter sw = new StringWriter()) {
            new CelestaSerializer(new PrintWriter(sw)).saveQuery(v);
            return sw.toString();
        }
    }

    /**
     * Serializes grain part to its CelestaSQL representation.
     *
     * @param gp  grain part
     * @throws IOException  if serialization fails
     */
    public void save(GrainPart gp) throws IOException {

        final Grain grain = gp.getGrain();

        writeCelestaDoc(grain);
        writer.printf("CREATE SCHEMA %s VERSION '%s'", grain.getName(), grain.getVersion());

        if (!grain.isAutoupdate()) {
            writer.printf(" WITH NO AUTOUPDATE");
        }
        writer.printf(";%n");
        writer.println();

        writer.println("-- *** SEQUENCES ***");
        List<SequenceElement> sequences = grain.getElements(SequenceElement.class, gp);
        for (SequenceElement s : sequences) {
            save(s);
        }

        writer.println("-- *** TABLES ***");
        List<Table> tables = grain.getElements(Table.class, gp);
        for (Table t : tables) {
            save(t);
        }

        writer.println("-- *** FOREIGN KEYS ***");
        for (Table t : tables) {
            for (ForeignKey fk : t.getForeignKeys()) {
                save(fk);
            }
        }

        writer.println("-- *** INDICES ***");
        List<Index> indices = grain.getElements(Index.class, gp);
        for (Index i : indices) {
            save(i);
        }

        writer.println("-- *** VIEWS ***");
        List<View> views = grain.getElements(View.class, gp);
        for (View v : views) {
            save(v);
        }

        writer.println("-- *** MATERIALIZED VIEWS ***");
        List<MaterializedView> materializedViews = grain.getElements(MaterializedView.class, gp);
        for (MaterializedView mv : materializedViews) {
            save(mv);
        }

        writer.println("-- *** PARAMETERIZED VIEWS ***");
        List<ParameterizedView> parameterizedViews = grain.getElements(ParameterizedView.class, gp);
        for (ParameterizedView pv : parameterizedViews) {
            save(pv);
        }
    }

    private boolean writeCelestaDoc(NamedElement e) {
        String doc = e.getCelestaDoc();
        if (doc == null) {
            return false;
        } else {
            writer.printf("/**%s*/%n", doc);
            return true;
        }
    }

    void save(SequenceElement s) throws IOException {

        writeCelestaDoc(s);
        writer.printf("CREATE SEQUENCE %s ", s.getName());


        if (s.hasArgument(Argument.START_WITH)) {
            writer.printf("START WITH %s ", s.getArgument(Argument.START_WITH));
        }

        if (s.hasArgument(Argument.INCREMENT_BY)) {
            writer.printf("INCREMENT BY %s ", s.getArgument(Argument.INCREMENT_BY));
        }

        if (s.hasArgument(Argument.MINVALUE)) {
            writer.printf("MINVALUE %s ", s.getArgument(Argument.MINVALUE));
        }

        if (s.hasArgument(Argument.MAXVALUE)) {
            writer.printf("MAXVALUE %s ", s.getArgument(Argument.MAXVALUE));
        }

        if (s.hasArgument(Argument.CYCLE) && (Boolean) s.getArgument(Argument.CYCLE)) {
            writer.write("CYCLE ");
        }

        writer.println(";");
        writer.println();
    }

    /**
     * Serializes table to its CelestaSQL representation.
     *
     * @param t  table
     * @throws IOException  if serialization fails
     */
    void save(Table t) throws IOException {

        writeCelestaDoc(t);

        writer.printf("CREATE TABLE %s(%n", t.getQuotedNameIfNeeded());
        boolean comma = false;
        for (Column c : t.getColumns().values()) {
            if (comma) {
                writer.println(",");
            }
            save(c);
            comma = true;
        }

        // Here we write the PK
        if (!t.getPrimaryKey().isEmpty()) {
            if (comma) {
                writer.write(",");
            }
            writer.println();
            writer.write("  CONSTRAINT ");
            writer.write(t.getPkConstraintName());
            writer.write(" PRIMARY KEY (");
            comma = false;
            for (Column c : t.getPrimaryKey().values()) {
                if (comma) {
                    writer.write(", ");
                }
                writer.write(c.getQuotedNameIfNeeded());
                comma = true;
            }
            writer.println(")");
        }

        writer.write(")");
        boolean withEmitted = false;
        if (t.isReadOnly()) {
            writer.write(" WITH READ ONLY");
            withEmitted = true;
        } else if (!t.isVersioned()) {
            writer.write(" WITH NO VERSION CHECK");
            withEmitted = true;
        }
        if (!t.isAutoUpdate()) {
            if (!withEmitted) {
                writer.write(" WITH");
            }
            writer.write(" NO AUTOUPDATE");
        }
        writer.println(";");
        writer.println();
    }

    /**
     * Serializes column to its CelestaSQL representation.
     *
     * @param c  column
     * @throws IOException  if serialization fails
     */
    void save(Column c) throws IOException {
        writer.write("  ");
        if (writeCelestaDoc(c)) {
            writer.write("  ");
        }
        writer.write(c.getName());

        switch (c.getCelestaType()) {
            case BinaryColumn.CELESTA_TYPE:
                saveColumn((BinaryColumn) c);
                break;
            case BooleanColumn.CELESTA_TYPE:
                saveColumn((BooleanColumn) c);
                break;
            case DateTimeColumn.CELESTA_TYPE:
                saveColumn((DateTimeColumn) c);
                break;
            case DecimalColumn.CELESTA_TYPE:
                saveColumn((DecimalColumn) c);
                break;
            case ZonedDateTimeColumn.CELESTA_TYPE:
                saveColumn((ZonedDateTimeColumn) c);
                break;
            case FloatingColumn.CELESTA_TYPE:
                saveColumn((FloatingColumn) c);
                break;
            case IntegerColumn.CELESTA_TYPE:
                saveColumn((IntegerColumn) c);
                break;
            case StringColumn.VARCHAR:
            case StringColumn.TEXT:
                saveColumn((StringColumn) c);
                break;
            default:
                throw new IOException(String.format("No serializer for column of type %s was found!",
                                                    c.getCelestaType()));
        }
    }

    private void saveColumn(BinaryColumn c) throws IOException {
        writer.write(" BLOB");
        if (!c.isNullable()) {
            writer.write(" NOT NULL");
        }
        String defaultVal = c.getDefaultValue();
        if (defaultVal != null) {
            writer.write(" DEFAULT ");
            writer.write(defaultVal);
        }
    }

    private void saveColumn(BooleanColumn c) throws IOException {
        writer.write(" BIT");
        if (!c.isNullable()) {
            writer.write(" NOT NULL");
        }
        Boolean defaultVal = c.getDefaultValue();
        if (defaultVal != null) {
            writer.write(" DEFAULT '");
            writer.write(defaultVal.toString().toUpperCase());
            writer.write("'");
        }
    }

    private void saveColumn(DateTimeColumn c) throws IOException {
        writer.write(" DATETIME");
        if (!c.isNullable()) {
            writer.write(" NOT NULL");
        }
        if (c.isGetdate()) {
            writer.write(" DEFAULT GETDATE()");
        } else {
            Date defaultVal = c.getDefaultValue();
            if (defaultVal != null) {
                writer.write(" DEFAULT '");
                DateFormat df = new SimpleDateFormat("yyyyMMdd");
                writer.write(df.format(defaultVal));
                writer.write("'");
            }
        }
    }

    private void saveColumn(DecimalColumn c) throws IOException {
        writer.write(" DECIMAL");
        if (!c.isNullable()) {
            writer.write(" NOT NULL");
        }
        BigDecimal defaultVal = c.getDefaultValue();
        if (defaultVal != null) {
            writer.write(" DEFAULT ");
            writer.write(defaultVal.toString());
        }
    }

    private void saveColumn(FloatingColumn c) throws IOException {
        writer.write(" REAL");
        if (!c.isNullable()) {
            writer.write(" NOT NULL");
        }
        Double defaultVal = c.getDefaultValue();
        if (defaultVal != null) {
            writer.write(" DEFAULT ");
            writer.write(defaultVal.toString());
        }
    }

    private void saveColumn(IntegerColumn c) throws IOException {
        writer.write(" INT");
        if (!c.isNullable()) {
            writer.write(" NOT NULL");
        }
        Integer defaultVal = c.getDefaultValue();
        if (defaultVal != null) {
            writer.write(" DEFAULT ");
            writer.write(defaultVal.toString());
        }
    }

    private void saveColumn(StringColumn c) throws IOException {
        if (c.isMax()) {
            writer.write(" TEXT");
        } else {
            writer.write(" VARCHAR(");
            writer.write(Integer.toString(c.getLength()));
            writer.write(")");
        }

        if (!c.isNullable()) {
            writer.write(" NOT NULL");
        }
        String defaultVal = c.getDefaultValue();
        if (defaultVal != null) {
            writer.write(" DEFAULT ");
            writer.write(StringColumn.quoteString(defaultVal));
        }
    }

    private void saveColumn(ZonedDateTimeColumn c) throws IOException {
        writer.write(" " + ZonedDateTimeColumn.CELESTA_TYPE);
        if (!c.isNullable()) {
            writer.write(" NOT NULL");
        }
    }

    /**
     * Serializes foreign key to its CelestaSQL representation.
     *
     * @param fk  foreign key
     * @throws IOException  if serialization fails
     */
    void save(ForeignKey fk) {
        writer.write("ALTER TABLE ");
        writer.write(fk.getParentTable().getQuotedNameIfNeeded());
        writer.write(" ADD CONSTRAINT ");
        String name = fk.getConstraintName();

        writer.write(name);
        writer.write(" FOREIGN KEY (");
        boolean comma = false;
        for (Column c : fk.getColumns().values()) {
            if (comma) {
                writer.write(", ");
            }
            writer.write(c.getQuotedNameIfNeeded());
            comma = true;
        }
        writer.write(") REFERENCES ");

        writer.write(fk.getReferencedTable().getGrain().getQuotedNameIfNeeded());
        writer.write(".");

        writer.write(fk.getReferencedTable().getQuotedNameIfNeeded());
        writer.write("(");
        comma = false;
        for (Column c : fk.getReferencedTable().getPrimaryKey().values()) {
            if (comma) {
                writer.write(", ");
            }
            writer.write(c.getQuotedNameIfNeeded());
            comma = true;
        }
        writer.write(")");

        switch (fk.getUpdateRule()) {
            case CASCADE:
                writer.write(" ON UPDATE CASCADE");
                break;
            case SET_NULL:
                writer.write(" ON UPDATE SET NULL");
                break;
            case NO_ACTION:
            default:
                break;
        }

        switch (fk.getDeleteRule()) {
            case CASCADE:
                writer.write(" ON DELETE CASCADE");
                break;
            case SET_NULL:
                writer.write(" ON DELETE SET NULL");
                break;
            case NO_ACTION:
                default: break;
        }

        writer.println(";");
    }

    /**
     * Serializes index to its CelestaSQL representation.
     *
     * @param i  index
     * @throws IOException  if serialization fails
     */
    void save(Index i) {

        writeCelestaDoc(i);

        writer.write("CREATE INDEX ");
        writer.write(i.getQuotedNameIfNeeded());
        writer.write(" ON ");
        writer.write(i.getTable().getQuotedNameIfNeeded());
        writer.write("(");
        boolean comma = false;
        for (Column c : i.getColumns().values()) {
            if (comma) {
                writer.write(", ");
            }
            writer.write(c.getQuotedNameIfNeeded());
            comma = true;
        }
        writer.println(");");
    }

    /**
     * Serializes view to its CelestaSQL representation.
     *
     * @param v  view
     * @throws IOException  if serialization fails
     */
    void save(View v) throws IOException {
      writeCelestaDoc(v);
      v.createViewScript(writer, new ViewCelestaSQLGen(v));
      writer.println(";");
      writer.println();
    }

    private void saveQuery(View v) throws IOException {
        v.selectScript(writer, new ViewCelestaSQLGen(v));
    }

    /**
     * Serializes materialized view to its CelestaSQL representation.
     *
     * @param mv  materialized view
     * @throws IOException  if serialization fails
     */
    void save(MaterializedView mv) throws IOException {
        writeCelestaDoc(mv);
        SQLGenerator gen = new MaterializedViewCelestaSQLGen(mv);
        writer.println(gen.preamble(mv));
        mv.selectScript(writer, gen);
        writer.println(";");
        writer.println();
    }

    /**
     * Serializes parameterized view to its CelestaSQL representation.
     *
     * @param mv  parameterized view
     * @throws IOException  if serialization fails
     */
    void save(ParameterizedView pv) throws IOException {
      writeCelestaDoc(pv);
      pv.createViewScript(writer, new ParameterizedViewCelestaSQLGen(pv));
      writer.println(";");
      writer.println();
    }


    private abstract static class AbstractViewCelestaSQLGen<V extends AbstractView> extends SQLGenerator {
        final V view;

        AbstractViewCelestaSQLGen(V view) {
            this.view = view;
        }

        @Override
        protected String preamble(AbstractView dummyView) {
            return String.format("create %s %s as", view.viewType(), viewName(view));
        }

        @Override
        protected String viewName(AbstractView dummyView) {
            return view.getQuotedNameIfNeeded();
        }

        @Override
        protected String tableName(TableRef tRef) {
            Table t = tRef.getTable();
            if (t.getGrain() == view.getGrain()) {
                return String.format("%s as %s", t.getQuotedNameIfNeeded(), tRef.getAlias());
            } else {
                return String.format("%s.%s as %s",
                                     t.getGrain().getQuotedNameIfNeeded(), t.getQuotedNameIfNeeded(), tRef.getAlias());
            }
        }

        @Override
        protected boolean quoteNames() {
            return false;
        }
    }

    private static class ViewCelestaSQLGen extends AbstractViewCelestaSQLGen<View> {
        ViewCelestaSQLGen(View view) {
            super(view);
        }
    }

    private static class MaterializedViewCelestaSQLGen extends AbstractViewCelestaSQLGen<MaterializedView> {
        MaterializedViewCelestaSQLGen(MaterializedView view) {
            super(view);
        }
    }

    private static class ParameterizedViewCelestaSQLGen extends AbstractViewCelestaSQLGen<ParameterizedView> {
        ParameterizedViewCelestaSQLGen(ParameterizedView view) {
            super(view);
        }

        @Override
        protected String preamble(AbstractView dummyView) {
            return String.format("create %s %s (%s) as",
                    view.viewType(),
                    viewName(view),
                    view.getParameters().values().stream()
                        .map(p -> p.getName() + " " + p.getType().toString())
                        .collect(Collectors.joining(", ")));
        }
    }

}
