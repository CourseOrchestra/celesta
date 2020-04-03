package ru.curs.celesta.score;

import java.io.*;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * View object in metadata.
 */
public class View extends AbstractView {

    Map<String, ViewColumnMeta<?>> columnTypes = null;


    View(GrainPart grainPart, String name) throws ParseException {
        super(grainPart, name);
        getGrain().addElement(this);
    }

    public View(GrainPart grainPart, String name, String sql) throws ParseException {
        this(grainPart, name);
        StringReader sr = new StringReader(sql);
        CelestaParser parser = new CelestaParser(sr);
        try {
            AbstractSelectStmt selectStmt = addSelectStatement();
            try {
                parser.select(selectStmt);
            } finally {
                sr.close();
            }
            selectStmt.finalizeParsing();
            finalizeParsing();
        } catch (ParseException e) {
            delete();
            throw e;
        }
    }

    @Override
    String viewType() {
        return "view";
    }

    @Override
    AbstractSelectStmt newSelectStatement() {
        return new ViewSelectStmt(this);
    }


    @Override
    public Map<String, ViewColumnMeta<?>> getColumns() {
        if (segments.size() > 0) {
            if (columnTypes == null) {
                columnTypes = new LinkedHashMap<>();
                for (Map.Entry<String, Expr> e : segments.get(0).columns.entrySet()) {
                    ViewColumnMeta<?> meta = e.getValue().getMeta();
                    meta.setName(e.getKey());
                    columnTypes.put(e.getKey(), meta);
                }
            }
            return columnTypes;
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Creates CREATE VIEW script in different SQL dialects by using 'visitor' pattern.
     *
     * @param bw  stream that the saving is performed into
     * @param gen generator-visitor
     * @throws IOException error on writing to stream
     */
    public void createViewScript(PrintWriter bw, SQLGenerator gen) throws IOException {
        bw.println(gen.preamble(this));
        selectScript(bw, gen);
    }


}
