package ru.curs.celesta.score;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Parameterized View object in metadata.
 *
 * @author ioann
 * @since 2017-08-09
 */
public final class ParameterizedView extends View {

    final Map<String, Parameter> parameters = new LinkedHashMap<>();
    final List<String> parameterRefsWithOrder = new ArrayList<>();
    private final List<Supplier<Set<String>>> unusedParametersSuppliers = new ArrayList<>();

    public ParameterizedView(GrainPart grainPart, String name) throws ParseException {
        super(grainPart, name);
    }

    @Override
    String viewType() {
        return "function";
    }

    @Override
    AbstractSelectStmt newSelectStatement() {
        ParameterizedViewSelectStmt result = new ParameterizedViewSelectStmt(this);
        unusedParametersSuppliers.add(result::getUnusedParameters);
        return result;
    }

    /**
     * Adds a parameter to the view.
     *
     * @param parameter parameter
     * @throws ParseException if parameter name is empty or parameter already exists in the view
     */
    public void addParameter(Parameter parameter) throws ParseException {
        if (parameter == null) {
            throw new IllegalArgumentException();
        }

        if (parameter.getName() == null || parameter.getName().isEmpty()) {
            throw new ParseException(String.format("%s '%s' contains a parameter with undefined name.",
                    viewType(), getName()));
        }

        if (parameters.containsKey(parameter.getName())) {
            throw new ParseException(
                    String.format("%s '%s' already contains parameter with name '%s'. " +
                                    "Use unique names for %s parameters.",
                            viewType(), getName(), parameter.getName(), viewType())
            );
        }

        parameters.put(parameter.getName(), parameter);
    }

    /**
     * Returns a map <b>parameter name</b> -> <b>parameter</b>.
     *
     * @return
     */
    public Map<String, Parameter> getParameters() {
        return Collections.unmodifiableMap(parameters);
    }

    public List<String> getParameterRefsWithOrder() {
        return parameterRefsWithOrder;
    }

    @Override
    void finalizeParsing() throws ParseException {
        super.finalizeParsing();
        Set<String> unused = new HashSet<>(unusedParametersSuppliers.get(0).get());
        for (int i = 1; i < unusedParametersSuppliers.size(); i++) {
            unused.retainAll(unusedParametersSuppliers.get(i).get());
        }
        if (!unused.isEmpty()) {
            String unusedParametersStr = String.join(", ", unused);
            throw new ParseException(String.format("%s '%s' contains not used parameters %s.",
                    viewType(), getName(), unusedParametersStr));
        }
    }
}
