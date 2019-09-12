package ru.curs.celesta.score;

public final class ParameterRef extends Expr {

    private Parameter parameter;
    private final String name;
    private ViewColumnMeta<?> meta;

    public ParameterRef(String name) {
        this.name = name;
    }

    @Override
    public ViewColumnMeta<?> getMeta() {
        if (meta == null) {
            if (parameter != null) {
                meta = new ViewColumnMeta<>(parameter.getType());
            } else {
                meta = new ViewColumnMeta<>(ViewColumnType.UNDEFINED);
            }
        }
        return meta;
    }

    @Override
    void accept(ExprVisitor visitor) throws ParseException {
        visitor.visitParameterRef(this);
    }

    public Parameter getParameter() {
        return parameter;
    }

    public void setParameter(Parameter parameter) {
        this.parameter = parameter;
    }

    public String getName() {
        return name;
    }

}
