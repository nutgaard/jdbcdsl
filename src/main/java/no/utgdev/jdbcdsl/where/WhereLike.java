package no.utgdev.jdbcdsl.where;


public class WhereLike extends WhereClause {
    private final String field;
    private final Object value;

    public WhereLike(String field, Object value) {
        this.field = field;
        this.value = value;
    }

    @Override
    public Object[] getArgs() {
        return new Object[]{this.value};
    }

    @Override
    public String toSql() {
        return String.format("%s LIKE ?", this.field);
    }
}
