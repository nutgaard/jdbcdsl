package no.utgdev.jdbcdsl.where;


public class WhereIsNull extends WhereClause {
    private String field;


    WhereIsNull(String field) {
        this.field = field;
    }

    static WhereIsNull of(String field) {
        return new WhereIsNull(field);
    }

    @Override
    public Object[] getArgs() {
        return new Object[]{};
    }

    @Override
    public String toSql() {
        return String.format("%s is null", field);
    }
}
