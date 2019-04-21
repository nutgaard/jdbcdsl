package no.utgdev.jdbcdsl.where;


public class WhereIsNotNull extends WhereClause {
    private String field;


    WhereIsNotNull(String field) {
        this.field = field;
    }

    static WhereIsNotNull of(String field) {
        return new WhereIsNotNull(field);
    }

    @Override
    public Object[] getArgs() {
        return new Object[]{};
    }

    @Override
    public String toSql() {
        return String.format("%s is not null", field);
    }
}
