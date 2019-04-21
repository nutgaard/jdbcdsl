package no.utgdev.jdbcdsl.where;

public enum WhereOperator {
    EQUALS("="),
    NOT_EQUALS("!="),
    AND("AND"),
    OR("OR"),
    IN("IN"),
    GT(">"),
    GTEQ(">="),
    LT("<"),
    LTEQ("<=");

    public final String sql;

    WhereOperator(String sql) {
        this.sql = sql;
    }
}
