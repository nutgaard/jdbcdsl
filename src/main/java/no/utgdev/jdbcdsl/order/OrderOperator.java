package no.utgdev.jdbcdsl.order;

public enum OrderOperator {
    ASC("ASC"), DESC("DESC");

    public final String sql;

    OrderOperator(String sql) {
        this.sql = sql;
    }
}
