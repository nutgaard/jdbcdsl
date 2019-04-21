package no.utgdev.jdbcdsl.order;

public class OrderByExpression {
    public static OrderByExpression asc(final String field) {
        return new OrderByExpression(OrderOperator.ASC, field);
    }

    public static OrderByExpression desc(final String field) {
        return new OrderByExpression(OrderOperator.DESC, field);
    }

    private final OrderOperator operator;
    private final String columnName;

    private OrderByExpression(final OrderOperator operator, final String columnName) {
        this.operator = operator;
        this.columnName = columnName;
    }

    public String toSql() {
        return String.format("%s %s", this.columnName, this.operator.sql);
    }
}
