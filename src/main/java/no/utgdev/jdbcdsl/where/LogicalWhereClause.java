package no.utgdev.jdbcdsl.where;

import java.util.stream.Stream;

public class LogicalWhereClause extends WhereClause {
    private final WhereOperator operator;
    private final WhereClause wc1;
    private final WhereClause wc2;

    public LogicalWhereClause(WhereOperator operator, WhereClause wc1, WhereClause wc2) {
        this.operator = operator;
        this.wc1 = wc1;
        this.wc2 = wc2;

    }

    @Override
    public Object[] getArgs() {
        Stream<Object> wc1Stream = Stream.of(wc1.getArgs());
        Stream<Object> wc2Stream = Stream.of(wc2.getArgs());

        return Stream.concat(wc1Stream, wc2Stream).toArray();
    }

    @Override
    public String toSql() {
        return String.format("(%s) %s (%s)", this.wc1.toSql(), this.operator.sql, this.wc2.toSql());
    }
}
