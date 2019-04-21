package no.utgdev.jdbcdsl.where;

import no.utgdev.jdbcdsl.SqlFragment;

import java.util.Collection;

public abstract class WhereClause implements SqlFragment  {
    public static WhereClause equals(String field, Object value) {
        return new ComparativeWhereClause(WhereOperator.EQUALS, field, value);
    }
    public static WhereClause gt(String field, Object value) {
        return new ComparativeWhereClause(WhereOperator.GT, field, value);
    }
    public static WhereClause gteq(String field, Object value) {
        return new ComparativeWhereClause(WhereOperator.GTEQ, field, value);
    }
    public static WhereClause lt(String field, Object value) {
        return new ComparativeWhereClause(WhereOperator.LT, field, value);
    }
    public static WhereClause lteq(String field, Object value) {
        return new ComparativeWhereClause(WhereOperator.LTEQ, field, value);
    }
    public static WhereClause in(String field, Collection<?> objects) {
        return WhereIn.of(field, objects);
    }

    public static WhereClause isNotNull(String field) {
        return WhereIsNotNull.of(field);
    }

    public static WhereClause isNull(String field) {
        return WhereIsNull.of(field);
    }

    public WhereClause and(WhereClause other) {
        return new LogicalWhereClause(WhereOperator.AND, this, other);
    }

    public WhereClause andIf(WhereClause other, boolean add) {
        return add ? and(other) : this;
    }

    public WhereClause or(WhereClause other) {
        return new LogicalWhereClause(WhereOperator.OR, this, other);
    }

    public static WhereClause alwaysTrue() {
        return new ComparativeWhereClause(WhereOperator.EQUALS, "1", "1");
    }

    public static WhereClause alwaysFalse() {
        return new ComparativeWhereClause(WhereOperator.NOT_EQUALS, "1", "1");
    }

    public static WhereClause like(String field, String value) {
        return new WhereLike(field, value);
    }
}
