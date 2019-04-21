package no.utgdev.jdbcdsl.where;


import java.util.Collection;
import java.util.stream.Collectors;

public class WhereIn extends WhereClause {
    private String field;
    private Collection<? extends Object> objects;

    WhereIn(String field, Collection<? extends Object> objects) {
        this.field = field;
        this.objects = objects;
    }

    static WhereIn of(String field, Collection<? extends Object> objects) {
        return new WhereIn(field, objects);
    }

    public Object[] getArgs() {
        return objects.toArray();
    }

    @Override
    public String toSql() {
        String parameters = objects.stream().map(dummy -> "?").collect(Collectors.joining(","));

        return String.format("%s %s (%s)", field, WhereOperator.IN.sql, parameters);
    }
}
