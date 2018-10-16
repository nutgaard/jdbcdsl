package no.utgdev.jdbcdsl;

import no.utgdev.jdbcdsl.value.Value;
import org.jdbi.v3.core.Jdbi;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class UpdateQuery implements DatachangeingQuery<UpdateQuery> {
    private final Jdbi db;
    private final String tableName;
    private final Map<String, Value> setParams;
    private String whereParam;
    private Object whereValue;

    public UpdateQuery(Jdbi db, String tableName) {
        this.db = db;
        this.tableName = tableName;
        this.setParams = new LinkedHashMap<>();
    }

    public UpdateQuery set(String param, Object value) {
        return this.set(param, Value.of(value));
    }

    public UpdateQuery set(String param, DbConstants value) {
        return this.set(param, Value.of(value));
    }

    public UpdateQuery set(String param, Value value) {
        if (this.setParams.containsKey(param)) {
            throw new IllegalArgumentException(String.format("Param[%s] was already set.", param));
        }

        this.setParams.put(param, value);
        return this;
    }


    public UpdateQuery whereEquals(String whereParam, Object whereValue) {
        this.whereParam = whereParam;
        this.whereValue = whereValue;
        return this;
    }

    public Integer execute() {
        assert tableName != null;
        assert !setParams.isEmpty();

        StringBuilder sqlBuilder = new StringBuilder()
                .append("update ").append(tableName)
                .append(createSetStatement());

        if (this.whereParam != null) {
            sqlBuilder.append(" where ").append(whereParam).append(" = ?");
        }

        String sql = sqlBuilder.toString();
        return db.withHandle(handle -> handle.execute(sql, createSqlArgumentArray()));
    }

    private Object[] createSqlArgumentArray() {
        List<Object> args = setParams
                .values()
                .stream()
                .filter(Value::hasPlaceholder)
                .map(Value::getSql)
                .collect(toList());

        if (whereValue != null) {
            args.add(whereValue);
        }

        return args.toArray();
    }

    private String createSetStatement() {
        return " set " + setParams.entrySet().stream()
                .map(entry -> entry.getKey() + " = " + entry.getValue().getValuePlaceholder())
                .collect(joining(", "));
    }

}
