package no.utgdev.jdbcdsl;

import no.utgdev.jdbcdsl.value.Value;
import org.apache.commons.lang3.StringUtils;
import org.jdbi.v3.core.Jdbi;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.stream.Collectors.joining;

public class InsertQuery implements DatachangeingQuery<InsertQuery>{
    private final Jdbi db;
    private final String tableName;
    private final Map<String, Value> insertParams;

    public InsertQuery(Jdbi db, String tableName) {
        this.db = db;
        this.tableName = tableName;
        this.insertParams = new LinkedHashMap<>();
    }

    public InsertQuery set(String columnName, Object value) {
        return this.value(columnName, value);
    }
    public InsertQuery value(String columnName, Value value) {
        this.insertParams.put(columnName, value);
        return this;
    }

    public InsertQuery value(String columnName, DbConstants value) {
        return this.value(columnName, Value.of(value));
    }

    public InsertQuery value(String columnName, Object value) {
        return this.value(columnName, Value.of(value));
    }

    public int execute() {
        String sql = createSqlStatement();
        Object[] args = insertParams.values()
                .stream()
                .filter(Value::hasPlaceholder)
                .map(Value::getSql)
                .toArray();


        return db.withHandle(handle -> handle.execute(sql, args));
    }

    private String createSqlStatement() {
        String columns = StringUtils.join(insertParams.keySet(), ",");

        String values = insertParams
                .values()
                .stream()
                .map(Value::getValuePlaceholder)
                .collect(joining(","));


        return String.format("insert into %s (%s) values (%s)", tableName, columns, values);
    }

    public String toString() {
        return createSqlStatement();
    }
}
