package no.utgdev.jdbcdsl;

import no.utgdev.jdbcdsl.value.Value;
import org.jdbi.v3.core.Handle;

import java.util.LinkedHashMap;
import java.util.Map;

public class InsertQuery implements DatachangeingQuery<InsertQuery>{
    private final Handle db;
    private final String tableName;
    private final Map<String, Value> insertParams;

    public InsertQuery(Handle db, String tableName) {
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
        String sql = Helpers.createInsertSqlStatement(this.tableName, this.insertParams);
        Object[] args = insertParams
                .values()
                .stream()
                .filter(Value::hasPlaceholder)
                .map(Value::getSql)
                .toArray();


        return db.execute(sql, args);
    }

    public String toString() {
        return Helpers.createInsertSqlStatement(this.tableName, this.insertParams);
    }
}
