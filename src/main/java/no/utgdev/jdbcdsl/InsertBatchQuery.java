package no.utgdev.jdbcdsl;

import no.utgdev.jdbcdsl.value.FunctionValue;
import no.utgdev.jdbcdsl.value.Value;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.statement.PreparedBatch;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class InsertBatchQuery<T> implements DatachangeingQuery<InsertBatchQuery<T>> {
    private final Handle db;
    private final String tableName;
    private final Map<String, Value> values;

    public InsertBatchQuery(Handle db, String tableName) {
        this.db = db;
        this.tableName = tableName;
        this.values = new LinkedHashMap<>();
    }

    public InsertBatchQuery<T> set(String param, Object paramValue) {
        if (paramValue.getClass().isAssignableFrom(Function.class)) {
            return this.add(param, (Function<T, Object>) paramValue);
        } else {
            return this.add(param, (ignore) -> paramValue);
        }
    }

    public InsertBatchQuery<T> add(String param, Function<T, Object> paramValue) {
        return this.add(param, new FunctionValue(paramValue));
    }

    public InsertBatchQuery<T> add(String param, DbConstants value) {
        return this.add(param, Value.of(value));
    }

    public InsertBatchQuery<T> add(String param, Value value) {
        if (this.values.containsKey(param)) {
            throw new IllegalArgumentException(String.format("Param[%s] was already set.", param));
        }
        this.values.put(param, value);
        return this;
    }

    public int[] execute(List<T> data) {
        if (data.isEmpty()) {
            return new int[0];
        }
        String sql = Helpers.createInsertSqlStatement(this.tableName, this.values);
        PreparedBatch batch = db.prepareBatch(sql);
        Helpers.bindBatchData(batch, data, values);
        return batch.execute();
    }
}
