package no.utgdev.jdbcdsl;

import no.utgdev.jdbcdsl.value.FunctionValue;
import no.utgdev.jdbcdsl.value.Value;
import org.apache.commons.lang3.StringUtils;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;

public class InsertBatchQuery<T> implements DatachangeingQuery<InsertBatchQuery<T>> {
    private final Jdbi db;
    private final String tableName;
    private final Map<String, Value> values;

    public InsertBatchQuery(Jdbi db, String tableName) {
        this.db = db;
        this.tableName = tableName;
        this.values = new LinkedHashMap<>();
    }

    public InsertBatchQuery<T> set(String param, Object paramValue) {
        if (paramValue.getClass().isAssignableFrom(Function.class)) {
            return this.add(param, (Function<T, Object>)paramValue);
        } else {
            return this.add(param, (ignore) -> paramValue);
        }
    }

    public InsertBatchQuery<T> add(String param, Function<T, Object> paramValue) {
        return this.add(param, new FunctionValue( paramValue));
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
            return null;
        }
        String sql = createSqlStatement();
        return db.withHandle(handle -> {
            PreparedBatch batch = handle.prepareBatch(sql);

            for (int i = 0; i < data.size(); i++) {
                T t = data.get(i);
                int j = 0;
                for (Value param : values.values()) {
                    if (param instanceof FunctionValue) {
                        FunctionValue<T> functionValue = (FunctionValue) param;
                        Function<T, Object> config = functionValue.getSql();

                        batch.bind(j++, config.apply(t));
                    }
                }

                batch.add();
            }

            return batch.execute();
        });
    }

    static void setParam(PreparedStatement ps, int i, Class type, Object value) throws SQLException {
        if (String.class == type) {
            ps.setString(i, (String) value);
        } else if (Timestamp.class == type) {
            ps.setTimestamp(i, (Timestamp) value);
        } else if (Boolean.class == type) {
            ps.setBoolean(i, (Boolean) value);
        } else if (Integer.class == type) {
            ps.setInt(i, (Integer) value);
        }
    }

    private String createSqlStatement() {
        String columns = StringUtils.join(values.keySet(), ",");
        String valueParams = values
                .values()
                .stream()
                .map(Value::getValuePlaceholder)
                .collect(joining(","));
        return String.format("insert into %s (%s) values (%s)", tableName, columns, valueParams);
    }
}
