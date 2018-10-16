package no.utgdev.jdbcdsl;

import no.utgdev.jdbcdsl.value.FunctionValue;
import no.utgdev.jdbcdsl.value.Value;
import no.utgdev.jdbcdsl.where.WhereClause;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Batch;
import org.jdbi.v3.core.statement.PreparedBatch;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;

public class UpdateBatchQuery<T> {
    private final Jdbi db;
    private final String tableName;
    private final Map<String, Value> setParams;
    private Function<T, WhereClause> whereClause;

    public UpdateBatchQuery(Jdbi db, String tableName) {
        this.db = db;
        this.tableName = tableName;
        this.setParams = new LinkedHashMap<>();
    }

    public UpdateBatchQuery<T> add(String param, Function<T, Object> paramValue) {
        return this.add(param, new FunctionValue<>(paramValue));
    }

    public UpdateBatchQuery<T> add(String param, DbConstants value) {
        return this.add(param, Value.of(value));
    }

    UpdateBatchQuery<T> add(String param, Value value) {
        if (this.setParams.containsKey(param)) {
            throw new IllegalArgumentException(String.format("Param[%s] was already set.", param));
        }
        this.setParams.put(param, value);
        return this;
    }

    public UpdateBatchQuery<T> addWhereClause(Function<T, WhereClause> whereClause) {
        this.whereClause = whereClause;
        return this;
    }

    public int[] execute(List<T> data) {
        if (data.isEmpty()) {
            return null;
        }
        String sql = createSql(data.get(0));

        boolean hasSetParamBindings = setParams
                .values()
                .stream()
                .anyMatch((param) -> param instanceof FunctionValue);

        if (!hasSetParamBindings && Objects.isNull(this.whereClause)) {
            return db.withHandle(handle -> {
                Batch batch = handle.createBatch();
                for (int i = 0; i < data.size(); i++) {
                    batch.add(sql);
                }
                return batch.execute();
            });
        }

        return db.withHandle(handle -> {
            PreparedBatch batch = handle.prepareBatch(sql);

            for (int i = 0; i < data.size(); i++) {
                T t = data.get(i);

                int j = 0;
                for (Value param : setParams.values()) {
                    if (param instanceof FunctionValue) {
                        FunctionValue<T> funcValue = (FunctionValue) param;
                        Function<T, Object> config = funcValue.getSql();

                        batch.bind(j++, config.apply(t));
                    }
                }
                if (Objects.nonNull(whereClause)) {
                    for (Object obj : whereClause.apply(t).getArgs()) {
                        batch.bind(j++, obj);
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
        } else if (Integer.class == type) {
            if (value != null) {
                ps.setInt(i, (Integer) value);
            } else {
                ps.setInt(i, -1);
            }
        } else if (Boolean.class == type) {
            ps.setBoolean(i, (Boolean) value);
        }
    }

    String createSql(T t) {
        StringBuilder sqlBuilder = new StringBuilder()
                .append("update ").append(tableName)
                .append(createSetStatement());

        if (Objects.nonNull(whereClause)) {
            sqlBuilder.append(" where ").append(whereClause.apply(t).toSql());
        }

        return sqlBuilder.toString();
    }

    private String createSetStatement() {
        return " set " + setParams.entrySet().stream()
                .map(entry -> entry.getKey() + " = " + entry.getValue().getValuePlaceholder())
                .collect(joining(", "));
    }
}
