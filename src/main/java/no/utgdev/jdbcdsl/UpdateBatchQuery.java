package no.utgdev.jdbcdsl;

import no.utgdev.jdbcdsl.value.FunctionValue;
import no.utgdev.jdbcdsl.value.Value;
import no.utgdev.jdbcdsl.where.WhereClause;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.statement.Batch;
import org.jdbi.v3.core.statement.PreparedBatch;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class UpdateBatchQuery<T> {
    private final Handle db;
    private final String tableName;
    private final Map<String, Value> setParams;
    private Function<T, WhereClause> whereClause;

    public UpdateBatchQuery(Handle db, String tableName) {
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
            return new int[0];
        }
        String sql = createSql(data.get(0));

        boolean hasSetParamBindings = setParams
                .values()
                .stream()
                .anyMatch((param) -> param instanceof FunctionValue);

        if (!hasSetParamBindings && Objects.isNull(this.whereClause)) {
            Batch batch = db.createBatch();
            for (int i = 0; i < data.size(); i++) {
                batch.add(sql);
            }
            return batch.execute();
        }

        PreparedBatch batch = db.prepareBatch(sql);
        Helpers.bindBatchData(batch, data, setParams, whereClause);

        return batch.execute();
    }

    String createSql(T t) {
        StringBuilder sqlBuilder = new StringBuilder()
                .append("update ").append(tableName)
                .append(Helpers.createSetStatement(setParams));

        if (Objects.nonNull(whereClause)) {
            sqlBuilder.append(" where ").append(whereClause.apply(t).toSql());
        }

        return sqlBuilder.toString();
    }
}
