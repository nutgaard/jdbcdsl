package no.utgdev.jdbcdsl;

import no.utgdev.jdbcdsl.value.FunctionValue;
import no.utgdev.jdbcdsl.value.Value;
import no.utgdev.jdbcdsl.where.WhereClause;
import org.apache.commons.lang3.StringUtils;
import org.jdbi.v3.core.statement.PreparedBatch;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static java.util.stream.Collectors.joining;

class Helpers {
    static String createInsertSqlStatement(String tableName, Map<String, Value> values) {
        String columns = StringUtils.join(values.keySet(), ",");

        String sql = values
                .values()
                .stream()
                .map(Value::getValuePlaceholder)
                .collect(joining(","));


        return String.format("insert into %s (%s) values (%s)", tableName, columns, sql);
    }

    static String createSetStatement(Map<String, Value> params) {
        return " set " + params.entrySet().stream()
                .map(entry -> entry.getKey() + " = " + entry.getValue().getValuePlaceholder())
                .collect(joining(", "));
    }

    static <T> void bindBatchData(PreparedBatch batch, List<T> data, Map<String, Value> values) {
        bindBatchData(batch, data, values, null);
    }

    static <T> void bindBatchData(PreparedBatch batch, List<T> data, Map<String, Value> values, Function<T, WhereClause> whereClause) {
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

            if (Objects.nonNull(whereClause)) {
                for (Object obj : whereClause.apply(t).getArgs()) {
                    batch.bind(j++, obj);
                }
            }

            batch.add();
        }
    }
}
