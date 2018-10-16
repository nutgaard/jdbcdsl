package no.utgdev.jdbcdsl;

import no.utgdev.jdbcdsl.mapping.QueryMapping;
import no.utgdev.jdbcdsl.mapping.SqlRecord;
import org.jdbi.v3.core.Jdbi;

import java.sql.ResultSet;
import java.util.function.Function;

public class SqlUtils {
    static Function<String, String> append(final String suffix) {
        return (String value) -> value + suffix;
    }

    public static UpdateQuery update(Jdbi db, String tableName) {
        return new UpdateQuery(db, tableName);
    }

    public static <S> UpdateBatchQuery<S> updateBatch(Jdbi db, String tableName) {
        return new UpdateBatchQuery<>(db, tableName);
    }

    public static InsertQuery insert(Jdbi db, String tableName) {
        return new InsertQuery(db, tableName);
    }

    public static UpsertQuery upsert(Jdbi db, String tableName) {
        return new UpsertQuery(db, tableName);
    }

    public static <T> SelectQuery<T> select(Jdbi db, String tableName, SQLFunction<ResultSet, T> mapper) {
        return new SelectQuery<>(db, tableName, mapper);
    }

    public static SelectQuery<Long> nextFromSeq(Jdbi db, String sekvens) {
        return select(db, "dual", resultSet -> resultSet.getLong(1))
                .column(String.format("%s.NEXTVAL", sekvens));
    }

    public static DeleteQuery delete(Jdbi db, String tableName) {
        return new DeleteQuery(db, tableName);
    }

    public static <T extends SqlRecord> UpdateQuery update(Jdbi db, String tableName, T record) {
        QueryMapping<T> querymapping = QueryMapping.of((Class<T>)record.getClass());

        UpdateQuery updateQuery = update(db, tableName);
        return querymapping.applyColumn(updateQuery, record);
    }

    public static <T extends SqlRecord> UpdateBatchQuery<T> updateBatch(Jdbi db, String tableName, Class<T> cls) {
        QueryMapping<T> querymapping = QueryMapping.of(cls);

        UpdateBatchQuery<T> batchQuery = updateBatch(db, tableName, cls);
        return querymapping.applyColumn(batchQuery);
    }

    public static <T extends SqlRecord> InsertQuery insert(Jdbi db, String tableName, T record) {
        QueryMapping<T> querymapping = QueryMapping.of((Class<T>)record.getClass());

        InsertQuery insertQuery = insert(db, tableName);
        return querymapping.applyColumn(insertQuery, record);
    }

    public static <T extends SqlRecord> UpsertQuery upsert(Jdbi db, String tableName, T record) {
        QueryMapping<T> querymapping = QueryMapping.of((Class<T>)record.getClass());

        UpsertQuery upsertQuery = upsert(db, tableName);
        return querymapping.applyColumn(upsertQuery, record);
    }

    public static <T extends SqlRecord> SelectQuery<T> select(Jdbi db, String tableName, Class<T> recordClass) {
        QueryMapping<T> querymapping = QueryMapping.of(recordClass);

        SelectQuery<T> selectQuery = new SelectQuery<>(db, tableName, querymapping::createMapper);
        return querymapping.applyColumn(selectQuery);
    }

}
