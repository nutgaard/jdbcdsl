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

    public static <S> UpdateBatchQuery<S> updateBatch(Jdbi db, String tableName, Class<S> cls) {
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

    public static <T extends SqlRecord> SelectQuery<T> select(Jdbi db, String tableName, Class<T> recordClass) {
        QueryMapping<T> querymapping = QueryMapping.of(recordClass);

        SelectQuery<T> selectQuery = new SelectQuery<>(db, tableName, querymapping::createMapper);
        querymapping.applyColumn(selectQuery);

        return selectQuery;
    }

    public static SelectQuery<Long> nextFromSeq(Jdbi db, String sekvens) {
        return select(db, "dual", resultSet -> resultSet.getLong(1))
                .column(String.format("%s.NEXTVAL", sekvens));
    }

    public static DeleteQuery delete(Jdbi db, String tableName) {
        return new DeleteQuery(db, tableName);
    }

}
