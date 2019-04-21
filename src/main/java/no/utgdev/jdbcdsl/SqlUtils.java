package no.utgdev.jdbcdsl;

import io.vavr.CheckedFunction0;
import io.vavr.CheckedFunction1;
import io.vavr.control.Option;
import io.vavr.control.Try;
import no.utgdev.jdbcdsl.mapping.QueryMapping;
import no.utgdev.jdbcdsl.mapping.SqlRecord;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.HandleCallback;
import org.jdbi.v3.core.HandleConsumer;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SqlUtils {
    private static final ThreadLocal<Boolean> runInTransaction = ThreadLocal.withInitial(() -> false);
    private static final ThreadLocal<Throwable> markedForRollback = ThreadLocal.withInitial(() -> null);
    private static final ThreadLocal<Handle> transactionHandle = ThreadLocal.withInitial(() -> null);
    private static final ThreadLocal<TransactionIsolationLevel> transactionLevel = ThreadLocal.withInitial(() -> null);

    public static SqlQuery run(Handle db, String sql) {
        return new SqlQuery(db, sql);
    }

    public static UpdateQuery update(Handle db, String tableName) {
        return new UpdateQuery(db, tableName);
    }

    public static <S> UpdateBatchQuery<S> updateBatch(Handle db, String tableName) {
        return new UpdateBatchQuery<>(db, tableName);
    }

    public static InsertQuery insert(Handle db, String tableName) {
        return new InsertQuery(db, tableName);
    }

    public static <T> SelectQuery<T> select(Handle db, String tableName, SQLFunction<ResultSet, T> mapper) {
        return new SelectQuery<>(db, tableName, mapper);
    }

    public static DeleteQuery delete(Handle db, String tableName) {
        return new DeleteQuery(db, tableName);
    }

    public static <T extends SqlRecord> UpdateQuery update(Handle db, String tableName, T record) {
        QueryMapping<T> querymapping = QueryMapping.of((Class<T>) record.getClass());

        UpdateQuery updateQuery = update(db, tableName);
        return querymapping.applyColumn(updateQuery, record);
    }

    public static <T extends SqlRecord> UpdateBatchQuery<T> updateBatch(Handle db, String tableName, Class<T> cls) {
        QueryMapping<T> querymapping = QueryMapping.of(cls);

        UpdateBatchQuery<T> batchQuery = updateBatch(db, tableName, cls);
        return querymapping.applyColumn(batchQuery);
    }

    public static <T extends SqlRecord> InsertQuery insert(Handle db, String tableName, T record) {
        QueryMapping<T> querymapping = QueryMapping.of((Class<T>) record.getClass());

        InsertQuery insertQuery = insert(db, tableName);
        return querymapping.applyColumn(insertQuery, record);
    }

    public static <T extends SqlRecord> SelectQuery<T> select(Handle db, String tableName, Class<T> recordClass) {
        QueryMapping<T> querymapping = QueryMapping.of(recordClass);

        SelectQuery<T> selectQuery = new SelectQuery<>(db, tableName, querymapping::createMapper);
        return querymapping.applyColumn(selectQuery);
    }


    // Transactions-helpers
    public static <R, X extends Exception> R withHandle(Jdbi jdbi, HandleCallback<R, X> callback) throws X {
        if (runInTransaction.get()) {
            Handle handle = getTransactionHandle(jdbi);
            return callback.withHandle(handle);
        } else {
            return jdbi.withHandle(callback);
        }
    }

    public static <X extends Exception> void useHandle(Jdbi jdbi, HandleConsumer<X> callback) throws X {
        withHandle(jdbi, (handle) -> {
            callback.useHandle(handle);
            return null;
        });
    }

    public static <S, T> SQLFunction<S, T> transactional(TransactionIsolationLevel level, CheckedFunction1<S, T> fn) {
        return (S s) -> {
            boolean isNestedTransaction = runInTransaction.get();

            if (!isNestedTransaction) {
                transactionLevel.set(level);
            }

            Try<T> retValue = Try.failure(new RuntimeException("Alvorlig feil"));
            try {
                runInTransaction.set(true);
                retValue = Try.of(() -> fn.apply(s));
            } catch (Exception e) {
                markedForRollback.set(e);
            } finally {
                Throwable throwable = markedForRollback.get();
                Option<Handle> maybeHandle = Option.of(transactionHandle.get());

                boolean hasError = throwable != null || retValue.isFailure();

                if (!isNestedTransaction) {
                    if (hasError) {
                        maybeHandle.map(Handle::rollback);
                    }
                    maybeHandle.map(Handle::commit);
                    maybeHandle.forEach(Handle::close);
                    transactionHandle.remove();
                    transactionLevel.remove();
                    markedForRollback.remove();
                }

                runInTransaction.set(isNestedTransaction);

                if (throwable != null) {
                    markedForRollback.remove();
                    throw new SQLException(throwable);
                }

                return retValue.get();
            }
        };
    }

    public static <S, T> SQLFunction<S, T> transactional(CheckedFunction1<S, T> fn) {
        return transactional(null, fn);
    }

    public static <T> T transactional(TransactionIsolationLevel level, CheckedFunction0<T> supplier) {
        return transactional(level, (aVoid) -> supplier.apply()).apply(null);
    }

    public static void transactional(TransactionIsolationLevel level, Runnable runnable) {
        transactional(level, (aVoid) -> { runnable.run(); return null; }).apply(null);
    }

    public static <R> R transactional(CheckedFunction0<R> supplier) {
        return transactional(null, supplier);
    }

    public static void transactional(Runnable runnable) {
        transactional(null, runnable);
    }

    private static Handle getTransactionHandle(Jdbi jdbi) {
        Handle handle = transactionHandle.get();
        if (handle == null) {
            handle = jdbi.open();
            if (transactionLevel.get() != null) {
                handle.setTransactionIsolation(transactionLevel.get());
            }
            handle.begin();
            transactionHandle.set(handle);
        }

        return handle;
    }
}
