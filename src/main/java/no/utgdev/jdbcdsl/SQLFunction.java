package no.utgdev.jdbcdsl;

import lombok.SneakyThrows;

import java.sql.SQLException;
import java.util.function.Function;

@FunctionalInterface
public interface SQLFunction<T, R> extends Function<T, R> {

    R sqlFunction(T t) throws SQLException;

    @Override
    @SneakyThrows
    default R apply(T t) {
        return sqlFunction(t);
    }
}
