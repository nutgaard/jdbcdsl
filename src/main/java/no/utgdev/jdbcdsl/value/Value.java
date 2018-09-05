package no.utgdev.jdbcdsl.value;


import no.utgdev.jdbcdsl.DbConstants;

public abstract class Value<T> {
    public final T sql;

    Value(T sql) {
        this.sql = sql;
    }

    public T getSql() {
        return this.sql;
    }

    public abstract boolean hasPlaceholder();

    public abstract String getValuePlaceholder();

    public static Value of(Object value) {
        return new ObjectValue(value);
    }

    public static Value of(DbConstants value) {
        return new ConstantValue(value);
    }
}
