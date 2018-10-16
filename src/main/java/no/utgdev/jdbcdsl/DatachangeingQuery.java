package no.utgdev.jdbcdsl;

public interface DatachangeingQuery<T extends DatachangeingQuery> {
    public T set(String param, Object value);
}
