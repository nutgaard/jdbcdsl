package no.utgdev.jdbcdsl.value;

public class CastValue<T> extends Value<Object> {
    private final String sqlType;

    public CastValue(String sqlType, T value) {
        super(value);
        this.sqlType = sqlType;
    }

    @Override
    public boolean hasPlaceholder() {
        return true;
    }

    @Override
    public String getValuePlaceholder() {
        return "CAST(? as " + sqlType + ")";
    }
}
