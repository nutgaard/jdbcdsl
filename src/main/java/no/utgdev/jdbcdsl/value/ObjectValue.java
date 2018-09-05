package no.utgdev.jdbcdsl.value;

public class ObjectValue extends Value<Object> {
    public ObjectValue(Object value) {
        super(value);
    }

    @Override
    public boolean hasPlaceholder() {
        return true;
    }

    @Override
    public String getValuePlaceholder() {
        return "?";
    }
}
