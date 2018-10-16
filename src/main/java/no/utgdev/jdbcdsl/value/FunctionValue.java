package no.utgdev.jdbcdsl.value;

import java.util.function.Function;

public class FunctionValue<T> extends Value<Function<T, Object>> {
    public FunctionValue(Function<T, Object> paramValue) {
        super(paramValue);
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
