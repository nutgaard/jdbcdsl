package no.utgdev.jdbcdsl.mapping;

import io.vavr.collection.HashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.*;

import static no.utgdev.jdbcdsl.mapping.QueryMappingTest.column;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TypeMappingTest {
    @AfterEach
    public void after_each() {
        TypeMapping.typemappers = HashMap.empty();
        TypeMapping.registerDefaults();
    }

    @Test
    public void should_register_mapper() {
        TypeMapping.register(Character.class, String.class, Object::toString);
        String string = TypeMapping.convert('C', column(Character.class, String.class));

        assertThat(string).isEqualTo("C");
    }

    @Test
    public void should_override_mapper() {
        TypeMapping.register(Character.class, String.class, Object::toString);
        String string = TypeMapping.convert('C', column(Character.class, String.class));

        assertThat(string).isEqualTo("C");

        TypeMapping.register(Character.class, String.class, (ch) -> ch.toString().toLowerCase());
        String string2 = TypeMapping.convert('C', column(Character.class, String.class));

        assertThat(string2).isEqualTo("c");
    }

    @Test
    public void should_throw_on_unknown_mapper() {
        assertThatThrownBy(() -> TypeMapping.convert('C', column(Character.class, String.class)))
                .isExactlyInstanceOf(IllegalStateException.class);
    }

    @Test
    public void should_handle_all_identity_mappings() {
        String string = TypeMapping.convert("String", column(String.class, String.class));
        Boolean bool = TypeMapping.convert(Boolean.TRUE, column(Boolean.class, Boolean.class));

        assertThat(string).isEqualTo("String");
        assertThat(bool).isTrue();
    }

    @Test
    public void should_handle_null_values() {
        String string = TypeMapping.convert(null, column(String.class, String.class));

        assertThat(string).isNull();
    }

    @Test
    public void should_recover_from_NPE_in_mapping() {
        TypeMapping.register(Character.class, String.class, Object::toString);
        String string = TypeMapping.convert(null, column(Character.class, String.class));

        assertThat(string).isNull();
    }

    @Test
    public void should_try_mapper_if_value_is_null() {
        TypeMapping.register(Character.class, String.class, (ch) -> ch == null ? "NULL" : ch.toString());
        String string = TypeMapping.convert(null, column(Character.class, String.class));

        assertThat(string).isEqualTo("NULL");
    }

    @Test
    public void default_mappers() {
        LocalDate dateNow = LocalDate.now();
        LocalTime timeNow = LocalTime.now().withNano(0); // nanoes not supported in java.sql.Time
        LocalDateTime datetimeNow = LocalDateTime.now();
        ZonedDateTime zoneddatetimeNow = datetimeNow.atZone(ZoneId.systemDefault());

        LocalDate date = TypeMapping.convert(Date.valueOf(dateNow), column(Date.class, LocalDate.class));
        LocalTime time = TypeMapping.convert(Time.valueOf(timeNow), column(Time.class, LocalTime.class));
        LocalDateTime localdatetime = TypeMapping.convert(Timestamp.valueOf(datetimeNow), column(Timestamp.class, LocalDateTime.class));
        ZonedDateTime zoneddatetime = TypeMapping.convert(Timestamp.valueOf(datetimeNow), column(Timestamp.class, ZonedDateTime.class));

        Boolean numberBool = TypeMapping.convert(0, column(Integer.class, Boolean.class));
        Boolean numberBool2 = TypeMapping.convert(1, column(Integer.class, Boolean.class));
        Boolean strBool = TypeMapping.convert("N", column(String.class, Boolean.class));
        Boolean strBool2 = TypeMapping.convert("J", column(String.class, Boolean.class));
        Boolean strBool3 = TypeMapping.convert("true", column(String.class, Boolean.class));
        Boolean strBool4 = TypeMapping.convert("false", column(String.class, Boolean.class));

        assertThat(dateNow.isEqual(date)).isTrue();
        assertThat(timeNow.equals(time)).isTrue();
        assertThat(datetimeNow.isEqual(localdatetime)).isTrue();
        assertThat(zoneddatetimeNow.isEqual(zoneddatetime)).isTrue();

        assertThat(numberBool).isFalse();
        assertThat(numberBool2).isTrue();
        assertThat(strBool).isFalse();
        assertThat(strBool2).isTrue();
        assertThat(strBool3).isTrue();
        assertThat(strBool4).isFalse();
    }
}
