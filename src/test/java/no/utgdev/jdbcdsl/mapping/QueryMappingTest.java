package no.utgdev.jdbcdsl.mapping;

import lombok.Value;
import no.utgdev.jdbcdsl.SelectQuery;
import no.utgdev.jdbcdsl.mapping.QueryMapping.Column;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class QueryMappingTest {

    @Test
    public void should_not_create_multiple_mappers_of_same_type() {
        QueryMapping<TestRecord> mapper = QueryMapping.of(TestRecord.class);
        QueryMapping<TestRecord> mapper2 = QueryMapping.of(TestRecord.class);

        assertThat(mapper == mapper2).isTrue();
    }

    @Test
    public void should_throw_error_for_non_column_types() {
        assertThatThrownBy(() -> QueryMapping.of(NonSupportedFieldRecord.class))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void should_throw_error_for_unknown_mapping() {
        assertThatThrownBy(() -> QueryMapping.of(NonSupportedMappingRecord.class))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void should_throw_if_a_matching_constructor_is_not_found() {
        assertThatThrownBy(() -> QueryMapping.of(NoMatchingConstructorRecord.class))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void should_throw_if_a_matching_constructor_is_not_found2() {
        assertThatThrownBy(() -> QueryMapping.of(NoMatchingConstructorRecord2.class))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void should_throw_if_a_matching_constructor_is_not_found3() {
        assertThatThrownBy(() -> QueryMapping.of(MultipleConstructorRecord.class))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void should_add_columns_based_on_field_names() {
        ArgumentCaptor<String> names = ArgumentCaptor.forClass(String.class);
        SelectQuery query = mock(SelectQuery.class);
        when(query.column(names.capture())).thenReturn(query);

        QueryMapping.of(TestRecord.class).applyColumn(query);

        verify(query, times(3)).column(anyString());
        assertThat(names.getAllValues()).isEqualTo(Arrays.asList("name", "age", "birth"));
    }

    @Test
    public void should_create_resultset_mapper() throws SQLException {
        LocalDate now = LocalDate.now();

        ResultSet rs = mock(ResultSet.class);
        when(rs.getString(anyString())).thenReturn("Bruce Wayne");
        when(rs.getInt(anyString())).thenReturn(42);
        when(rs.getDate(anyString())).thenReturn(Date.valueOf(now));

        TestRecord record = QueryMapping.of(TestRecord.class).createMapper(rs);

        assertThat(record.name.value).isEqualTo("Bruce Wayne");
        assertThat(record.age.value).isEqualTo(42);
        assertThat(record.birth.value).isEqualTo(now);
    }

    @Value
    static class TestRecord implements SqlRecord {
        Column<String, String> name;
        Column<Integer, Integer> age;
        Column<Date, LocalDate> birth;
    }

    @Value
    static class NonSupportedFieldRecord implements SqlRecord {
        String name;
    }

    @Value
    static class NonSupportedMappingRecord implements SqlRecord {
        Column<String, Character[]> name;
    }

    static class NoMatchingConstructorRecord implements SqlRecord {
        Column<String, Boolean> name;

        public NoMatchingConstructorRecord(Column<String, LocalDate> name) {

        }
    }

    static class NoMatchingConstructorRecord2 implements SqlRecord {
        Column<String, Boolean> name;

        public NoMatchingConstructorRecord2(String name) {

        }
    }
    static class MultipleConstructorRecord implements SqlRecord {
        Column<String, Boolean> name;

        public MultipleConstructorRecord(String name) {

        }

        public MultipleConstructorRecord(Boolean name) {

        }
    }

    static <FROM, TO> QueryMapping.InternalColumn<FROM, TO> column(Class<FROM> fromCls, Class<TO> toCls) {
        return QueryMapping.InternalColumn.of("name", null, fromCls, toCls);
    }
}
