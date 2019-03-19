package no.utgdev.jdbcdsl.mapping;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static no.utgdev.jdbcdsl.mapping.QueryMappingTest.column;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ValueMappingTest {
    @Test
    public void should_handle_common_resultset_type() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        ValueMapping.getValue(column(String.class, null), rs);

        verify(rs).getString("name");
    }

    @Test
    public void should_throw_exception_on_unknown_types() {
        assertThatThrownBy(() -> ValueMapping.getValue(column(List.class, null), null))
                .isExactlyInstanceOf(RuntimeException.class);
    }
}
