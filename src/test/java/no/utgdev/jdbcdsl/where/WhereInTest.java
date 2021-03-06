package no.utgdev.jdbcdsl.where;



import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class WhereInTest {

    @Test
    public void where_in_sql() {
        WhereIn whereIn = new WhereIn("FIELD", asList("value1", "value2"));
        assertThat(whereIn.toSql()).isEqualTo("FIELD IN (?,?)");
    }



}
