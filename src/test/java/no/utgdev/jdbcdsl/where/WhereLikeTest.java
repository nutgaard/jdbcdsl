package no.utgdev.jdbcdsl.where;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class WhereLikeTest {

    @Test
    public void where_like_sql_med_bare_value() {
        WhereLike where = new WhereLike("FIELD", "value");

        assertThat(where.toSql()).isEqualTo("FIELD LIKE ?");
        assertThat(where.getArgs()).hasSize(1);
        assertThat(where.getArgs()[0]).isEqualTo("value");
    }

    @Test
    public void where_like_sql_med_wildcard_matching() {
        WhereLike where = new WhereLike("FIELD", "%value_");

        assertThat(where.toSql()).isEqualTo("FIELD LIKE ?");
        assertThat(where.getArgs()).hasSize(1);
        assertThat(where.getArgs()[0]).isEqualTo("%value_");

    }

}
