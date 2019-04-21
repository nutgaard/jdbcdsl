package no.utgdev.jdbcdsl;

import io.vavr.collection.List;
import no.utgdev.jdbcdsl.CaseClause.WhenClause;
import org.junit.Test;

import static no.utgdev.jdbcdsl.value.Value.Int;
import static no.utgdev.jdbcdsl.where.WhereClause.like;
import static org.assertj.core.api.Assertions.assertThat;


public class CaseClauseTest {
    @Test
    public void skal_bygge_opp_case_sql() {
        CaseClause clause = CaseClause.of(
                WhenClause.when(like("field", "startsWith%"), Int(1)),
                WhenClause.when(like("field", "%contains%"), Int(2)),
                WhenClause.when(like("field", "%endsWith"), Int(3)),
                WhenClause.orElse(Int(4))
        );

        assertThat(clause.toSql()).isEqualTo("CASE WHEN field LIKE ? THEN CAST(? as INT) WHEN field LIKE ? THEN CAST(? as INT) WHEN field LIKE ? THEN CAST(? as INT) ELSE CAST(? as INT) END");
        assertThat(clause.getArgs()).hasSize(7);
        assertThat(clause.getArgs()).containsExactlyElementsOf(List.of(
                "startsWith%",
                1,
                "%contains%",
                2,
                "%endsWith",
                3,
                4
        ));
    }
}