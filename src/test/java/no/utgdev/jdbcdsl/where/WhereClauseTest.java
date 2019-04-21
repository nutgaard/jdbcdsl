package no.utgdev.jdbcdsl.where;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class WhereClauseTest {
    @Test
    public void where_or() {
        WhereClause whereClause1 = WhereClause.equals("felt1","verdi1");
        WhereClause whereClause2 = WhereClause.equals("felt2","verdi2");
        WhereClause whereClause3 = WhereClause.equals("felt3","verdi3");
        assertThat(whereClause1.and(whereClause2.or(whereClause3)).toSql()).isEqualTo("(felt1 = ?) AND ((felt2 = ?) OR (felt3 = ?))");
    }
}
