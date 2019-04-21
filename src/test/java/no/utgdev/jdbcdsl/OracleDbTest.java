package no.utgdev.jdbcdsl;

import org.junit.Before;

public class OracleDbTest extends DbTest {

    @Before
    public void setup() {
        db = TestUtils.jdbcTemplate(SqlUtils.DbSupport.ORACLE);
        super.setup();
    }
}
