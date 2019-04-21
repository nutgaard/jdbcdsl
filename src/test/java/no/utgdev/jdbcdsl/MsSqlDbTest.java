package no.utgdev.jdbcdsl;

import org.junit.Before;

public class MsSqlDbTest extends DbTest {

    @Before
    public void setup() {
        db = TestUtils.jdbcTemplate(SqlUtils.DbSupport.MSSQL);
        super.setup();
    }
}
