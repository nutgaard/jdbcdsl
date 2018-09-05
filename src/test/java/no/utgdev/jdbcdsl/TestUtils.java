package no.utgdev.jdbcdsl;

import org.hsqldb.jdbc.JDBCDataSource;
import org.jdbi.v3.core.Jdbi;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TestUtils {

    private static int counter;

    public static Jdbi jdbcTemplate() {
        JDBCDataSource dataSource = new JDBCDataSource();
        String url = "jdbc:hsqldb:mem:" + TestUtils.class.getSimpleName() + (counter++);
        dataSource.setUrl(url);
        dataSource.setUser("sa");
        dataSource.setPassword("");
        System.out.println(url);
        setHsqlToOraSyntax(dataSource);
        return Jdbi.create(dataSource);
    }

    private static void setHsqlToOraSyntax(JDBCDataSource ds) {
        try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
            st.execute("SET DATABASE SQL SYNTAX ORA TRUE;");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
