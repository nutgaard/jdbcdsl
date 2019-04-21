package no.utgdev.jdbcdsl;

import no.utgdev.jdbcdsl.SqlUtils.DbSupport;
import org.hsqldb.jdbc.JDBCDataSource;
import org.jdbi.v3.core.Jdbi;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class TestUtils {
    private static int counter;

    public static Jdbi jdbcTemplate(DbSupport db) {
        JDBCDataSource dataSource = new JDBCDataSource();
        String url = "jdbc:hsqldb:mem:" + TestUtils.class.getSimpleName() + (counter++);
        dataSource.setUrl(url);
        dataSource.setUser("sa");
        dataSource.setPassword("");
        System.out.println(url);
        setHsqlToDbSyntax(dataSource, db);
        SqlUtils.db = db;
        return Jdbi.create(dataSource);
    }

    private static void setHsqlToDbSyntax(JDBCDataSource ds, DbSupport db) {
        try (Connection conn = ds.getConnection(); Statement st = conn.createStatement()) {
            if (db == DbSupport.ORACLE) {
                st.execute("SET DATABASE SQL SYNTAX ORA TRUE;"); // Oracle
            } else if (db == DbSupport.MSSQL) {
                st.execute("SET DATABASE SQL SYNTAX MSS TRUE;"); // MS Sql
                st.execute("SET DATABASE SQL CONCAT NULLS FALSE;"); // MS Sql
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> dump(ResultSet rs) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < columnCount; i++) {
            String label = rs.getMetaData().getColumnLabel(i + 1);
            map.put(label, rs.getObject(label));
        }
        return map;
    }

}
