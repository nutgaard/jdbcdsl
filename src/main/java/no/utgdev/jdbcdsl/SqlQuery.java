package no.utgdev.jdbcdsl;

import org.jdbi.v3.core.Handle;

public class SqlQuery {

    private final Handle handle;
    private final String sql;

    public SqlQuery(Handle handle, String sql) {
        this.handle = handle;
        this.sql = sql;
    }

    public int execute() {
        return handle.execute(this.sql);
    }
}
