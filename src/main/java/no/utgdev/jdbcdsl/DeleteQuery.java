package no.utgdev.jdbcdsl;

import lombok.SneakyThrows;
import no.utgdev.jdbcdsl.where.WhereClause;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.statement.Update;


public class DeleteQuery {
    private final Handle db;
    private final String tableName;
    private WhereClause where;

    DeleteQuery(Handle db, String tableName) {
        this.db = db;
        this.tableName = tableName;
    }

    public DeleteQuery where(WhereClause where) {
        this.where = where;
        return this;
    }

    @SneakyThrows
    public int execute() {
        if (tableName == null || this.where == null) {
            throw new SqlUtilsException(
                    "I need more data to create a sql-statement. " +
                            "Did you remember to specify table and a where clause?"
            );
        }

        String sql = createDeleteStatement();
        Update update = db.createUpdate(sql);
        if (this.where.getArgs().length > 0) {
            update.bind(0, this.where.getArgs()[0]);
        }
        return update.execute();
    }

    private String createDeleteStatement() {
        return String.format(
                "DELETE FROM %s WHERE %s",
                tableName,
                this.where.toSql()
        );
    }

    @Override
    public String toString() {
        return createDeleteStatement();
    }
}
