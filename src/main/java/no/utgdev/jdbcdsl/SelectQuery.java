package no.utgdev.jdbcdsl;

import io.vavr.Tuple;
import io.vavr.Tuple3;
import io.vavr.control.Option;
import lombok.SneakyThrows;
import no.utgdev.jdbcdsl.order.OrderClause;
import no.utgdev.jdbcdsl.where.WhereClause;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.RowMapper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;



public class SelectQuery<T> {
    private Jdbi db;
    private String tableName;
    private List<String> columnNames;
    private Function<ResultSet, T> mapper;
    private WhereClause where;
    private OrderClause order;
    private String groupBy;
    private Integer offset;
    private Integer rowCount;
    private Tuple3<String, String, String> leftJoinOn;

    SelectQuery(Jdbi db, String tableName, Function<ResultSet, T> mapper) {
        this.db = db;
        this.tableName = tableName;
        this.columnNames = new ArrayList<>();
        this.mapper = mapper;
    }

    public SelectQuery<T> leftJoinOn(String joinTableName, String leftOn, String rightOn) {
        leftJoinOn = Tuple.of(joinTableName, leftOn, rightOn);
        return this;
    }

    public SelectQuery<T> column(String columnName) {
        this.columnNames.add(columnName);
        return this;
    }

    public SelectQuery<T> where(WhereClause where) {
        this.where = where;
        return this;
    }

    public SelectQuery<T> groupBy(String column) {
        this.groupBy = column;
        return this;
    }

    public SelectQuery<T> orderBy(OrderClause order) {
        this.order = order;
        return this;
    }

    public SelectQuery<T> limit(int offset, int rowCount) {
        this.offset = offset;
        this.rowCount = rowCount;
        return this;
    }

    public SelectQuery<T> limit(int rowCount) {
        return limit(0, rowCount);
    }

    @SneakyThrows
    public T execute() {
        validate();
        String sql = createSelectStatement();

        Object[] args = Option.of(this.where).map(WhereClause::getArgs).getOrElse(new Object[]{});

        RowMapper<T> mapper = (rs, rowNum) -> this.mapper.apply(rs);

        return db.withHandle(handle -> handle.select(sql, args).map(mapper).findFirst().orElse(null));
    }

    @SneakyThrows
    public List<T> executeToList() {
        validate();
        String sql = createSelectStatement();

        Object[] args = Option.of(this.where).map(WhereClause::getArgs).getOrElse(new Object[]{});

        RowMapper<T> mapper = (rs, rowNum) -> this.mapper.apply(rs);

        return db.withHandle(handle -> handle.select(sql, args).map(mapper).list());
    }

    private void validate() {
        if (tableName == null || columnNames.isEmpty()) {
            throw new SqlUtilsException(
                    "I need more data to create a sql-statement. " +
                            "Did you remember to specify table and columns?"
            );
        }

        if (groupBy != null && !columnNames.contains(groupBy)) {
            throw new SqlUtilsException("You have to select the column which you are grouping by.");
        }

        if (mapper == null) {
            throw new SqlUtilsException("I need a mapper function in order to return the right data type.");
        }
    }

    private String createSelectStatement() {
        StringBuilder sqlBuilder = new StringBuilder()
                .append("SELECT ");

        columnNames.stream()
                .flatMap(x -> Stream.of(", ", x))
                .skip(1)
                .forEach(sqlBuilder::append);

        sqlBuilder
                .append(" ")
                .append("FROM ")
                .append(tableName);

        if (Objects.nonNull(leftJoinOn)) {
            sqlBuilder.append(String.format(" LEFT JOIN %s ON %s.%s = %s.%s",
                    leftJoinOn._1,
                    tableName,
                    leftJoinOn._2,
                    leftJoinOn._1,
                    leftJoinOn._3));
        }

        if (this.where != null) {
            sqlBuilder
                    .append(" WHERE ");

            sqlBuilder.append(this.where.toSql());
        }

        if (this.groupBy != null) {
            sqlBuilder.append(" GROUP BY ").append(this.groupBy);
        }

        if (this.order != null) {
            sqlBuilder.append(this.order.toSql());
        }

        if (this.offset != null) {
            sqlBuilder.append(String.format(" OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", offset, rowCount));
        }

        return sqlBuilder.toString();
    }

    @Override
    public String toString() {
        return createSelectStatement();
    }

}