package no.utgdev.jdbcdsl;

import io.vavr.Tuple;
import io.vavr.Tuple3;
import io.vavr.collection.List;
import io.vavr.control.Option;
import lombok.SneakyThrows;
import no.utgdev.jdbcdsl.order.OrderByExpression;
import no.utgdev.jdbcdsl.where.WhereClause;
import org.apache.commons.lang3.ArrayUtils;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.mapper.RowMapper;

import java.sql.ResultSet;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;


public class SelectQuery<T> {
    public interface ColumnFragment extends SqlFragment {
        default AsClause as(String name) {
            return AsClause.of(this, name);
        }
    }

    private Handle db;
    private String tableName;
    private List<ColumnFragment> columnNames;
    private Function<ResultSet, T> mapper;
    private WhereClause where;
    private List<OrderByExpression> orderByExpressions;
    private String groupBy;
    private Integer offset;
    private Integer rowCount;
    private Tuple3<String, String, String> leftJoinOn;

    SelectQuery(Handle db, String tableName, Function<ResultSet, T> mapper) {
        this.db = db;
        this.tableName = tableName;
        this.columnNames = List.empty();
        this.orderByExpressions = List.empty();
        this.mapper = mapper;
    }

    public SelectQuery<T> leftJoinOn(String joinTableName, String leftOn, String rightOn) {
        leftJoinOn = Tuple.of(joinTableName, leftOn, rightOn);
        return this;
    }

    public SelectQuery<T> column(String columnName) {
        this.columnNames = this.columnNames.append(SqlFragment.fromString(columnName));
        return this;
    }

    public SelectQuery<T> column(ColumnFragment caseClause) {
        this.columnNames = this.columnNames.append(caseClause);
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

    public SelectQuery<T> orderBy(final OrderByExpression orderByExpression) {
        this.orderByExpressions = this.orderByExpressions.append(orderByExpression);
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
    public Option<T> execute() {
        Tuple3<String, Object[], RowMapper<T>> context = prepareExecution();

        return Option.ofOptional(db.select(context._1, context._2).map(context._3).findFirst());
    }

    @SneakyThrows
    public java.util.List<T> executeToList() {
        Tuple3<String, Object[], RowMapper<T>> context = prepareExecution();

        return db.select(context._1, context._2).map(context._3).list();
    }

    private Tuple3<String, Object[], RowMapper<T>> prepareExecution() {
        validate();
        return Tuple.of(
                createSelectStatement(),
                createObjectArgs(),
                (rs, rowNum) -> this.mapper.apply(rs)
        );
    }

    private void validate() {
        if (tableName == null || columnNames.isEmpty()) {
            throw new SqlUtilsException(
                    "I need more data to create a sql-statement. " +
                            "Did you remember to specify table and columns?"
            );
        }

        boolean hasSelectOnGroupBy = columnNames
                .find((fragment) -> fragment instanceof SqlFragment.StringFragment && fragment.toSql().equals(groupBy) ||
                                fragment instanceof AsClause && ((AsClause)fragment).getName().equals(groupBy)
                        )
                .isDefined();

        if (groupBy != null && !hasSelectOnGroupBy) {
            throw new SqlUtilsException("You have to select the column which you are grouping by.");
        }

        if (mapper == null) {
            throw new SqlUtilsException("I need a mapper function in order to return the right data type.");
        }
    }

    private Object[] createObjectArgs() {
        Object[] columnArgs = this.columnNames
                .map(ColumnFragment::getArgs)
                .reduce(ArrayUtils::addAll);

        Object[] whereArgs = Option.of(this.where).map(WhereClause::getArgs).getOrElse(new Object[]{});

        return ArrayUtils.addAll(columnArgs, whereArgs);
    }

    private String createSelectStatement() {
        StringBuilder sqlBuilder = new StringBuilder()
                .append("SELECT ");

        columnNames
                .toJavaList()
                .stream()
                .map(SqlFragment::toSql)
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

        if (orderByExpressions != null && !orderByExpressions.isEmpty()) {
            sqlBuilder.append(" ORDER BY ");
            orderByExpressions
                    .toJavaList()
                    .stream()
                    .flatMap(orderByExpression -> Stream.of(", ", orderByExpression.toSql()))
                    .skip(1)
                    .forEach(sqlBuilder::append);
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
