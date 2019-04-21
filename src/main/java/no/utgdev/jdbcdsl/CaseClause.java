package no.utgdev.jdbcdsl;

import io.vavr.collection.List;
import no.utgdev.jdbcdsl.value.CastValue;
import no.utgdev.jdbcdsl.where.WhereClause;
import org.apache.commons.lang3.ArrayUtils;

import static java.util.stream.Collectors.joining;

public class CaseClause<T> implements SqlFragment, SelectQuery.ColumnFragment {
    public interface WhenClause<T> extends SqlFragment {
        static <T> WhenClause<T> when(WhereClause condition, CastValue<T> value) {
            return new WhenWhereClause<T>(condition, value);
        }

        static <T> WhenClause<T> orElse(CastValue<T> value) {
            return new WhenElseClause<T>(value);
        }
    }

    static class WhenWhereClause<T> implements WhenClause {
        private final WhereClause condition;
        private final CastValue<T> value;

        private WhenWhereClause(WhereClause condition, CastValue<T> value) {
            this.condition = condition;
            this.value = value;
        }

        public String toSql() {
            return "WHEN " + condition.toSql() + " THEN " + value.getValuePlaceholder();
        }

        public Object[] getArgs() {
            Object[] conditionArgs = condition.getArgs();
            Object[] valueArgs = value.hasPlaceholder() ? new Object[]{value.getSql()} : new Object[0];
            return ArrayUtils.addAll(conditionArgs, valueArgs);
        }
    }

    static class WhenElseClause<T> implements WhenClause {
        private final CastValue value;

        private WhenElseClause(CastValue value) {
            this.value = value;
        }

        @Override
        public Object[] getArgs() {
            return value.hasPlaceholder() ? new Object[]{value.getSql()} : new Object[0];
        }

        @Override
        public String toSql() {
            return "ELSE " + value.getValuePlaceholder();
        }
    }

    private final List<WhenClause> clauses;

    private CaseClause(WhenClause<T>[] clauses) {
        this.clauses = List.of(clauses);
    }


    public static <T> CaseClause of(WhenClause<T>... clauses) {
        return new CaseClause<>(clauses);
    }

    @Override
    public String toSql() {
        String whenclauses = clauses
                .map(WhenClause::toSql)
                .collect(joining(" "));

        return "CASE " + whenclauses + " END";
    }

    @Override
    public Object[] getArgs() {
        return clauses
                .map(WhenClause::getArgs)
                .reduce(ArrayUtils::addAll);
    }
}
