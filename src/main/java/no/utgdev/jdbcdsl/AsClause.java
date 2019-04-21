package no.utgdev.jdbcdsl;

public class AsClause implements SqlFragment, SelectQuery.ColumnFragment{
    private final SelectQuery.ColumnFragment fragment;
    private final String name;

    private AsClause(SelectQuery.ColumnFragment fragment, String name) {
        if (name == null || name.length() == 0) {
            throw new SqlUtilsException("'name' cannot be null or empty");
        }

        this.fragment = fragment;
        this.name = name;
    }

    public static AsClause of(SelectQuery.ColumnFragment fragment, String name) {
        return new AsClause(fragment, name);
    }

    @Override
    public Object[] getArgs() {
        return fragment.getArgs();
    }

    @Override
    public String toSql() {
        return fragment.toSql() + " as " + name;
    }

    public String getName() {
        return name;
    }
}
