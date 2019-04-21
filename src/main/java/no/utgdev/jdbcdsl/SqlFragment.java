package no.utgdev.jdbcdsl;

public interface SqlFragment {
    Object[] getArgs();
    String toSql();
    
    class StringFragment implements SqlFragment, SelectQuery.ColumnFragment {
        private final String str;

        public StringFragment(String str) {
            this.str = str;
        }

        @Override
            public Object[] getArgs() {
                return new Object[0];
            }

            @Override
            public String toSql() {
                return str;
            }
    }

    static StringFragment fromString(String str) {
        return new StringFragment(str);
    }
}
