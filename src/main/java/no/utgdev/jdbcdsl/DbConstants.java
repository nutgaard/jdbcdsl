package no.utgdev.jdbcdsl;


import no.utgdev.jdbcdsl.value.ConstantValue;

public enum DbConstants {
    CURRENT_TIMESTAMP("CURRENT_TIMESTAMP"), NULL("NULL");

    public final String sql;

    DbConstants(String sql) {
        this.sql = sql;
    }

    public static ConstantValue nextSeq(String seq) {
        if (SqlUtils.db == SqlUtils.DbSupport.ORACLE) {
            return new ConstantValue(String.format("%s.NEXTVAL", seq)); // Oracle
        } else if (SqlUtils.db == SqlUtils.DbSupport.MSSQL) {
            return new ConstantValue(String.format("NEXT VALUE FOR %s", seq)); // MS Sql
        } else {
            return new ConstantValue(String.format("%s.NEXTVAL", seq));
        }
    }
}
