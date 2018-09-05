package no.utgdev.jdbcdsl;


import no.utgdev.jdbcdsl.value.ConstantValue;

public enum DbConstants {
    CURRENT_TIMESTAMP("CURRENT_TIMESTAMP"), NULL("NULL");

    public final String sql;

    DbConstants(String sql) {
        this.sql = sql;
    }

    public static ConstantValue nextSeq(String seq) {
        return new ConstantValue(String.format("%s.NEXTVAL", seq));
    }
}