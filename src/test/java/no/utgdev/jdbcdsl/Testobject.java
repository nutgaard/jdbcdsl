package no.utgdev.jdbcdsl;


import lombok.Data;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import org.jdbi.v3.core.Handle;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;


@Data
@Accessors(chain = true)
public class Testobject {
    String navn;
    String address;
    String id;
    Timestamp birthday;
    boolean dead;
    int numberOfPets;

    public static Testobject mapper(ResultSet rs) throws SQLException {
        return new Testobject()
                .setBirthday(rs.getTimestamp(SqlUtilsTest.BIRTHDAY))
                .setDead(rs.getBoolean(SqlUtilsTest.DEAD))
                .setId(rs.getString(SqlUtilsTest.ID))
                .setNavn(rs.getString(SqlUtilsTest.NAVN))
                .setNumberOfPets(rs.getInt(SqlUtilsTest.NUMBER_OF_PETS));
    }

    @SneakyThrows
    public static Testobject mapperWithAddress(ResultSet rs) {
        return new Testobject()
                .setBirthday(rs.getTimestamp(SqlUtilsTest.BIRTHDAY))
                .setDead(rs.getBoolean(SqlUtilsTest.DEAD))
                .setId(rs.getString(SqlUtilsTest.ID))
                .setNavn(rs.getString(SqlUtilsTest.NAVN))
                .setNumberOfPets(rs.getInt(SqlUtilsTest.NUMBER_OF_PETS))
                .setAddress(rs.getString(SqlUtilsTest.ADDRESS));
    }

    public static SelectQuery<Testobject> getSelectWithAddressQuery(Handle db, String table) {
        return SqlUtils.select(db, table, Testobject::mapperWithAddress)
                .column(SqlUtilsTest.BIRTHDAY)
                .column(SqlUtilsTest.DEAD)
                .column(SqlUtilsTest.ID)
                .column(SqlUtilsTest.NAVN)
                .column(SqlUtilsTest.NUMBER_OF_PETS)
                .column(SqlUtilsTest.ADDRESS);
    }

    public static SelectQuery<Testobject> getSelectQuery(Handle db, String table) {
        return SqlUtils.select(db, table, Testobject::mapper)
                .column(SqlUtilsTest.BIRTHDAY)
                .column(SqlUtilsTest.DEAD)
                .column(SqlUtilsTest.ID)
                .column(SqlUtilsTest.NAVN)
                .column(SqlUtilsTest.NUMBER_OF_PETS);
    }

    public InsertQuery toInsertQuery(Handle db, String table) {
        return SqlUtils.insert(db, table)
                .value(SqlUtilsTest.BIRTHDAY, birthday)
                .value(SqlUtilsTest.ID, id)
                .value(SqlUtilsTest.DEAD, dead)
                .value(SqlUtilsTest.NUMBER_OF_PETS, numberOfPets)
                .value(SqlUtilsTest.NAVN, navn);
    }

    public static InsertBatchQuery<Testobject> getInsertBatchQuery(Handle db, String table) {
        InsertBatchQuery<Testobject> insertBatchQuery = new InsertBatchQuery<>(db, table);
        return insertBatchQuery
                .add(SqlUtilsTest.NAVN, Testobject::getNavn)
                .add(SqlUtilsTest.DEAD, Testobject::isDead)
                .add(SqlUtilsTest.ID, Testobject::getId)
                .add(SqlUtilsTest.BIRTHDAY, Testobject::getBirthday)
                .add(SqlUtilsTest.NUMBER_OF_PETS, Testobject::getNumberOfPets);
    }
}
