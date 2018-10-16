package no.utgdev.jdbcdsl;


import lombok.Data;
import lombok.SneakyThrows;
import lombok.experimental.Accessors;
import org.jdbi.v3.core.Jdbi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import static no.utgdev.jdbcdsl.SqlUtilsTest.*;


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
                .setBirthday(rs.getTimestamp(BIRTHDAY))
                .setDead(rs.getBoolean(DEAD))
                .setId(rs.getString(ID))
                .setNavn(rs.getString(NAVN))
                .setNumberOfPets(rs.getInt(NUMBER_OF_PETS));
    }

    @SneakyThrows
    public static Testobject mapperWithAddress(ResultSet rs) {
        return new Testobject()
                .setBirthday(rs.getTimestamp(BIRTHDAY))
                .setDead(rs.getBoolean(DEAD))
                .setId(rs.getString(ID))
                .setNavn(rs.getString(NAVN))
                .setNumberOfPets(rs.getInt(NUMBER_OF_PETS))
                .setAddress(rs.getString(ADDRESS));
    }

    public static SelectQuery<Testobject> getSelectWithAddressQuery(Jdbi db, String table) {
        return SqlUtils.select(db, table, Testobject::mapperWithAddress)
                .column(BIRTHDAY)
                .column(DEAD)
                .column(ID)
                .column(NAVN)
                .column(NUMBER_OF_PETS)
                .column(ADDRESS);
    }

    public static SelectQuery<Testobject> getSelectQuery(Jdbi db, String table) {
        return SqlUtils.select(db, table, Testobject::mapper)
                .column(BIRTHDAY)
                .column(DEAD)
                .column(ID)
                .column(NAVN)
                .column(NUMBER_OF_PETS);
    }

    public InsertQuery toInsertQuery(Jdbi db, String table) {
        return SqlUtils.insert(db, table)
                .value(BIRTHDAY, birthday)
                .value(ID, id)
                .value(DEAD, dead)
                .value(NUMBER_OF_PETS, numberOfPets)
                .value(NAVN, navn);
    }

    public static InsertBatchQuery<Testobject> getInsertBatchQuery(Jdbi db, String table) {
        InsertBatchQuery<Testobject> insertBatchQuery = new InsertBatchQuery<>(db, table);
        return insertBatchQuery
                .add(NAVN, Testobject::getNavn)
                .add(DEAD, Testobject::isDead)
                .add(ID, Testobject::getId)
                .add(BIRTHDAY, Testobject::getBirthday)
                .add(NUMBER_OF_PETS, Testobject::getNumberOfPets);
    }
}
