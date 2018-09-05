package no.utgdev.jdbcdsl;

import no.utgdev.jdbcdsl.order.OrderClause;
import no.utgdev.jdbcdsl.where.WhereClause;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Batch;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class SqlUtilsTest {

    public final static String TESTTABLE1 = "TESTTABLE1";
    public final static String TESTTABLE2 = "TESTTABLE2";
    public final static String ID = "ID";
    public final static String NAVN = "NAVN";
    public final static String DEAD = "DEAD";
    public final static String BIRTHDAY = "BIRTHDAY";
    public final static String NUMBER_OF_PETS = "NUMBER_OF_PETS";
    public final static String ADDRESS = "ADDRESS";
    public final static String TEST_ID_SEQ = "TEST_ID_SEQ";

    private Jdbi db;

    @Before
    public void setup() {
        db = TestUtils.jdbcTemplate();
        db.useHandle(handle -> {
            Batch batch = handle.createBatch();
            batch.add("CREATE TABLE TESTTABLE1 (\n" +
                    "  ID VARCHAR(255) NOT NULL,\n" +
                    "  NAVN VARCHAR(255) NOT NULL,\n" +
                    "  DEAD VARCHAR(20),\n" +
                    "  BIRTHDAY TIMESTAMP,\n" +
                    "  NUMBER_OF_PETS NUMBER,\n" +
                    "  PRIMARY KEY(ID)\n" +
                    ")");
            batch.add("CREATE TABLE TESTTABLE2 (\n" +
                    "  ID VARCHAR(255) NOT NULL,\n" +
                    "  ADDRESS VARCHAR(255),\n" +
                    "  PRIMARY KEY(ID)\n" +
                    ")"
            );
            batch.add("CREATE SEQUENCE TEST_ID_SEQ START WITH 1 INCREMENT BY 1");
            batch.execute();
        });
    }

    @Test
    public void insertAndSelect() {
        Testobject object = getTestobjectWithId("007");

        SqlUtils.insert(db, TESTTABLE1)
                .value(ID, object.getId())
                .value(NAVN, object.getNavn())
                .value(DEAD, object.isDead())
                .value(BIRTHDAY, object.getBirthday())
                .value(NUMBER_OF_PETS, object.getNumberOfPets())
                .execute();

        Testobject retrieved = Testobject.getSelectQuery(db, TESTTABLE1)
                .where(WhereClause.equals(ID, object.getId()))
                .execute();

        assertThat(object).isEqualTo(retrieved);
    }

    @Test
    public void insertWithNextSequenceId() {
        SqlUtils.insert(db, TESTTABLE1)
                .value(ID, DbConstants.nextSeq(TEST_ID_SEQ))
                .value(NAVN, DbConstants.CURRENT_TIMESTAMP)
                .value(DEAD, true)
                .execute();

        SqlUtils.insert(db, TESTTABLE1)
                .value(ID, DbConstants.nextSeq(TEST_ID_SEQ))
                .value(NAVN, DbConstants.CURRENT_TIMESTAMP)
                .value(DEAD, false)
                .execute();

        List<Testobject> testobjects = Testobject.getSelectQuery(db, TESTTABLE1).executeToList();
        assertThat(testobjects.size()).isEqualTo(2);
        assertThat(testobjects.stream().map(Testobject::getId).collect(Collectors.toList())).containsExactly("1", "2");

    }

    @Test
    public void updatequery() {
        String oppdatertNavn = "oppdatert navn";
        getTestobjectWithId("007").toInsertQuery(db, TESTTABLE1).execute();
        SqlUtils.update(db, TESTTABLE1).set(NAVN, oppdatertNavn)
                .whereEquals(ID, "007").execute();

        Testobject retrieved = Testobject.getSelectQuery(db, TESTTABLE1)
                .where(WhereClause.equals(ID, "007")).execute();

        assertThat(retrieved.getNavn()).isEqualTo(oppdatertNavn);
    }

    @Test
    public void updateBatchQuery() {
        String oppdatertNavn = "oppdatert navn";
        List<Testobject> objects = new ArrayList<>();
        objects.add(getTestobjectWithId("001"));
        objects.add(getTestobjectWithId("002"));
        objects.add(getTestobjectWithId("003"));
        objects.add(getTestobjectWithId("004"));
        objects.add(getTestobjectWithId("005"));
        objects.add(getTestobjectWithId("006"));
        objects.add(getTestobjectWithId("007"));

        Testobject.getInsertBatchQuery(db, TESTTABLE1)
                .execute(objects);

        UpdateBatchQuery<Testobject> updateBatchQuery = new UpdateBatchQuery<>(db, TESTTABLE1);
        List<Testobject> updateObjects = new ArrayList<>();
        updateObjects.add(getTestobjectWithId("001").setNavn(oppdatertNavn));
        updateObjects.add(getTestobjectWithId("002").setNavn(oppdatertNavn));
        updateObjects.add(getTestobjectWithId("003").setNavn(oppdatertNavn));
        updateObjects.add(getTestobjectWithId("004").setNavn(oppdatertNavn));
        updateObjects.add(getTestobjectWithId("005").setNavn(oppdatertNavn));
        updateObjects.add(getTestobjectWithId("006").setNavn(oppdatertNavn));
        updateObjects.add(getTestobjectWithId("007").setNavn(oppdatertNavn));

        updateBatchQuery
                .add(NAVN, Testobject::getNavn, String.class)
                .addWhereClause(object -> WhereClause.equals(ID, object.getId())).execute(updateObjects);

        List<Testobject> retrieved = Testobject.getSelectQuery(db, TESTTABLE1)
                .where(WhereClause.in(ID, asList("001", "002", "003", "004", "005", "006", "007"))).executeToList();

        assertThat(retrieved.stream().map(Testobject::getNavn).distinct().collect(Collectors.toList())).containsOnly(oppdatertNavn);
    }

    @Test
    public void selectNextFromSequens() {
        Long id1 = SqlUtils.nextFromSeq(db, TEST_ID_SEQ).execute();
        Long id2 = SqlUtils.nextFromSeq(db, TEST_ID_SEQ).execute();
        assertThat(id1).isEqualTo(1);
        assertThat(id2).isEqualTo(2);
    }

    @Test
    public void deleteQuery() {
        getTestobjectWithId("007").toInsertQuery(db, TESTTABLE1).execute();
        assertThat(Testobject.getSelectQuery(db, TESTTABLE1).where(WhereClause.equals(ID, "007")).execute()).isNotNull();

        SqlUtils.delete(db, TESTTABLE1).where(WhereClause.equals(ID, "007")).execute();

        assertThat(Testobject.getSelectQuery(db, TESTTABLE1).where(WhereClause.equals(ID, "007")).execute()).isNull();
    }

    @Test
    public void batchInsertAndSelect() {
        List<Testobject> objects = new ArrayList<>();
        objects.add(getTestobjectWithId("001"));
        objects.add(getTestobjectWithId("002"));
        objects.add(getTestobjectWithId("003"));
        objects.add(getTestobjectWithId("004"));
        objects.add(getTestobjectWithId("005"));
        objects.add(getTestobjectWithId("006"));
        objects.add(getTestobjectWithId("007"));

        Testobject.getInsertBatchQuery(db, TESTTABLE1)
                .execute(objects);

        List<Testobject> retrieved = SqlUtils.select(db, TESTTABLE1, Testobject::mapper)
                .column(ID)
                .column(NAVN)
                .column(BIRTHDAY)
                .column(DEAD)
                .column(NUMBER_OF_PETS)
                .where(WhereClause.in(ID, asList("001", "002", "003", "004", "005", "006", "007")))
                .executeToList();

        assertThat(retrieved).isEqualTo(objects);
    }

    @Test
    public void leftJoinOn() {
        getTestobjectWithId("007").toInsertQuery(db, TESTTABLE1).execute();
        db.useHandle(handle -> handle.createUpdate("INSERT INTO TESTTABLE2 (ID, ADDRESS) VALUES ('007', 'andeby')").execute());
        Testobject retrieved = Testobject
                .getSelectWithAddressQuery(db, TESTTABLE1)
                .leftJoinOn(TESTTABLE2, ID, ID)
                .where(WhereClause.equals(ID, "007"))
                .execute();

        assertThat(retrieved.getAddress()).isEqualTo("andeby");
    }

    @Test
    public void selectAll() {
        getTestobjectWithId("001").toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("002").toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("003").toInsertQuery(db, TESTTABLE1).execute();
        List<Testobject> testobjects = Testobject.getSelectQuery(db, TESTTABLE1).executeToList();

        assertThat(testobjects.size()).isEqualTo(3);
    }

    @Test
    public void orderByDesc() {
        getTestobjectWithId("001").setNumberOfPets(0).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("002").setNumberOfPets(5).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("003").setNumberOfPets(10).toInsertQuery(db, TESTTABLE1).execute();

        List<Testobject> testobjects = Testobject.getSelectQuery(db, TESTTABLE1)
                .orderBy(OrderClause.desc("NUMBER_OF_PETS"))
                .executeToList();

        assertThat(testobjects.size()).isEqualTo(3);
        assertThat(testobjects.get(0).numberOfPets).isEqualTo(10);
        assertThat(testobjects.get(0).id).isEqualTo("003");
    }

    @Test
    public void orderByAsc() {
        getTestobjectWithId("001").setNumberOfPets(10).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("002").setNumberOfPets(5).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("003").setNumberOfPets(0).toInsertQuery(db, TESTTABLE1).execute();

        List<Testobject> testobjects = Testobject.getSelectQuery(db, TESTTABLE1)
                .orderBy(OrderClause.asc("NUMBER_OF_PETS"))
                .executeToList();

        assertThat(testobjects.size()).isEqualTo(3);
        assertThat(testobjects.get(0).numberOfPets).isEqualTo(0);
        assertThat(testobjects.get(0).id).isEqualTo("003");
    }

    @Test
    public void orderAndWhere() {
        getTestobjectWithId("001").setDead(true).setNumberOfPets(0).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("002").setDead(true).setNumberOfPets(5).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("003").setDead(true).setNumberOfPets(10).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("004").setNumberOfPets(20).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("005").setNumberOfPets(25).toInsertQuery(db, TESTTABLE1).execute();

        List<Testobject> testobjects = Testobject.getSelectQuery(db, TESTTABLE1)
                .where(WhereClause.equals("DEAD", true))
                .orderBy(OrderClause.desc("NUMBER_OF_PETS"))
                .executeToList();

        assertThat(testobjects.size()).isEqualTo(3);
        assertThat(testobjects.get(0).numberOfPets).isEqualTo(10);
        assertThat(testobjects.get(0).id).isEqualTo("003");
    }

    @Test
    public void whereIsNotNull() {
        getTestobjectWithId("007").setBirthday(null).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("006").toInsertQuery(db, TESTTABLE1).execute();

        List<Testobject> birthdayNotNull = Testobject.getSelectQuery(db, TESTTABLE1).where(WhereClause.isNotNull(BIRTHDAY)).executeToList();
        List<Testobject> birthdayNull = Testobject.getSelectQuery(db, TESTTABLE1).where(WhereClause.isNull(BIRTHDAY)).executeToList();

        assertThat(birthdayNotNull.size()).isEqualTo(1);
        assertThat(birthdayNull.size()).isEqualTo(1);

        assertThat(birthdayNotNull.get(0).getBirthday()).isNotNull();
        assertThat(birthdayNull.get(0).getBirthday()).isNull();
    }

    @Test
    public void limit() {
        getTestobjectWithId("003").setDead(true).setNumberOfPets(2).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("004").setDead(true).setNumberOfPets(3).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("009").setDead(true).setNumberOfPets(8).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("007").setDead(true).setNumberOfPets(6).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("002").setDead(true).setNumberOfPets(1).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("008").setDead(true).setNumberOfPets(7).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("001").setDead(true).setNumberOfPets(0).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("006").setDead(true).setNumberOfPets(5).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("005").setDead(true).setNumberOfPets(4).toInsertQuery(db, TESTTABLE1).execute();

        List<Testobject> testobjects = Testobject.getSelectQuery(db, TESTTABLE1)
                .orderBy(OrderClause.asc(NUMBER_OF_PETS))
                .limit(5)
                .executeToList();

        assertThat(testobjects.stream()
                .map(Testobject::getNumberOfPets).collect(Collectors.toList())).isEqualTo(asList(0, 1, 2, 3, 4));
    }

    @Test
    public void limitWithOffset() {
        getTestobjectWithId("003").setDead(true).setNumberOfPets(2).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("004").setDead(true).setNumberOfPets(3).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("009").setDead(true).setNumberOfPets(8).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("007").setDead(true).setNumberOfPets(6).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("002").setDead(true).setNumberOfPets(1).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("008").setDead(true).setNumberOfPets(7).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("001").setDead(true).setNumberOfPets(0).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("006").setDead(true).setNumberOfPets(5).toInsertQuery(db, TESTTABLE1).execute();
        getTestobjectWithId("005").setDead(true).setNumberOfPets(4).toInsertQuery(db, TESTTABLE1).execute();

        List<Testobject> testobjects = Testobject.getSelectQuery(db, TESTTABLE1)
                .orderBy(OrderClause.asc(NUMBER_OF_PETS))
                .limit(2, 5)
                .executeToList();

        assertThat(testobjects.stream()
                .map(Testobject::getNumberOfPets).collect(Collectors.toList())).isEqualTo(asList(2, 3, 4, 5, 6));
    }

    @Test
    public void whereComparativTest() {
        List<Testobject> objects = new ArrayList<>();
        objects.add(getTestobjectWithId("001"));
        objects.add(getTestobjectWithId("002"));
        objects.add(getTestobjectWithId("003"));
        objects.add(getTestobjectWithId("004"));

        Testobject.getInsertBatchQuery(db, TESTTABLE1).execute(objects);

        int greaterThenTwo = Testobject.getSelectQuery(db, TESTTABLE1)
                .where(WhereClause.gt("ID", "002"))
                .executeToList()
                .size();

        int greaterThenOrEqualTwo = Testobject.getSelectQuery(db, TESTTABLE1)
                .where(WhereClause.gteq("ID", "002"))
                .executeToList()
                .size();

        int lessThenTwo = Testobject.getSelectQuery(db, TESTTABLE1)
                .where(WhereClause.lt("ID", "002"))
                .executeToList()
                .size();

        int lessThenOrEqualTwo = Testobject.getSelectQuery(db, TESTTABLE1)
                .where(WhereClause.lteq("ID", "002"))
                .executeToList()
                .size();

        assertThat(greaterThenTwo).isEqualTo(2);
        assertThat(greaterThenOrEqualTwo).isEqualTo(3);
        assertThat(lessThenTwo).isEqualTo(1);
        assertThat(lessThenOrEqualTwo).isEqualTo(2);
    }

    @Test
    public void groupByTest() {
        List<Testobject> objects = new ArrayList<>();
        objects.add(getTestobjectWithId("001"));
        objects.add(getTestobjectWithId("002"));
        objects.add(getTestobjectWithId("003"));
        objects.add(getTestobjectWithId("004"));
        objects.get(1).dead = true;
        objects.get(3).dead = true;

        Testobject.getInsertBatchQuery(db, TESTTABLE1).execute(objects);

        Map<Boolean, Integer> grouped = SqlUtils.select(db, TESTTABLE1, (resultset) -> {
            Map<Boolean, Integer> result = new HashMap<>();
            do {
                boolean dead = resultset.getBoolean("dead");
                int nof = resultset.getInt("nof");
                result.put(dead, nof);
            } while (resultset.next());
            return result;
        })
                .column("dead")
                .column("count(*) as nof")
                .groupBy("dead")
                .execute();

        assertThat(grouped.get(true)).isEqualTo(2);
        assertThat(grouped.get(false)).isEqualTo(2);
    }

    @Test
    public void insertCurrentTimestamp() {
        SqlUtils.insert(db, TESTTABLE1)
                .value(ID, "001")
                .value(NAVN, "navn")
                .value(BIRTHDAY, DbConstants.CURRENT_TIMESTAMP)
                .execute();

        Testobject object = Testobject.getSelectQuery(db, TESTTABLE1).execute();
        assertThat(object.getBirthday()).isNotNull();
    }

    @Test
    public void updateCurrentTimestamp() {
        Timestamp epoch0 = new Timestamp(0);
        SqlUtils.insert(db, TESTTABLE1)
                .value(ID, "001")
                .value(NAVN, "navn")
                .value(BIRTHDAY, epoch0)
                .execute();

        SqlUtils.update(db, TESTTABLE1)
                .set(BIRTHDAY, DbConstants.CURRENT_TIMESTAMP)
                .whereEquals(ID, "001")
                .execute();

        Testobject object = Testobject.getSelectQuery(db, TESTTABLE1).execute();
        assertThat(object.getBirthday()).isAfter(new Timestamp(0));
    }

    @Test
    public void batchInsertWithCurrentTimestamp() {
        InsertBatchQuery<Testobject> insertBatchQuery = new InsertBatchQuery<>(db, TESTTABLE1);
        insertBatchQuery
                .add(ID, Testobject::getId, String.class)
                .add(NAVN, Testobject::getNavn, String.class)
                .add(BIRTHDAY, DbConstants.CURRENT_TIMESTAMP);

        List<Testobject> objects = new ArrayList<>();
        objects.add(getTestobjectWithId("001"));
        objects.add(getTestobjectWithId("002"));

        insertBatchQuery.execute(objects);

        List<Testobject> retrievedObjects = Testobject.getSelectQuery(db, TESTTABLE1).executeToList();
        assertThat(retrievedObjects.get(0)).isNotNull();
        assertThat(retrievedObjects.get(1)).isNotNull();
    }

    @Test
    public void batchUpdateWithCurrentTimestamp() {
        InsertBatchQuery<Testobject> insertBatchQuery = new InsertBatchQuery<>(db, TESTTABLE1);
        insertBatchQuery
                .add(ID, Testobject::getId, String.class)
                .add(NAVN, Testobject::getNavn, String.class)
                .add(BIRTHDAY, Testobject::getBirthday, Timestamp.class);

        List<Testobject> objects = new ArrayList<>();
        objects.add(getTestobjectWithId("001"));
        objects.add(getTestobjectWithId("002"));

        insertBatchQuery.execute(objects);

        UpdateBatchQuery<Testobject> updateBatchQuery = new UpdateBatchQuery<>(db, TESTTABLE1);
        updateBatchQuery.add(BIRTHDAY, DbConstants.CURRENT_TIMESTAMP);

        List<Testobject> updateobjects = new ArrayList<>();
        updateobjects.add(getTestobjectWithId("001"));
        updateobjects.add(getTestobjectWithId("002"));

        updateBatchQuery.execute(updateobjects);

        List<Testobject> retrievedObjects = Testobject.getSelectQuery(db, TESTTABLE1).executeToList();
        assertThat(retrievedObjects.get(0).getBirthday()).isAfter(new Timestamp(0));
        assertThat(retrievedObjects.get(1).getBirthday()).isAfter(new Timestamp(0));
    }

    private Testobject getTestobjectWithId(String id) {
        return new Testobject()
                .setNavn("navn navnesen")
                .setId(id)
                .setBirthday(new Timestamp(0))
                .setNumberOfPets(4)
                .setDead(false);
    }
}