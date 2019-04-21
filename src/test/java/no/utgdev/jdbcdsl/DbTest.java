package no.utgdev.jdbcdsl;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.control.Try;
import no.utgdev.jdbcdsl.order.OrderByExpression;
import no.utgdev.jdbcdsl.where.WhereClause;
import org.assertj.core.api.Assertions;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.Batch;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static no.utgdev.jdbcdsl.CaseClause.WhenClause.orElse;
import static no.utgdev.jdbcdsl.CaseClause.WhenClause.when;
import static no.utgdev.jdbcdsl.value.Value.Int;
import static org.assertj.core.api.Assertions.assertThat;

abstract public class DbTest {

    public final static String TESTTABLE1 = "TESTTABLE1";
    public final static String TESTTABLE2 = "TESTTABLE2";
    public final static String ID = "ID";
    public final static String NAVN = "NAVN";
    public final static String DEAD = "DEAD";
    public final static String BIRTHDAY = "BIRTHDAY";
    public final static String NUMBER_OF_PETS = "NUMBER_OF_PETS";
    public final static String ADDRESS = "ADDRESS";
    public final static String TEST_ID_SEQ = "TEST_ID_SEQ";

    protected Jdbi db;

    @Before
    public void setup() {
        db.useHandle(handle -> {
            Batch batch = handle.createBatch();
            batch.add("CREATE TABLE TESTTABLE1 (\n" +
                    "  ID VARCHAR(255) NOT NULL,\n" +
                    "  NAVN VARCHAR(255) NOT NULL,\n" +
                    "  DEAD VARCHAR(20),\n" +
                    "  BIRTHDAY TIMESTAMP,\n" +
                    "  NUMBER_OF_PETS NUMERIC,\n" +
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
    public void insert_and_select() {
        Testobject object = getTestobjectWithId("007");

        Testobject retrieved = db.withHandle(handle -> {
            insert(object, handle);

            return Testobject.getSelectQuery(handle, TESTTABLE1)
                    .where(WhereClause.equals(ID, object.getId()))
                    .execute();
        }).get();

        assertThat(object).isEqualTo(retrieved);
    }

    @Test
    public void insert_with_next_sequence_id() {
        List<Testobject> testobjects = db.withHandle(handle -> {
            SqlUtils.insert(handle, TESTTABLE1)
                    .value(ID, DbConstants.nextSeq(TEST_ID_SEQ))
                    .value(NAVN, DbConstants.CURRENT_TIMESTAMP)
                    .value(DEAD, true)
                    .execute();

            SqlUtils.insert(handle, TESTTABLE1)
                    .value(ID, DbConstants.nextSeq(TEST_ID_SEQ))
                    .value(NAVN, DbConstants.CURRENT_TIMESTAMP)
                    .value(DEAD, false)
                    .execute();

            return Testobject.getSelectQuery(handle, TESTTABLE1).executeToList();
        });
        assertThat(testobjects).hasSize(2);
        assertThat(testobjects.stream().map(Testobject::getId).collect(Collectors.toList())).containsExactly("1", "2");

    }

    @Test
    public void updatequery() {
        String oppdatertNavn = "oppdatert navn";
        Testobject retrieved = db.withHandle(handle -> {
            getTestobjectWithId("007").toInsertQuery(handle, TESTTABLE1).execute();
            SqlUtils.update(handle, TESTTABLE1).set(NAVN, oppdatertNavn)
                    .whereEquals(ID, "007").execute();

            return Testobject.getSelectQuery(handle, TESTTABLE1)
                    .where(WhereClause.equals(ID, "007")).execute();
        }).get();

        assertThat(retrieved.getNavn()).isEqualTo(oppdatertNavn);
    }

    @Test
    public void update_batch_query() {
        String oppdatertNavn = "oppdatert navn";

        List<Testobject> retrieved = db.withHandle(handle -> {
            List<Testobject> objects = new ArrayList<>();
            objects.add(getTestobjectWithId("001"));
            objects.add(getTestobjectWithId("002"));
            objects.add(getTestobjectWithId("003"));
            objects.add(getTestobjectWithId("004"));
            objects.add(getTestobjectWithId("005"));
            objects.add(getTestobjectWithId("006"));
            objects.add(getTestobjectWithId("007"));


            Testobject.getInsertBatchQuery(handle, TESTTABLE1)
                    .execute(objects);

            UpdateBatchQuery<Testobject> updateBatchQuery = new UpdateBatchQuery<>(handle, TESTTABLE1);
            List<Testobject> updateObjects = new ArrayList<>();
            updateObjects.add(getTestobjectWithId("001").setNavn(oppdatertNavn));
            updateObjects.add(getTestobjectWithId("002").setNavn(oppdatertNavn));
            updateObjects.add(getTestobjectWithId("003").setNavn(oppdatertNavn));
            updateObjects.add(getTestobjectWithId("004").setNavn(oppdatertNavn));
            updateObjects.add(getTestobjectWithId("005").setNavn(oppdatertNavn));
            updateObjects.add(getTestobjectWithId("006").setNavn(oppdatertNavn));
            updateObjects.add(getTestobjectWithId("007").setNavn(oppdatertNavn));

            updateBatchQuery
                    .add(NAVN, Testobject::getNavn)
                    .addWhereClause(object -> WhereClause.equals(ID, object.getId())).execute(updateObjects);

            return Testobject.getSelectQuery(handle, TESTTABLE1)
                    .where(WhereClause.in(ID, asList("001", "002", "003", "004", "005", "006", "007"))).executeToList();
        });

        assertThat(retrieved.stream().map(Testobject::getNavn).distinct().collect(Collectors.toList())).containsOnly(oppdatertNavn);
    }

    @Test
    public void delete_query() {
        Tuple2<Testobject, Testobject> retrieved = db.withHandle(handle -> {
            getTestobjectWithId("007").toInsertQuery(handle, TESTTABLE1).execute();
            Testobject retrieved1 = Testobject.getSelectQuery(handle, TESTTABLE1).where(WhereClause.equals(ID, "007")).execute().getOrElse((Testobject) null);

            SqlUtils.delete(handle, TESTTABLE1).where(WhereClause.equals(ID, "007")).execute();

            Testobject retrieved2 = Testobject.getSelectQuery(handle, TESTTABLE1).where(WhereClause.equals(ID, "007")).execute().getOrElse((Testobject) null);
            return Tuple.of(retrieved1, retrieved2);
        });

        assertThat(retrieved._1).isNotNull();
        assertThat(retrieved._2).isNull();
    }

    @Test
    public void batch_insert_and_select() {
        List<Testobject> objects = new ArrayList<>();
        objects.add(getTestobjectWithId("001"));
        objects.add(getTestobjectWithId("002"));
        objects.add(getTestobjectWithId("003"));
        objects.add(getTestobjectWithId("004"));
        objects.add(getTestobjectWithId("005"));
        objects.add(getTestobjectWithId("006"));
        objects.add(getTestobjectWithId("007"));

        List<Testobject> retrieved = db.withHandle(handle -> {

            Testobject.getInsertBatchQuery(handle, TESTTABLE1)
                    .execute(objects);

            return SqlUtils.select(handle, TESTTABLE1, Testobject::mapper)
                    .column(ID)
                    .column(NAVN)
                    .column(BIRTHDAY)
                    .column(DEAD)
                    .column(NUMBER_OF_PETS)
                    .where(WhereClause.in(ID, asList("001", "002", "003", "004", "005", "006", "007")))
                    .executeToList();
        });

        Assertions.assertThat(retrieved).isEqualTo(objects);
    }

    @Test
    public void left_join_on() {
        Testobject retrieved = db.withHandle(handle -> {
            getTestobjectWithId("007").toInsertQuery(handle, TESTTABLE1).execute();
            handle.createUpdate("INSERT INTO TESTTABLE2 (ID, ADDRESS) VALUES ('007', 'andeby')").execute();

            return Testobject
                    .getSelectWithAddressQuery(handle, TESTTABLE1)
                    .leftJoinOn(TESTTABLE2, ID, ID)
                    .where(WhereClause.equals(ID, "007"))
                    .execute();
        }).get();

        assertThat(retrieved.getAddress()).isEqualTo("andeby");
    }

    @Test
    public void select_all() {
        List<Testobject> testobjects = db.withHandle(handle -> {
            getTestobjectWithId("001").toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("002").toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("003").toInsertQuery(handle, TESTTABLE1).execute();
            return Testobject.getSelectQuery(handle, TESTTABLE1).executeToList();
        });

        assertThat(testobjects).hasSize(3);
    }

    @Test
    public void select_med_rename() {
        List<Map<String, Object>> result = db.withHandle(handle -> {
            getTestobjectWithId("001").toInsertQuery(handle, TESTTABLE1).execute();

            return SqlUtils.select(handle, TESTTABLE1, TestUtils::dump)
                    .column(DbTest.ID)
                    .column(DbTest.BIRTHDAY)
                    .column(SqlFragment.fromString(DbTest.BIRTHDAY).as("STARTOFLIFE"))
                    .executeToList();
        });

        assertThat(result).hasSize(1);
        assertThat(result.get(0).get(DbTest.ID)).isEqualTo("001");
        assertThat(result.get(0).get("STARTOFLIFE")).isEqualTo(result.get(0).get(DbTest.BIRTHDAY));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void select_case_when() {
        List<Map<String, Object>> result = db.withHandle(handle -> {
            getTestobjectWithId("001").toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("002").toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("003").toInsertQuery(handle, TESTTABLE1).execute();

            return SqlUtils.select(handle, TESTTABLE1, TestUtils::dump)
                    .column(DbTest.BIRTHDAY)
                    .column(DbTest.DEAD)
                    .column(DbTest.ID)
                    .column(DbTest.NAVN)
                    .column(DbTest.NUMBER_OF_PETS)
                    .column(CaseClause.of(
                            when(WhereClause.like(DbTest.ID, "%1"), Int(3)),
                            when(WhereClause.like(DbTest.ID, "%2"), Int(2)),
                            when(WhereClause.like(DbTest.ID, "%3"), Int(1)),
                            orElse(Int(0))
                    ).as("matchScore"))
                    .orderBy(OrderByExpression.asc("matchScore"))
                    .executeToList();
        });

        assertThat(result).hasSize(3);
        assertThat(result.get(0).get(DbTest.ID)).isEqualTo("003");
        assertThat(result.get(1).get(DbTest.ID)).isEqualTo("002");
        assertThat(result.get(2).get(DbTest.ID)).isEqualTo("001");
    }

    @Test
    public void order_by_desc() {
        List<Testobject> testobjects = db.withHandle(handle -> {
            getTestobjectWithId("001").setNumberOfPets(0).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("002").setNumberOfPets(5).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("003").setNumberOfPets(10).toInsertQuery(handle, TESTTABLE1).execute();

            return Testobject.getSelectQuery(handle, TESTTABLE1)
                    .orderBy(OrderByExpression.desc("NUMBER_OF_PETS"))
                    .executeToList();
        });

        assertThat(testobjects).hasSize(3);
        assertThat(testobjects.get(0).numberOfPets).isEqualTo(10);
        assertThat(testobjects.get(0).id).isEqualTo("003");
    }

    @Test
    public void order_by_multiple_desc_chained() {
        List<Testobject> testobjects = db.withHandle(handle -> {
            getTestobjectWithId("001").setNumberOfPets(10).setBirthday(new Timestamp(0)).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("002").setNumberOfPets(0).setBirthday(new Timestamp(20)).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("003").setNumberOfPets(10).setBirthday(new Timestamp(10)).toInsertQuery(handle, TESTTABLE1).execute();

            return Testobject.getSelectQuery(handle, TESTTABLE1)
                    .orderBy(OrderByExpression.desc("NUMBER_OF_PETS"))
                    .orderBy(OrderByExpression.desc("BIRTHDAY"))
                    .executeToList();
        });

        assertThat(testobjects).hasSize(3);
        assertThat(testobjects.get(0).id).isEqualTo("003");
        assertThat(testobjects.get(0).numberOfPets).isEqualTo(10);
        assertThat(testobjects.get(0).birthday).isEqualTo(new Timestamp(10));
        assertThat(testobjects.get(1).id).isEqualTo("001");
        assertThat(testobjects.get(1).numberOfPets).isEqualTo(10);
        assertThat(testobjects.get(1).birthday).isEqualTo(new Timestamp(0));
    }

    @Test
    public void order_by_asc() {
        final List<Testobject> testobjects = db.withHandle(handle -> {
            getTestobjectWithId("001").setNumberOfPets(10).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("002").setNumberOfPets(5).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("003").setNumberOfPets(0).toInsertQuery(handle, TESTTABLE1).execute();

            return Testobject.getSelectQuery(handle, TESTTABLE1)
                    .orderBy(OrderByExpression.asc("NUMBER_OF_PETS"))
                    .executeToList();
        });

        assertThat(testobjects).hasSize(3);
        assertThat(testobjects.get(0).numberOfPets).isEqualTo(0);
        assertThat(testobjects.get(0).id).isEqualTo("003");
    }

    @Test
    public void order_by_multiple_asc_chained() {
        final List<Testobject> testobjects = db.withHandle(handle -> {
            getTestobjectWithId("001").setNumberOfPets(0).setBirthday(new Timestamp(20)).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("002").setNumberOfPets(10).setBirthday(new Timestamp(0)).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("003").setNumberOfPets(0).setBirthday(new Timestamp(10)).toInsertQuery(handle, TESTTABLE1).execute();

            return Testobject.getSelectQuery(handle, TESTTABLE1)
                    .orderBy(OrderByExpression.asc("NUMBER_OF_PETS"))
                    .orderBy(OrderByExpression.asc("BIRTHDAY"))
                    .executeToList();
        });

        assertThat(testobjects).hasSize(3);
        assertThat(testobjects.get(0).id).isEqualTo("003");
        assertThat(testobjects.get(0).numberOfPets).isEqualTo(0);
        assertThat(testobjects.get(0).birthday).isEqualTo(new Timestamp(10));
        assertThat(testobjects.get(1).id).isEqualTo("001");
        assertThat(testobjects.get(1).numberOfPets).isEqualTo(0);
        assertThat(testobjects.get(1).birthday).isEqualTo(new Timestamp(20));
    }

    @Test
    public void order_byMultiple_asc_and_desc_chained() {
        final List<Testobject> testobjects = db.withHandle(handle -> {
            getTestobjectWithId("001").setNumberOfPets(0).setBirthday(new Timestamp(10)).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("002").setNumberOfPets(10).setBirthday(new Timestamp(0)).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("003").setNumberOfPets(0).setBirthday(new Timestamp(20)).toInsertQuery(handle, TESTTABLE1).execute();

            return Testobject.getSelectQuery(handle, TESTTABLE1)
                    .orderBy(OrderByExpression.asc("NUMBER_OF_PETS"))
                    .orderBy(OrderByExpression.desc("BIRTHDAY"))
                    .executeToList();
        });

        assertThat(testobjects).hasSize(3);
        assertThat(testobjects.get(0).id).isEqualTo("003");
        assertThat(testobjects.get(0).numberOfPets).isEqualTo(0);
        assertThat(testobjects.get(0).birthday).isEqualTo(new Timestamp(20));
        assertThat(testobjects.get(1).id).isEqualTo("001");
        assertThat(testobjects.get(1).numberOfPets).isEqualTo(0);
        assertThat(testobjects.get(1).birthday).isEqualTo(new Timestamp(10));
    }

    @Test
    public void order_and_where() {
        List<Testobject> testobjects = db.withHandle(handle -> {
            getTestobjectWithId("001").setDead(true).setNumberOfPets(0).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("002").setDead(true).setNumberOfPets(5).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("003").setDead(true).setNumberOfPets(10).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("004").setNumberOfPets(20).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("005").setNumberOfPets(25).toInsertQuery(handle, TESTTABLE1).execute();

            return Testobject.getSelectQuery(handle, TESTTABLE1)
                    .where(WhereClause.equals("DEAD", true))
                    .orderBy(OrderByExpression.desc("NUMBER_OF_PETS"))
                    .executeToList();
        });

        assertThat(testobjects).hasSize(3);
        assertThat(testobjects.get(0).numberOfPets).isEqualTo(10);
        assertThat(testobjects.get(0).id).isEqualTo("003");
    }

    @Test
    public void where_is_not_null() {
        Tuple2<List<Testobject>, List<Testobject>> result = db.withHandle(handle -> {
            getTestobjectWithId("007").setBirthday(null).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("006").toInsertQuery(handle, TESTTABLE1).execute();

            List<Testobject> birthdayNotNull = Testobject.getSelectQuery(handle, TESTTABLE1).where(WhereClause.isNotNull(BIRTHDAY)).executeToList();
            List<Testobject> birthdayNull = Testobject.getSelectQuery(handle, TESTTABLE1).where(WhereClause.isNull(BIRTHDAY)).executeToList();
            return Tuple.of(birthdayNotNull, birthdayNull);
        });

        assertThat(result._1).hasSize(1);
        assertThat(result._2).hasSize(1);

        assertThat(result._1.get(0).getBirthday()).isNotNull();
        assertThat(result._2.get(0).getBirthday()).isNull();
    }

    @Test
    public void where_is_allways_true() {
        List<Testobject> retrieved = db.withHandle(handle -> {
            getTestobjectWithId("003").setDead(true).setNumberOfPets(2).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("004").setDead(true).setNumberOfPets(3).toInsertQuery(handle, TESTTABLE1).execute();

            return Testobject
                    .getSelectQuery(handle, TESTTABLE1)
                    .where(WhereClause.alwaysTrue())
                    .executeToList();
        });

        assertThat(retrieved).hasSize(2);
    }

    @Test
    public void where_is_allways_false() {
        List<Testobject> retrieved = db.withHandle(handle -> {
            getTestobjectWithId("003").setDead(true).setNumberOfPets(2).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("004").setDead(true).setNumberOfPets(3).toInsertQuery(handle, TESTTABLE1).execute();

            return Testobject
                    .getSelectQuery(handle, TESTTABLE1)
                    .where(WhereClause.alwaysFalse())
                    .executeToList();
        });

        assertThat(retrieved).hasSize(0);
    }

    @Test
    public void wherelike_skal_stotte_wildcards(){
        List<Testobject> retrieved = db.withHandle(handle -> {
            getTestobjectWithId("003").setDead(true).setNumberOfPets(2).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("004").setDead(true).setNumberOfPets(3).toInsertQuery(handle, TESTTABLE1).execute();

            return Testobject
                    .getSelectQuery(handle, TESTTABLE1)
                    .where(WhereClause.like("id", "%0_"))
                    .executeToList();
        });

        assertThat(retrieved).hasSize(2);
    }

    @Test
    public void wherelike_skal_ikke_stotte_sqlinjections(){
        List<Testobject> retrieved = db.withHandle(handle -> {
            getTestobjectWithId("003").setDead(true).setNumberOfPets(2).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("004").setDead(true).setNumberOfPets(3).toInsertQuery(handle, TESTTABLE1).execute();

            return Testobject
                    .getSelectQuery(handle, TESTTABLE1)
                    .where(WhereClause.like("id", "%0_'; DROP TABLE TESTTABLE1; '"))
                    .executeToList();
        });

        assertThat(retrieved).hasSize(0);
    }



    @Test
    public void limit() {
        List<Testobject> testobjects = db.withHandle(handle -> {
            getTestobjectWithId("003").setDead(true).setNumberOfPets(2).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("004").setDead(true).setNumberOfPets(3).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("009").setDead(true).setNumberOfPets(8).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("007").setDead(true).setNumberOfPets(6).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("002").setDead(true).setNumberOfPets(1).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("008").setDead(true).setNumberOfPets(7).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("001").setDead(true).setNumberOfPets(0).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("006").setDead(true).setNumberOfPets(5).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("005").setDead(true).setNumberOfPets(4).toInsertQuery(handle, TESTTABLE1).execute();

            return Testobject.getSelectQuery(handle, TESTTABLE1)
                    .orderBy(OrderByExpression.asc(NUMBER_OF_PETS))
                    .limit(5)
                    .executeToList();
        });

        assertThat(testobjects.stream()
                .map(Testobject::getNumberOfPets).collect(Collectors.toList())).isEqualTo(asList(0, 1, 2, 3, 4));
    }

    @Test
    public void limit_with_offset() {
        List<Testobject> testobjects = db.withHandle(handle -> {
            getTestobjectWithId("003").setDead(true).setNumberOfPets(2).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("004").setDead(true).setNumberOfPets(3).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("009").setDead(true).setNumberOfPets(8).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("007").setDead(true).setNumberOfPets(6).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("002").setDead(true).setNumberOfPets(1).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("008").setDead(true).setNumberOfPets(7).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("001").setDead(true).setNumberOfPets(0).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("006").setDead(true).setNumberOfPets(5).toInsertQuery(handle, TESTTABLE1).execute();
            getTestobjectWithId("005").setDead(true).setNumberOfPets(4).toInsertQuery(handle, TESTTABLE1).execute();

            return Testobject.getSelectQuery(handle, TESTTABLE1)
                    .orderBy(OrderByExpression.asc(NUMBER_OF_PETS))
                    .limit(2, 5)
                    .executeToList();
        });

        assertThat(testobjects.stream()
                .map(Testobject::getNumberOfPets).collect(Collectors.toList())).isEqualTo(asList(2, 3, 4, 5, 6));
    }

    @Test
    public void where_comparativ_test() {
        List<Testobject> objects = new ArrayList<>();
        objects.add(getTestobjectWithId("001"));
        objects.add(getTestobjectWithId("002"));
        objects.add(getTestobjectWithId("003"));
        objects.add(getTestobjectWithId("004"));

        db.withHandle(handle -> {
            Testobject.getInsertBatchQuery(handle, TESTTABLE1).execute(objects);

            int greaterThenTwo = Testobject.getSelectQuery(handle, TESTTABLE1)
                    .where(WhereClause.gt("ID", "002"))
                    .executeToList()
                    .size();

            int greaterThenOrEqualTwo = Testobject.getSelectQuery(handle, TESTTABLE1)
                    .where(WhereClause.gteq("ID", "002"))
                    .executeToList()
                    .size();

            int lessThenTwo = Testobject.getSelectQuery(handle, TESTTABLE1)
                    .where(WhereClause.lt("ID", "002"))
                    .executeToList()
                    .size();

            int lessThenOrEqualTwo = Testobject.getSelectQuery(handle, TESTTABLE1)
                    .where(WhereClause.lteq("ID", "002"))
                    .executeToList()
                    .size();

            assertThat(greaterThenTwo).isEqualTo(2);
            assertThat(greaterThenOrEqualTwo).isEqualTo(3);
            assertThat(lessThenTwo).isEqualTo(1);
            assertThat(lessThenOrEqualTwo).isEqualTo(2);
            return true;
        });
    }

    @Test
    public void group_by_test() {
        List<Testobject> objects = new ArrayList<>();
        objects.add(getTestobjectWithId("001"));
        objects.add(getTestobjectWithId("002"));
        objects.add(getTestobjectWithId("003"));
        objects.add(getTestobjectWithId("004"));
        objects.get(1).dead = true;
        objects.get(3).dead = true;

        Map<Boolean, Integer> grouped = db.withHandle(handle -> {
            Testobject.getInsertBatchQuery(handle, TESTTABLE1).execute(objects);

            return SqlUtils.select(handle, TESTTABLE1, (resultset) -> {
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
        }).get();

        assertThat(grouped.get(true)).isEqualTo(2);
        assertThat(grouped.get(false)).isEqualTo(2);
    }

    @Test
    public void insert_current_timestamp() {
        Testobject object = db.withHandle(handle -> {
            SqlUtils.insert(handle, TESTTABLE1)
                    .value(ID, "001")
                    .value(NAVN, "navn")
                    .value(BIRTHDAY, DbConstants.CURRENT_TIMESTAMP)
                    .execute();

            return Testobject.getSelectQuery(handle, TESTTABLE1).execute();
        }).get();

        assertThat(object.getBirthday()).isNotNull();
    }

    @Test
    public void update_current_timestamp() {
        Timestamp epoch0 = new Timestamp(0);
        Testobject object = db.withHandle(handle -> {
            SqlUtils.insert(handle, TESTTABLE1)
                    .value(ID, "001")
                    .value(NAVN, "navn")
                    .value(BIRTHDAY, epoch0)
                    .execute();

            SqlUtils.update(handle, TESTTABLE1)
                    .set(BIRTHDAY, DbConstants.CURRENT_TIMESTAMP)
                    .whereEquals(ID, "001")
                    .execute();

            return Testobject.getSelectQuery(handle, TESTTABLE1).execute();
        }).get();

        assertThat(object.getBirthday()).isAfter(new Timestamp(0));
    }

    @Test
    public void batch_insert_with_current_timestamp() {
        List<Testobject> retrievedObjects = db.withHandle(handle -> {
            InsertBatchQuery<Testobject> insertBatchQuery = new InsertBatchQuery<>(handle, TESTTABLE1);
            insertBatchQuery
                    .add(ID, Testobject::getId)
                    .add(NAVN, Testobject::getNavn)
                    .add(BIRTHDAY, DbConstants.CURRENT_TIMESTAMP);

            List<Testobject> objects = new ArrayList<>();
            objects.add(getTestobjectWithId("001"));
            objects.add(getTestobjectWithId("002"));

            insertBatchQuery.execute(objects);

            return Testobject.getSelectQuery(handle, TESTTABLE1).executeToList();
        });

        assertThat(retrievedObjects.get(0)).isNotNull();
        assertThat(retrievedObjects.get(1)).isNotNull();
    }

    @Test
    public void batch_update_with_current_timestamp() {
        List<Testobject> retrievedObjects = db.withHandle(handle -> {
            InsertBatchQuery<Testobject> insertBatchQuery = new InsertBatchQuery<>(handle, TESTTABLE1);
            insertBatchQuery
                    .add(ID, Testobject::getId)
                    .add(NAVN, Testobject::getNavn)
                    .add(BIRTHDAY, Testobject::getBirthday);

            List<Testobject> objects = new ArrayList<>();
            objects.add(getTestobjectWithId("001"));
            objects.add(getTestobjectWithId("002"));

            insertBatchQuery.execute(objects);

            UpdateBatchQuery<Testobject> updateBatchQuery = new UpdateBatchQuery<>(handle, TESTTABLE1);
            updateBatchQuery.add(BIRTHDAY, DbConstants.CURRENT_TIMESTAMP);

            List<Testobject> updateobjects = new ArrayList<>();
            updateobjects.add(getTestobjectWithId("001"));
            updateobjects.add(getTestobjectWithId("002"));

            updateBatchQuery.execute(updateobjects);

            return Testobject.getSelectQuery(handle, TESTTABLE1).executeToList();
        });

        assertThat(retrievedObjects.get(0).getBirthday()).isAfter(new Timestamp(0));
        assertThat(retrievedObjects.get(1).getBirthday()).isAfter(new Timestamp(0));
    }

    @Test
    public void skal_wrappe_sprorringer_i_try() {
        Try<Void> noReturn = Try.of(() -> {
            SqlUtils.useHandle(db, handle -> { });
            return null;
        });
        Try<Void> noReturnWithError = Try.of(() -> {
            SqlUtils.useHandle(db, handle -> {
                SqlUtils.select(handle, "NOTABLE", (rs) -> "").execute();
            });
            return null;
        });

        assertThat(noReturn.isSuccess()).isEqualTo(true);
        assertThat(noReturnWithError.isSuccess()).isEqualTo(false);
        assertThat(noReturnWithError.getCause()).isExactlyInstanceOf(SqlUtilsException.class);
    }

    @Test
    public void skal_mappe_optional_til_try() {
        Try<Testobject> not_found = Try.of(() -> SqlUtils.withHandle(db, handle -> Testobject
                .getSelectQuery(handle, TESTTABLE1)
                .execute()
                .getOrElseThrow(() -> new RuntimeException("Not found"))
        ));

        assertThat(not_found.isFailure()).isTrue();
    }

    @Test
    public void skal_gjennomfore_alle_sql_sporringer() {
        Testobject object1 = getTestobjectWithId("001");
        Testobject object2 = getTestobjectWithId("002");
        Testobject object3 = getTestobjectWithId("003");

        List<Testobject> transactional = SqlUtils.transactional(() -> {
            lagInsertHandler(db, object1);
            lagInsertHandler(db, object2);
            lagInsertHandler(db, object3);
            return SqlUtils.withHandle(db, (handle) -> Testobject.getSelectQuery(handle, TESTTABLE1).executeToList());
        });

        List<Testobject> testobjects = db.withHandle(handle -> Testobject.getSelectQuery(handle, TESTTABLE1)
                .executeToList());

        assertThat(transactional).hasSize(3);
        assertThat(testobjects).hasSize(3);
    }

    @Test
    public void skal_gjor_rollback_om_det_kastes_exception() {
        Testobject object1 = getTestobjectWithId("001");
        Testobject object2 = getTestobjectWithId("002");

        Try<List<Testobject>> transactional = Try.of(() -> SqlUtils.transactional(() -> {
            lagInsertHandlerVoid(db, object1);
            lagInsertHandlerVoid(db, object2);
            throw new RuntimeException("Det skjedde en feil her,");
        }));

        List<Testobject> testobjects = db.withHandle(handle -> Testobject.getSelectQuery(handle, TESTTABLE1)
                .executeToList());

        assertThat(testobjects).hasSize(0);
    }

    @Test
    public void skal_gjor_rollback_om_en_try_kommer_fra_withHandle_som_kaster_feil() {
        Testobject object1 = getTestobjectWithId("001");
        Testobject object2 = getTestobjectWithId("002");

        Try<List<Testobject>> transactional = Try.of(() -> SqlUtils.transactional(() -> {
            lagInsertHandlerVoid(db, object1);
            lagInsertHandlerVoid(db, object2);

            SqlUtils.withHandle(db, (handle) -> {
                throw new RuntimeException("Det skjedde en feil her,");
            });

            return null;
        }));

        List<Testobject> testobjects = db.withHandle(handle -> Testobject.getSelectQuery(handle, TESTTABLE1)
                .executeToList());

        assertThat(testobjects).hasSize(0);
    }

    @Test
    public void skal_gjor_rollback_om_en_try_kommer_fra_useHandle_som_kaster_feil() {
        Testobject object1 = getTestobjectWithId("001");
        Testobject object2 = getTestobjectWithId("002");

        Try.of(() -> SqlUtils.transactional(() -> {
            lagInsertHandlerVoid(db, object1);
            lagInsertHandlerVoid(db, object2);

            SqlUtils.useHandle(db, (handle) -> {
                throw new RuntimeException("Det skjedde en feil her,");
            });

            return null;
        }));

        List<Testobject> testobjects = db.withHandle(handle -> Testobject.getSelectQuery(handle, TESTTABLE1)
                .executeToList());

        assertThat(testobjects).hasSize(0);
    }

    @Test
    public void skal_gjenbruke_transaksjon_ved_nostede_transaksjoner() {
        Testobject object1 = getTestobjectWithId("001");
        Testobject object2 = getTestobjectWithId("002");
        Testobject object3 = getTestobjectWithId("003");
        AtomicReference<Handle> handle1 = new AtomicReference<>();
        AtomicReference<Handle> handle2 = new AtomicReference<>();
        AtomicReference<Handle> handle3 = new AtomicReference<>();

        Integer transactional = SqlUtils.transactional(() -> {
            SqlUtils.useHandle(db, (handle) -> { handle1.set(handle); insert(object1, handle); });
            SqlUtils.transactional(() -> SqlUtils.useHandle(db, (handle) -> { handle2.set(handle); insert(object2, handle); }));
            return SqlUtils.transactional(() ->
                    SqlUtils.transactional(() ->
                            SqlUtils.transactional(() ->
                                    SqlUtils.withHandle(db, (handle) -> { handle3.set(handle); return insert(object3, handle); })
                            )
                    )
            );
        });

        List<Testobject> testobjects = db.withHandle(handle -> Testobject.getSelectQuery(handle, TESTTABLE1)
                .executeToList());

        assertThat(testobjects).hasSize(3);
        assertThat(handle1.get()).isNotNull();
        assertThat(handle2.get()).isNotNull();
        assertThat(handle3.get()).isNotNull();
        assertThat(handle1.get() == handle2.get()).isTrue();
        assertThat(handle1.get() == handle3.get()).isTrue();
    }

    @Test
    public void feil_i_nosted_transaksjon_skal_propagere_til_toppen() {
        Try<Integer> transaction = Try.of(() -> SqlUtils.transactional(() ->
                SqlUtils.transactional(() ->
                        SqlUtils.transactional(() -> {
                            throw new RuntimeException("Det skjedde en feil");
                        })
                )
        ));

        assertThat(transaction.isFailure()).isTrue();
        assertThat(transaction.getCause()).isExactlyInstanceOf(RuntimeException.class);
        assertThat(transaction.getCause()).hasMessage("Det skjedde en feil");
    }

    @Test
    public void exception_blir_propagert_opp_gjennom_transactions() {
        Try<Integer> transaction = Try.of(() -> SqlUtils.transactional(() -> {
            SqlUtils.transactional(() ->
                    SqlUtils.transactional(() -> SqlUtils.useHandle(db, (handle) -> {
                        throw new RuntimeException("Det skjedde en feil her,");
                    }))
            );
            lagInsertHandlerVoid(db, getTestobjectWithId("001"));

            return 1;
        }));

        assertThat(transaction.isFailure()).isTrue();

        List<Testobject> testobjects = db.withHandle(handle -> Testobject.getSelectQuery(handle, TESTTABLE1)
                .executeToList());
        assertThat(testobjects).hasSize(0);
    }

    @Test
    public void skal_stotte_nostede_transaksjoner() {
        DummyRepository repo = new DummyRepository(db);
        Testobject object1 = getTestobjectWithId("001");
        Testobject object2 = getTestobjectWithId("002");
        Testobject object3 = getTestobjectWithId("003");
        Testobject object4 = getTestobjectWithId("004");

        List<Testobject> result = SqlUtils.transactional(() -> {
            Integer res1 = repo.insert(object1);
            Integer res2 = SqlUtils.transactional(() -> repo.insert(object2));
            Integer res3 = repo.insert(object3);
            Integer res4 = SqlUtils.transactional(() -> repo.insert(object4));

            return repo.getAll();
        });

        List<Testobject> testobjects = db.withHandle(handle -> Testobject.getSelectQuery(handle, TESTTABLE1)
                .executeToList());

        assertThat(testobjects).hasSize(4);
    }

    private Integer lagInsertHandler(Jdbi jdbi, Testobject object) {
        return SqlUtils.withHandle(jdbi, (handle) -> insert(object, handle));
    }

    private int insert(Testobject object, Handle handle) {
        return SqlUtils.insert(handle, TESTTABLE1)
                .value(ID, object.getId())
                .value(NAVN, object.getNavn())
                .value(DEAD, object.isDead())
                .value(BIRTHDAY, object.getBirthday())
                .value(NUMBER_OF_PETS, object.getNumberOfPets())
                .execute();
    }

    private void lagInsertHandlerVoid(Jdbi jdbi, Testobject object) {
        SqlUtils.useHandle(jdbi, (handle) -> insert(object, handle));
    }


    private Testobject getTestobjectWithId(String id) {
        return new Testobject()
                .setNavn("navn navnesen")
                .setId(id)
                .setBirthday(new Timestamp(0))
                .setNumberOfPets(4)
                .setDead(false);
    }

    class DummyRepository {
        Jdbi jdbi;

        public DummyRepository(Jdbi jdbi) {
            this.jdbi = jdbi;
        }

        Integer insert(Testobject testobject) {
            return SqlUtils.withHandle(jdbi, (handle) -> testobject
                    .toInsertQuery(handle, TESTTABLE1)
                    .execute()
            );
        }

        List<Testobject> getAll() {
            return SqlUtils.withHandle(jdbi, (handle) -> Testobject.getSelectQuery(handle, TESTTABLE1).executeToList());
        }
    }
}

