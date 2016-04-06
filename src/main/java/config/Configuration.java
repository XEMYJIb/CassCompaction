package config;

/**
 * Created by Anton on 3/25/16.
 */
public interface Configuration {

    int TTL = 30;

    String KEY_SPACE = "comp_v1";
    String TABLE_NAME = "testEvents";

    String DROP = "DROP KEYSPACE IF EXISTS " + KEY_SPACE + ";";
    String CREATE = "CREATE KEYSPACE " + KEY_SPACE + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    String USE = "use " + KEY_SPACE + ";";

    String CREATE_TABLE = "CREATE TABLE " + TABLE_NAME + " (\n" +
            "    eve text,\n" +
            "    eTime timestamp,\n" +
            "    longN bigint,\n" +
            "    intN int,\n" +
            "    load bigint,\n" +
            "    PRIMARY KEY((eve), eTime, longN)\n" +
            ") WITH compaction = {'class': 'DateTieredCompactionStrategy'} AND gc_grace_seconds = 100;";

    String WRITE_WITH_TTL = "INSERT INTO " + KEY_SPACE + "." + TABLE_NAME + "\n" +
            "(eve, eTime, longN, intN, load)\n" +
            "VALUES ( %s, '%s', %s , %s, %s)\n" +
            " USING TTL %d;";

    String WRITE_WO_TTL = "INSERT INTO " + KEY_SPACE + "." + TABLE_NAME + "\n" +
            "(eve, eTime, longN, intN, load)\n" +
            "VALUES ( %s, '%s', %s , %s, %s);";

    String COMP_HISTORY = "SELECT * FROM system.compaction_history;";
    String SELECT = "SELECT * FROM " + KEY_SPACE + "." + TABLE_NAME + ";";
}
