import com.datastax.driver.core.*;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

import static config.Configuration.*;

/**
 * @author anton
 *         Date: 3/9/16
 */
public class CassandraDao implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(CassandraDao.class);

    private final Cluster cluster;
    private final Session session;

    private final Set<UUID> uuids = Sets.newHashSet();
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSS");

    public CassandraDao(String node, int port) {
        cluster = Cluster.builder().addContactPoint(node).withPort(port).withProtocolVersion(ProtocolVersion.V2).build();
        session = cluster.connect();
        init();
    }

    public CassandraDao(String node) {
        this(node, 9042);
    }

    private void init() {
        execute(DROP);
        execute(CREATE);
        execute(USE);
        execute(CREATE_TABLE);
    }

    public void write(boolean withTTL) {
        if (withTTL) {
            execute(String.format(WRITE_WITH_TTL, "'wttl'", new Date().getTime(), 777l, 444, Long.MAX_VALUE, TTL));
        } else {
            execute(String.format(WRITE_WO_TTL, "'wottl'", new Date().getTime(), 777l, 444, Long.MAX_VALUE));
        }
    }

    public void readThrough(Map<Long, String> trace) throws InterruptedException {
        ResultSet resultSet = execute(session.prepare(SELECT).bind().enableTracing());
        QueryTrace queryTrace = resultSet.getExecutionInfo().getQueryTrace();

        List<QueryTrace.Event> events = queryTrace.getEvents();
        for (QueryTrace.Event event : events) {
            String desc = event.getDescription();
            if (desc.contains("tombstoned") && !" 0 ".equals(desc.substring(desc.indexOf("tombstoned") - 3, desc.indexOf("tombstoned")))) {
                trace.put(System.currentTimeMillis(), desc);
            }
        }
    }

    public void showCompactionHistory(Map<Long, String> trace) {
        ResultSet rs = execute(COMP_HISTORY);

        for (Row row : rs.all()) {
            String keyspace = row.getString("keyspace_name");
            UUID id = row.getUUID("id");
            if (!uuids.contains(id) && KEY_SPACE.equalsIgnoreCase(keyspace)) {
                Date compacted = row.getDate("compacted_at");
                trace.put(compacted.getTime(), "keyspace: " + keyspace + ";   compacted_at: " + format.format(compacted));
                uuids.add(id);
            }
        }
    }

    private ResultSet execute(String query) {
        ResultSet resultSet = session.execute(query);
        logger.debug("isFullyFetched: {}; query: {}", resultSet.isFullyFetched(), query);
        return resultSet;
    }

    private ResultSet execute(Statement statement) {
        ResultSet resultSet = session.execute(statement);
        logger.debug("isFullyFetched: {}; statement: {}", resultSet.isFullyFetched(), statement);
        return resultSet;
    }

    public void close() throws Exception {
        execute(DROP);

        session.close();
        cluster.close();
    }
}