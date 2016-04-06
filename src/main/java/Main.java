import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.NodeTool;
import utils.NodeToolv1;
import utils.SSTable2Json;

import java.util.Map;
import java.util.SortedMap;

/**
 * @author anton
 *         Date: 3/9/16
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int jmxPort = 7299;
        String yaml = "file:/Users/Anton/Programs/dse-4.6.5/resources/cassandra/conf/cassandra.yaml";

        SortedMap<Long, String> trace = Maps.newTreeMap();
        CassandraDao cassandraDao = null;
        NodeTool nodeTool = null;

        try {
            printStep("INIT");
            cassandraDao = new CassandraDao(host);
            nodeTool = new NodeToolv1(host, jmxPort);
            SSTable2Json ssTable2Json = new SSTable2Json(yaml);
            cassandraDao.showCompactionHistory(trace);

            //init check
            addToTrace(trace, "1");
            ssTable2Json.exportSST2Json(trace);
            cassandraDao.readThrough(trace);

            addToTrace(trace, "2");
            //start write with ttl
            logger.info("Write with ttl ...");

            for (int i = 0; i < 3000; i++) {
                if (i > 0 && i % 500 == 0) {
                    logger.info("Writes: " + i);
                }
                cassandraDao.write(true);

                if (nodeTool.sstCount(trace) == 4) {
                    logger.info("iterations:" + i);
                    break;
                }
            }

            addToTrace(trace, "3");
            ssTable2Json.exportSST2Json(trace);
            cassandraDao.readThrough(trace);
            cassandraDao.showCompactionHistory(trace);

            addToTrace(trace, "4");
            logger.info("Write without ttl ...");
            for (int i = 0; i < 3000; i++) {
                if (i > 0 && i % 500 == 0) {
                    logger.info("Writes: " + i);
                }
                cassandraDao.write(false);

                cassandraDao.showCompactionHistory(trace);
                if (trace.get(trace.lastKey()).contains("compacted_at")) {
                    logger.info("iterations:" + i);
                    break;
                }
            }

            addToTrace(trace, "5");
            ssTable2Json.exportSST2Json(trace);
            cassandraDao.readThrough(trace);
            cassandraDao.showCompactionHistory(trace);
        } finally {
            print(trace);
            //check directory
            close(nodeTool, cassandraDao);
        }

        printStep("EXIT");
    }

    private static void print(Map<Long, String> trace) {
        printStep("TRACE");
        for (Map.Entry<Long, String> entry : trace.entrySet()) {
            System.out.println(entry.getValue());
        }
    }

    private static void close(AutoCloseable... autoCloseables) {
        printStep("CLOSE");
        for (AutoCloseable closeable : autoCloseables) {
            try {
                if (closeable != null) {
                    logger.debug(closeable.getClass().getName());
                    closeable.close();
                }
            } catch (Exception ex) {
                logger.error("Error on close", ex);
            }
        }
    }

    private static void printStep(String step) {
        System.out.println(String.format("---------------------------%s------------------------------", step));
    }

    private static void addToTrace(Map<Long, String> trace, String info) {
        trace.put(System.currentTimeMillis(), String.format("-%s-", info));
    }
}
