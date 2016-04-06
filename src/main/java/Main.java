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
        //todo:console arguments
        SortedMap<Long, String> trace = Maps.newTreeMap();
        CassandraDao cassandraDao = null;
        NodeTool nodeTool = null;

        try {
            printStep("INIT");
            cassandraDao = new CassandraDao(host);
            nodeTool = new NodeToolv1(host, jmxPort);
            SSTable2Json ssTable2Json = args.length == 1 ? new SSTable2Json(args[0]) : new SSTable2Json();
            cassandraDao.showCompactionHistory(trace);

            addToTrace(trace, "1");
            ssTable2Json.exportSST2Json(trace);
            cassandraDao.readThrough(trace);

            addToTrace(trace, "2");
            //todo:optimize write block
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
            //todo:optimize write block
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
            //todo:check directory
            close(nodeTool, cassandraDao);
        }

        printStep("EXIT");

        //todo:optimize exit
        //from main method of tools
        System.exit(0);
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
