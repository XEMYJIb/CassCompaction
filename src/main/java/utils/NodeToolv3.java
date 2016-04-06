package utils;

import org.apache.cassandra.tools.NodeProbe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Anton on 3/10/16.
 */
public class NodeToolv3 implements NodeTool {

    private static final Logger logger = LoggerFactory.getLogger(NodeToolv3.class);

    private final NodeProbe nodeProbe;

    public NodeToolv3(String node, int port) throws IOException {
        nodeProbe = new NodeProbe(node, port);
    }

    public int sstCount(Map<Long, String> trace) {
        /*Integer count = (Integer) nodeProbe.getColumnFamilyMetric(KEY_SPACE.toLowerCase(), TABLE_NAME.toLowerCase(), "LiveSSTableCount");
        if(count != null) {
            trace.put(System.currentTimeMillis(), "SSTable count: " + count);
        } else {
            logger.error("No SSTables");
        }*/

        return 0;
    }

    public void close() throws Exception {
        nodeProbe.close();
    }
}
