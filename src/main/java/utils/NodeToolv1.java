package utils;

import com.google.common.base.Strings;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.tools.NodeProbe;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static config.Configuration.*;

/**
 * Created by Anton on 3/15/16.
 */
public class NodeToolv1 implements NodeTool {

    private final NodeProbe nodeProbe;

    private int prvCount = 0;

    public NodeToolv1(String node, int port) throws IOException {
        nodeProbe = new NodeProbe(node, port);
    }

    public NodeToolv1(String node) throws IOException {
        nodeProbe = new NodeProbe(node);
    }

    public int sstCount(Map<Long, String> trace) {
        Iterator<Map.Entry<String, ColumnFamilyStoreMBean>> cfBeans = nodeProbe.getColumnFamilyStoreMBeanProxies();
        int count = prvCount;
        while (cfBeans.hasNext()) {
            Map.Entry<String, ColumnFamilyStoreMBean> entry = cfBeans.next();
            if (!Strings.isNullOrEmpty(KEY_SPACE) && KEY_SPACE.equalsIgnoreCase(entry.getKey())) {
                long time = System.currentTimeMillis();
                count = entry.getValue().getLiveSSTableCount();
                if (prvCount != count) {
                    trace.put(time, "SSTable count: " + count);
                    prvCount = count;
                }
            }
        }

        return count;
    }

    public void close() throws Exception {
        nodeProbe.close();
    }
}
