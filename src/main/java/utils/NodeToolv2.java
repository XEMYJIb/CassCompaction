package utils;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.metrics.ColumnFamilyMetrics;

import java.util.Map;

import static config.Configuration.*;

/**
 * Created by Anton on 3/10/16.
 */
public class NodeToolv2 implements NodeTool {

    public int sstCount(Map<Long, String> trace) {
        Config.setClientMode(false);
        ColumnFamilyStore columnFamilyStore = ColumnFamilyStore.createColumnFamilyStore(Keyspace.open(TABLE_NAME.toLowerCase()), KEY_SPACE.toLowerCase(), false);
        ColumnFamilyMetrics familyMetrics = new ColumnFamilyMetrics(columnFamilyStore);
        trace.put(System.currentTimeMillis(), "SSTable count: " + familyMetrics.liveSSTableCount.value());
        return familyMetrics.liveSSTableCount.value();
    }

    public void close() throws Exception {
    }
}
