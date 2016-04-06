package utils;

import java.util.Map;

/**
 * Created by Anton on 3/25/16.
 */
public interface NodeTool extends AutoCloseable {

    int sstCount(Map<Long, String> trace);
}
