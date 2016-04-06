package utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.tools.SSTableExport;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;

import static config.Configuration.KEY_SPACE;
import static config.Configuration.TABLE_NAME;

/**
 * Created by Anton on 3/10/16.
 */
public class SSTable2Json {

    private static final Logger logger = LoggerFactory.getLogger(SSTable2Json.class);
    private static final String FOLDER_PATH = File.separator + KEY_SPACE.toLowerCase() + File.separator + TABLE_NAME.toLowerCase();

    public SSTable2Json() {
        this("file:/etc/dse/cassandra/cassandra.yaml");
    }

    public SSTable2Json(String yamlPath) {
        System.setProperty("cassandra.config", yamlPath);
        DatabaseDescriptor.loadSchemas(false);
    }

    public void exportSST2Json(Map<Long, String> trace) throws IOException, ConfigurationException, InterruptedException, JSONException {
        String[] sstDirectories = DatabaseDescriptor.getAllDataFileLocations();

        if (sstDirectories == null || sstDirectories.length == 0) {
            logger.warn("No SSTable directories");
            return;
        }

        List<String> files = scanSSTFiles(Sets.newHashSet(sstDirectories));

        for (String file : files) {
            Descriptor descriptor = Descriptor.fromFilename(file);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream stream = new PrintStream(baos);

            try {
                SSTableExport.export(descriptor, stream, new String[]{});
                String result = baos.toString();
                trace.put(System.currentTimeMillis(), String.format("JSON(in SSTables %s):", file));
                workWithJson(result, trace);
            } finally {
                stream.close();
            }
        }
    }

    private List<String> scanSSTFiles(Set<String> directories) {
        List<String> files = Lists.newArrayList();

        for (String directory : directories) {

            File folder = new File(directory + FOLDER_PATH);
            if (!folder.exists() || !folder.isDirectory()) {
                logger.warn("No SSTable directory");
                return Lists.newArrayListWithCapacity(0);
            }
            File[] listOfFiles = folder.listFiles();

            if (listOfFiles == null) {
                logger.warn("No SSTable files");
                return Lists.newArrayListWithCapacity(0);
            }

            for (File file : listOfFiles) {
                if (file.getName().startsWith(KEY_SPACE.toLowerCase())
                        && file.getName().endsWith("-Data.db")
                        && !file.getName().contains("-tmp-")) {
                    files.add(file.getAbsolutePath());
                }
            }
        }

        if (files.isEmpty()) {
            logger.info("No Data SSTables");
        }

        return files;
    }

    //The output is:{ROW_KEY:{[[COLUMN_NAME, COLUMN_VALUE, COLUMN_TIMESTAMP, IS_MARKED_FOR_DELETE],[COLUMN_NAME, ... ],...]},ROW_KEY:{...},...}
    //e-ttl, d-tombstones, t-tombstones, delete row.
    //todo:check row tombstones
    private void workWithJson(String ssTable, Map<Long, String> trace) throws JSONException {
        JSONArray array = new JSONArray(ssTable);

        if (array.length() < 1) {
            logger.error("No records in SSTable");
            return;
        }

        Map<String, Integer> result = new HashMap<String, Integer>();

        for (int i = 0; i < array.length(); i++) {
            JSONObject jsonObj = array.getJSONObject(i);
            JSONArray columnArray = jsonObj.getJSONArray("columns");
            for (int j = 0; j < columnArray.length(); j++) {
                JSONArray column = columnArray.getJSONArray(j);
                if (column.length() > 3) {
                    String value = column.get(3).toString();
                    Integer count = result.get(value);
                    if (count == null) {
                        result.put(value, 1);
                    } else {
                        result.put(value, ++count);
                    }
                }
            }
        }

        if (!result.isEmpty()) {
            trace.put(System.currentTimeMillis(), result.toString());
        } else {
            trace.put(System.currentTimeMillis(), "no tombstones");
        }
    }
}
