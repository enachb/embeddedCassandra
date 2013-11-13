package com;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import junit.framework.TestCase;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.log4j.BasicConfigurator;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static junit.framework.Assert.assertEquals;

/**
 * Sample on how to spin up an embedded Cassandra instance
 * using in-memory storage.
 */
public class CassandraTest{

    static CassandraDaemon cass;
    static Cluster cluster;

    @BeforeClass
    public static void setup() throws IOException {
        // get log4j crackin'
        BasicConfigurator.configure();

        // clean Cass dirs to be stateless
        Set<String> dirs = new HashSet<String>();
        dirs.addAll(Arrays.asList(DatabaseDescriptor.getAllDataFileLocations()));
        dirs.addAll(Arrays.asList(DatabaseDescriptor.getSavedCachesLocation()));
        dirs.addAll(Arrays.asList(DatabaseDescriptor.getCommitLogLocation()));
        for (String s :  dirs) {
            dirs.add(s);
            FileUtils.deleteRecursive(new File(s));
            FileUtils.createDirectory(s);
        }

        // init Cassandra
        cass = new CassandraDaemon();
        cass.init(null);
        cass.start();

        cluster = Cluster.builder().addContactPoint("localhost").build();

    }

    @AfterClass
    public static void shutdown() {
        //Shutting down everything in an orderly fashion
        cluster.shutdown();
        cass.stop();
    }

    @Test
    public void testBasic() throws IOException, InterruptedException, TTransportException {

        Session session = cluster.connect("system");

        List<Row> rows = session.execute("select * from system.local").all();
        assertEquals("Found rows", 1, rows.size());
        for (Row row : rows) {
            System.out.println(row);
        }

        session.shutdown();

    }


}
