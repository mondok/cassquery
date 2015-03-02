package com.edentech.cassquery;


import com.datastax.driver.core.*;
import com.sun.org.apache.xpath.internal.functions.WrongNumberArgsException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class App
{
    public static void main( String[] args ) throws IOException, WrongNumberArgsException {
        String csvPath = "/path/to/csv";

        if (args.length == 1){
            csvPath = args[0];
        } else{
            throw new WrongNumberArgsException("csv file must be passed in as argument");
        }
        int maxConnections = 2;
        int concurrency = 25;
        PoolingOptions pools = new PoolingOptions();
        pools.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, concurrency);
        pools.setCoreConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        pools.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
        pools.setCoreConnectionsPerHost(HostDistance.REMOTE, maxConnections);
        pools.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnections);

        Cluster cluster = new Cluster.Builder()
                .addContactPoints("localhost")
                .withPoolingOptions(pools)
                .withSocketOptions(new SocketOptions().setTcpNoDelay(true))
                .build();


        Session session = cluster.connect("beers");


        Metadata metadata = cluster.getMetadata();
        System.out.println(String.format("Connected to cluster '%s' on %s.", metadata.getClusterName(), metadata.getAllHosts()));
        for(KeyspaceMetadata keyspace: metadata.getKeyspaces()){
            System.out.println(String.format("Keyspace is %s", keyspace.getName()));
        }

        session.execute("truncate beers.beernames;");
        File csvFile = new File(csvPath);
        CSVParser parser = CSVParser.parse(csvFile, Charset.defaultCharset(),CSVFormat.RFC4180);
        int counter = 0;
        for (CSVRecord csvRecord : parser) {
            String idField = csvRecord.get(0);
            String name = csvRecord.get(2);
            if (StringUtils.isNumeric(idField)){
                int id = Integer.parseInt(idField);
                name = name.replaceAll("'", "");
                String insertStatement = String.format("insert into beernames (beerid, name) values(%s, '%s');", id, name);
                session.execute(insertStatement);
            }
            System.out.println(counter);
            counter++;
        }


        ResultSet resultSet = session.execute("SELECT * from beernames");
        for(Row row: resultSet){
            String beerName = row.getString("name");
            if (beerName != null){
                System.out.println(beerName);
            }
        }

        session.close();
        cluster.close();
    }
}
