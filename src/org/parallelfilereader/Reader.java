package org.parallelfilereader;

import com.google.common.base.Splitter;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;


public class Reader implements Runnable {
    private RandomAccessFile file;
    private ArrayList<Long> positionList;
    private ArrayList<String> out;
    private long size;
    private long pos;
    private long end;
    private Client client;
    private WebResource cypherResource ;
    private final String baseUri = "http://localhost:7474/db/data/cypher";
    private final String user = "neo4j";
    private final String password = "1234";
    private final static Splitter splitter = Splitter.on('\n');

    public Reader (String filePath,ArrayList<Long> positionList) throws IOException {
        file = new RandomAccessFile(filePath, "r");
        out = new ArrayList<String>();
        this.positionList = positionList;
    }

    @Override
    public void run() {
        client = Client.create();
        client.addFilter(new HTTPBasicAuthFilter(user, password));
        cypherResource = client.resource(baseUri);
        String index = "{\"query\":\"CREATE INDEX ON :Person(name)\"}";
        sendCypher(index);

    	try {
        pos = positionList.get(0);
	    	for (int i = 1; i < positionList.size(); i++) {
				end = positionList.get(i);
				size = end-pos+1;
				byte[] in = new byte[(int) size];
				file.seek(pos);
				file.read(in);
				String tmp = new String(in);
                uploadToNeo4j(tmp);
				//out.add(tmp);
				pos = end + 1;
                //Thread.currentThread().sleep(10);
	    	}
		} catch (IOException e) {
            e.printStackTrace();
        } finally {
        	System.out.println("Out Arraylist size : " + out.size());
		Main.executionTime();
		
        }
    }

    private void uploadToNeo4j(String edges) {

        //Thread.sleep(5000);
        StringBuilder sb = new StringBuilder("{\"query\":\"UNWIND [");
        int count = 0;
        Iterator<String> dataStreamIterator = splitter.split(edges).iterator();
        while (dataStreamIterator.hasNext()) {
            String item = dataStreamIterator.next().trim();
            if(item.length() == 0){
                continue;
            }
            //System.out.println("|"+item+"|");
            String[] nodes = item.split("\t");

            // sb.append("(n"+count+":Node { name : "+nodes[0]+"})-[:CONNECTED]->(m"+count+":Node { name : "+nodes[1]+"}),");
            sb.append("["+nodes[0]+","+nodes[1]+"], ");

            //if(count == 1000){
           // sb.setLength(sb.length() - 2);
           // sb.append("] AS pair MATCH (n:Person {name: pair[0]}), (m:Person {name: pair[1]}) CREATE (n)-[:X]->(m)\"}");
                //System.out.println(sb);
                //sendCypher(sb.toString());
                //sb = new StringBuilder("{\"query\":\"UNWIND [");
                count = -1;

                //long stopTime = System.currentTimeMillis();
                //long elapsedTime = (stopTime - startTime)/1000;
                //System.out.println("Time taken : "+elapsedTime+ " seconds.");
                //eturn;

            //}
            //count++;
        }

        sb.setLength(sb.length() - 2);
        sb.append("] AS pair MATCH (n:Person {name: pair[0]}), (m:Person {name: pair[1]}) CREATE (n)-[:X]->(m)\"}");
       // System.out.println(sb);
        sendCypher(sb.toString());
        //sb = new StringBuilder("{\"query\":\"UNWIND [");
    }

    public void sendCypher(String query) {
        ClientResponse cypherResponse = cypherResource.accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON_TYPE).entity(query).post(ClientResponse.class);

        System.out.println("Output from Server .... "+ cypherResponse.getStatus());

    }
}
