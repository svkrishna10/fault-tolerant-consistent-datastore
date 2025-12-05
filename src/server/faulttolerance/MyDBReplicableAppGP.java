package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * GigaPaxos implementation of the Replicated Database.
 * GigaPaxos handles Consensus. We handle Execution and State Transfer.
 */
public class MyDBReplicableAppGP implements Replicable {

    public static final int SLEEP = 1000;
    private static final String TABLE_NAME = "grade";
    
    private final String myID;
    private final Cluster cluster;
    private final Session session; // Connected to the DEFAULT keyspace for flexible querying
    private static final Logger log = Logger.getLogger(MyDBReplicableAppGP.class.getName());

    /**
     * Constructor required by GigaPaxos.
     * @param args args[0] is the Server ID (used as keyspace name).
     */
    public MyDBReplicableAppGP(String[] args) throws IOException {
        this.myID = args[0];
        
        String host = "localhost";
        int port = 9042;
        
        this.cluster = Cluster.builder()
                .addContactPoint(host)
                .withPort(port)
                .build();
        
        // Connect to this server's keyspace
        this.session = cluster.connect(myID);
        
        log.info("GP App initialized for server: " + myID);
    }

    /**
     * Execute a committed request against Cassandra.
     */
    @Override
    public boolean execute(Request request) {
        if (request instanceof RequestPacket) {
            String reqValue = ((RequestPacket) request).getRequestValue();
            return executeQuery(reqValue);
        }
        return false;
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        return execute(request);
    }

    /**
     * Parse and execute the SQL command.
     */
    private boolean executeQuery(String reqValue) {
        String command = reqValue;
        
        // Try to parse as JSON first
        try {
            JSONObject json = new JSONObject(reqValue);
            if (json.has("REQUEST")) {
                command = json.getString("REQUEST");
            }
        } catch (JSONException e) {
            // Not JSON, use as-is
        }

        // Execute against our keyspace
        try {
            session.execute(command);
            return true;
        } catch (Exception e) {
            log.log(Level.SEVERE, myID + " failed to execute: " + command, e);
            return false;
        }
    }

    /**
     * CHECKPOINT: Serialize the entire table state to a String.
     * 
     * CRITICAL: The parameter 'serviceName' is the GigaPaxos service name,
     * NOT the server ID. However, for this implementation, we checkpoint
     * OUR keyspace (myID), since that's where our data lives.
     */
    @Override
    public String checkpoint(String serviceName) {
        try {
            // Read all rows from our table
            ResultSet results = session.execute("SELECT * FROM " + myID + "." + TABLE_NAME);
            
            JSONArray rows = new JSONArray();
            
            for (Row row : results) {
                JSONObject jsonRow = new JSONObject();
                jsonRow.put("id", row.getInt("id"));
                jsonRow.put("events", row.getList("events", Integer.class));
                rows.put(jsonRow);
            }
            
            String checkpoint = rows.toString();
            log.info(myID + " created checkpoint with " + rows.length() + " rows");
            return checkpoint;
            
        } catch (Exception e) {
            log.log(Level.SEVERE, myID + " failed to create checkpoint", e);
            return "";
        }
    }

    /**
     * RESTORE: Clear and restore table state from a checkpoint string.
     * 
     * CRITICAL: serviceName is the GigaPaxos service name. We restore
     * to OUR keyspace (myID).
     */
    @Override
    public boolean restore(String serviceName, String state) {
        try {
            if (state == null || state.isEmpty()) {
                log.info(myID + " restore called with empty state");
                return true;
            }

            // Clear current state
            session.execute("TRUNCATE " + myID + "." + TABLE_NAME);

            // Parse checkpoint
            JSONArray rows = new JSONArray(state);

            // Restore each row
            for (int i = 0; i < rows.length(); i++) {
                JSONObject jsonRow = rows.getJSONObject(i);
                int id = jsonRow.getInt("id");
                JSONArray events = jsonRow.getJSONArray("events");
                
                // Build the events list string
                StringBuilder eventsStr = new StringBuilder("[");
                for (int j = 0; j < events.length(); j++) {
                    eventsStr.append(events.getInt(j));
                    if (j < events.length() - 1) eventsStr.append(",");
                }
                eventsStr.append("]");

                // Insert
                String insert = String.format("INSERT INTO %s.%s (id, events) VALUES (%d, %s)", 
                        myID, TABLE_NAME, id, eventsStr.toString());
                session.execute(insert);
            }
            
            log.info(myID + " restored " + rows.length() + " rows from checkpoint");
            return true;
            
        } catch (Exception e) {
            log.log(Level.SEVERE, myID + " failed to restore from checkpoint", e);
            return false;
        }
    }

    /**
     * Parse a string into a Request object (if needed by GigaPaxos).
     */
    @Override
    public Request getRequest(String s) {
        // GigaPaxos typically handles this internally
        // Return a RequestPacket if you need to support string->Request conversion
        try {
            return new RequestPacket(s, false);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Declare what packet types this app handles.
     */
    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<>();
    }
}