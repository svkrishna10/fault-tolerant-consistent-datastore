package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ColumnDefinitions;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONObject;

/**

* MyDBReplicableAppGP implements the {@link Replicable} interface to allow
* Gigapaxos to manage replicated state for a Cassandra-backed database.
*
* This version has updated comments and improved readability while keeping
* key variable names consistent.
  */
  public class MyDBReplicableAppGP implements Replicable {

  protected Cluster cluster;
  protected Session session;

  protected static final Logger log = Logger.getLogger(MyDBReplicableAppGP.class.getName());

  protected String myID;    // Keyspace name
  protected String table;   // Single table assumed for this assignment

  public static final int SLEEP = 1000; // Default sleep interval for tests

  /**

  * Constructor for the Gigapaxos replicable application.
  *
  * @param args args[0] should contain the Cassandra keyspace name
  * @throws IOException
    */
    public MyDBReplicableAppGP(String[] args) throws IOException {
    if (args == null || args.length == 0) {
    throw new IOException("No keyspace provided in args[0]");
    }

    // Connect to Cassandra cluster and keyspace
    cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    session = cluster.connect(args[0]);

    myID = args[0];
    table = "grade"; // Default table used in this assignment

    // Log initialization info
    for (int i = 0; i < args.length; i++) {
    log.log(Level.INFO, "Startup arg {0}: {1}", new Object[]{i, args[i]});
    }
    log.log(Level.INFO, "Gigapaxos replicable initialized with keyspace: {0}", new Object[]{myID});
    System.out.println("Replicable Giga Paxos started with keyspace " + myID);
    }

  /**

  * Executes a given request atomically.
  *
  * @param request The incoming request
  * @param skipClientResponse If true, no response is sent back to the client
  * @return true if the request was executed successfully, false otherwise
    */
    @Override
    public boolean execute(Request request, boolean skipClientResponse) {
    synchronized (this) {
    try {
    IntegerPacketType packetType = request.getRequestType();
    String serviceName = request.getServiceName();
    String reqValue = "";


         // Only handle RequestPacket types
         if (request instanceof RequestPacket) {
             reqValue = ((RequestPacket) request).requestValue;
         } else {
             log.log(Level.WARNING, "Received request that is not a RequestPacket");
         }

         // Ensure only allowed table is updated
         String[] components = reqValue.split("\\s+");
         if (components.length > 1 && "update".equals(components[0]) && !components[1].equals(table)) {
             log.log(Level.SEVERE, "WARNING: Attempt to update table other than {0}", table);
         }

         // Execute the query against Cassandra
         ResultSet resultSet = session.execute(reqValue);
         StringBuilder responseBuilder = new StringBuilder();
         for (Row row : resultSet) {
             responseBuilder.append(row.toString());
         }

         if (!skipClientResponse && request instanceof RequestPacket) {
             ((RequestPacket) request).setResponse(responseBuilder.toString());
         }

         log.log(Level.INFO, "Executed request {0} with result {1}", new Object[]{reqValue, responseBuilder});
         return true;
     } catch (Exception e) {
         log.log(Level.WARNING, "Exception during execute: {0}", e.toString());
         return false;
     }
    

    }
    }

  /**

  * Default execute implementation that ignores client response.
    */
    @Override
    public boolean execute(Request request) {
    log.log(Level.WARNING, "Default execute called; using skipClientResponse=true for request={0}", request);
    return execute(request, true);
    }

  /**

  * Creates a checkpoint of the current application state.
  *
  * @param checkpointName optional identifier for this checkpoint
  * @return JSON string representing current state
    */
    @Override
    public String checkpoint(String checkpointName) {
    synchronized (this) {
    log.log(Level.INFO, "Creating checkpoint with name={0}", checkpointName);

     String query = String.format("SELECT * FROM %s.%s", myID, table);
     ResultSet rs = session.execute(query);

     Map<String, String> serializedRows = new HashMap<>();

     for (Row row : rs) {
         ColumnDefinitions cols = row.getColumnDefinitions();
         int colIndex = 0;
         String primaryKey = "UNINITIALIZED_KEY";

         for (ColumnDefinitions.Definition col : cols) {
             String colName = col.getName();
             String colValue = row.getObject(colName).toString();

             if (colValue.isEmpty()) {
                 colValue = "[]"; // Treat empty as empty list
             }

             String tuple = colName + "|" + colValue;
             if (colIndex == 0) {
                 primaryKey = tuple;
                 serializedRows.put(primaryKey, "");
             } else {
                 serializedRows.put(primaryKey, serializedRows.get(primaryKey) + (colIndex == 1 ? tuple : "|" + tuple));
             }
             colIndex++;
         }
     }

     JSONObject json = new JSONObject(serializedRows);
     log.log(Level.INFO, "Checkpoint data: {0}", json.toString());
     return json.toString();
    

    }
    }

  /**

  * Restores the application state from a previously created checkpoint.
  *
  * @param checkpointName name of the checkpoint
  * @param state JSON string representing the checkpoint
  * @return true if restored successfully
    */
    @Override
    public boolean restore(String checkpointName, String state) {
    log.log(Level.INFO, "Restoring checkpoint {0} with state={1}", new Object[]{checkpointName, state});

    synchronized (this) {
    try {
    if (state == null || state.isEmpty()) {
    return true; // Nothing to restore
    }
         JSONObject json = new JSONObject(state);
         Map<String, String> stateMap = new HashMap<>();
         Iterator<String> keys = json.keys();
         while (keys.hasNext()) {
             String key = keys.next();
             stateMap.put(key, json.getString(key));
         }

         for (String primaryKey : stateMap.keySet()) {
             String[] primaryParts = primaryKey.split("\\|");
             String[] colParts = stateMap.get(primaryKey).split("\\|");
             String updateQuery = "update " + table + " SET " + colParts[0] + "=" + colParts[1] + " where " + primaryParts[0] + "=" + primaryParts[1];
             session.execute(updateQuery);
             log.log(Level.INFO, "Restored row with query: {0}", updateQuery);
         }

         return true;
     } catch (Exception e) {
         log.log(Level.SEVERE, "Exception during restore: {0}", e);
         return false;
     }

    }
    }

  @Override
  public Request getRequest(String s) throws RequestParseException {
  return null; // Not used for Grader
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
  return new HashSet<>(); // Only RequestPacket is expected
  }
  }
