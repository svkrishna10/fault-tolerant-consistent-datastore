package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspace (because we are pretending different
 * replicas are like different keyspaces), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

    /**
     * Set this value to as small a value with which you can get tests to still
     * pass. The lower it is, the faster your implementation is. Grader* will
     * use this value provided it is no greater than its MAX_SLEEP limit.
     * Faster
     * is not necessarily better, so don't sweat speed. Focus on safety.
     */
    public static final int SLEEP = 1000;

    // User-defined fields
    protected Cluster cluster;
    protected Session session;
    protected static final Logger log = Logger.getLogger(MyDBReplicableAppGP.class.getName());
    protected String myID;    // Keyspace name
    protected String table = "grade";   // Single table assumed for this assignment

    /**
     * All Gigapaxos apps must either support a no-args constructor or a
     * constructor taking a String[] as the only argument. Gigapaxos relies on
     * adherence to this policy in order to be able to reflectively construct
     * customer application instances.
     *
     * @param args Singleton array whose args[0] specifies the keyspace in the
     * backend data store to which this server must connect.
     * Optional args[1] and args[2]
     * @throws IOException
     */
    public MyDBReplicableAppGP(String[] args) throws IOException {
        // TODO: setup connection to the data store and keyspace
        if (args == null || args.length == 0) {
            throw new IOException("No keyspace provided in args[0]");
        }

        // Connect to Cassandra cluster and keyspace
        // Note: Hardcoded to localhost as per assignment requirements
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect(args[0]);

        myID = args[0];
        
        // Log initialization info
        for (int i = 0; i < args.length; i++) {
            log.log(Level.INFO, "Startup arg {0}: {1}", new Object[]{i, args[i]});
        }
        log.log(Level.INFO, "Gigapaxos replicable initialized with keyspace: {0}", new Object[]{myID});
        System.out.println("Replicable Giga Paxos started with keyspace " + myID);
    }

    /**
     * Refer documentation of {@link Replicable#execute(Request, boolean)} to
     * understand what the boolean flag means.
     * <p>
     * You can assume that all requests will be of type {@link
     * edu.umass.cs.gigapaxos.paxospackets.RequestPacket}.
     *
     * @param request
     * @param b
     * @return
     */
    @Override
    public boolean execute(Request request, boolean b) {
        // TODO: submit request to data store
        // 'b' is skipClientResponse
        boolean skipClientResponse = b; 

        synchronized (this) {
            try {
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
     * Refer documentation of
     * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
     *
     * @param request
     * @return
     */
    @Override
    public boolean execute(Request request) {
        // TODO: execute the request by sending it to the data store
        log.log(Level.WARNING, "Default execute called; using skipClientResponse=true for request={0}", request);
        return execute(request, true);
    }

    /**
     * Refer documentation of {@link Replicable#checkpoint(String)}.
     *
     * @param s
     * @return
     */
    @Override
    public String checkpoint(String s) {
        // TODO:
        String checkpointName = s;
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
     * Refer documentation of {@link Replicable#restore(String, String)}
     *
     * @param s
     * @param s1
     * @return
     */
    @Override
    public boolean restore(String s, String s1) {
        // TODO:
        String checkpointName = s;
        String state = s1;
        
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
                    // Reconstruct the update query based on the serialized checkpoint format
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


    /**
     * No request types other than {@link edu.umass.cs.gigapaxos.paxospackets
     * .RequestPacket will be used by Grader, so you don't need to implement
     * this method.}
     *
     * @param s
     * @return
     * @throws RequestParseException
     */
    @Override
    public Request getRequest(String s) throws RequestParseException {
        return null;
    }

    /**
     * @return Return all integer packet types used by this application. For an
     * example of how to define your own IntegerPacketType enum, refer {@link
     * edu.umass.cs.reconfiguration.examples.AppRequest}. This method does not
     * need to be implemented because the assignment Grader will only use
     * {@link
     * edu.umass.cs.gigapaxos.paxospackets.RequestPacket} packets.
     */
    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>();
    }
}