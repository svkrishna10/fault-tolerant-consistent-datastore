package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**

* Corrected GigaPaxos implementation of the Replicated Database.
* Handles execution, checkpointing, and recovery robustly.
  */
  public class MyDBReplicableAppGP implements Replicable {

  private static final String TABLE_NAME = "grade";
  private static final Logger log = Logger.getLogger(MyDBReplicableAppGP.class.getName());

  private final String myID;
  private final Cluster cluster;
  private final Session session;

  public MyDBReplicableAppGP(String[] args) throws IOException {
  this.myID = args[0];

  
   // Cassandra defaults
   String host = "127.0.0.1";
   int port = 9042;

   this.cluster = Cluster.builder().addContactPoint(host).withPort(port).build();
   this.session = cluster.connect(myID);
   log.info("GP App initialized for keyspace: " + myID);
  

  }

  // --- Execute committed requests ---
  @Override
  public boolean execute(Request request) {
  if (!(request instanceof RequestPacket)) return false;
  String value = ((RequestPacket) request).getRequestValue();
  return executeQuery(value);
  }

  @Override
  public boolean execute(Request request, boolean doNotReplyToClient) {
  // Delegate to main execute
  return execute(request);
  }

  private boolean executeQuery(String value) {
  String command = value;

  
   // Robust JSON parsing
   try {
       JSONObject json = new JSONObject(value);
       if (json.has("REQUEST")) {
           command = json.getString("REQUEST");
       }
   } catch (JSONException ignored) {
       // raw SQL fallback
       command = value;
   }

   try {
       session.execute(command);
       return true;
   } catch (Exception e) {
       log.log(Level.SEVERE, myID + " failed to execute: " + command, e);
       return false;
   }


  }

  // --- Checkpoint the current database state ---
  @Override
  public String checkpoint(String s) {
  try {
  ResultSet results = session.execute("SELECT * FROM " + TABLE_NAME);
  JSONArray arr = new JSONArray();
  for (Row row : results) {
  JSONObject obj = new JSONObject();
  obj.put("id", row.getInt("id"));
  obj.put("events", row.getList("events", Integer.class));
  arr.put(obj);
  }
  return arr.toString();
  } catch (Exception e) {
  log.log(Level.SEVERE, "Checkpoint failed", e);
  return "[]";
  }
  }

  // --- Restore from a checkpoint string ---
  @Override
  public boolean restore(String serviceName, String state) {
  try {
  // Clear table first
  session.execute("TRUNCATE " + TABLE_NAME);

  
       if (state == null || state.trim().isEmpty() || state.trim().equals("[]")) return true;

       JSONArray rows;
       try {
           rows = new JSONArray(state);
       } catch (JSONException e) {
           log.warning("Restore failed to parse JSON state: " + state);
           return true;
       }

       for (int i = 0; i < rows.length(); i++) {
           JSONObject row = rows.getJSONObject(i);
           int id = row.getInt("id");
           JSONArray events = row.getJSONArray("events");

           // Build event list as string
           StringBuilder eventsStr = new StringBuilder("[");
           for (int j = 0; j < events.length(); j++) {
               eventsStr.append(events.getInt(j));
               if (j < events.length() - 1) eventsStr.append(",");
           }
           eventsStr.append("]");

           String insert = String.format(
                   "INSERT INTO %s (id, events) VALUES (%d, %s)", TABLE_NAME, id, eventsStr
           );
           session.execute(insert);
       }
       return true;
   } catch (Exception e) {
       log.log(Level.SEVERE, "Restore failed", e);
       return false;
   }
  

  }

  // --- Boilerplate ---
  @Override
  public Request getRequest(String s) throws RequestParseException {
  return null;
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
  return new HashSet<>();
  }
  }
