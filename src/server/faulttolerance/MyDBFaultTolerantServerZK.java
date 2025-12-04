package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import server.MyDBSingleServer;
import server.ReplicatedServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Robust Fault-Tolerant Server using ZooKeeper.
 * Includes Retry Logic to prevent dropped requests during startup/crashes.
 */
public class MyDBFaultTolerantServerZK extends MyDBSingleServer implements Watcher {

    // --- Configuration ---
    public static final int SLEEP = 1000;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    public static final int MAX_LOG_SIZE = 400;
    private static final String ZK_HOST = "localhost:2181";

    // --- ZK Paths ---
    private static final String ROOT_PATH = "/ops";
    private static final String LOG_PATH = ROOT_PATH + "/log";
    private static final String STATUS_PATH = ROOT_PATH + "/status";
    private static final String LOG_PREFIX = "request-";
    private static final String CHECKPOINT_TABLE = "checkpoints";

    // --- Components ---
    protected static final Logger log = Logger.getLogger(MyDBFaultTolerantServerZK.class.getName());
    private final String myID;
    private final ZooKeeper zk;
    private final Session session;
    private final Cluster cluster;

    // --- State ---
    private long nextReqId = 0;
    private final ConcurrentHashMap<Long, NIOHeader> pendingClientRequests = new ConcurrentHashMap<>();
    
    // Lock for wait/notify logic
    private final Object lock = new Object();
    private volatile boolean running = true;

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET), isaDB, myID);
        this.myID = myID;

        // 1. Connect Cassandra
        cluster = Cluster.builder().addContactPoint(isaDB.getHostName()).withPort(isaDB.getPort()).build();
        session = cluster.connect(myID);
        ensureCheckpointTable();

        // 2. Connect ZK
        zk = new ZooKeeper(ZK_HOST, 3000, this);

        // 3. Setup Paths (blocks until ZK is ready)
        setupZNodes();

        // 4. Recover State
        recoverState();

        // 5. Start Engine
        new Thread(this::processRequestsLoop).start();
    }

    private void ensureCheckpointTable() {
        try {
            session.execute("CREATE TABLE IF NOT EXISTS " + CHECKPOINT_TABLE + " (replica text PRIMARY KEY, state text)");
        } catch (Exception e) { e.printStackTrace(); }
    }

    /**
     * Blocks until ZK is connected and paths are created.
     */
    private void setupZNodes() {
        boolean ready = false;
        while (!ready && running) {
            try {
                ensurePathExists(ROOT_PATH);
                ensurePathExists(LOG_PATH);
                ensurePathExists(STATUS_PATH);
                ready = true;
            } catch (Exception e) {
                // ZK not ready? Sleep and retry.
                try { Thread.sleep(100); } catch (InterruptedException ignored) {}
            }
        }
    }

    private void ensurePathExists(String path) throws KeeperException, InterruptedException {
        if (zk.exists(path, false) == null) {
            try {
                zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ignored) {}
        }
    }

    private void recoverState() {
        try {
            String myStatusPath = STATUS_PATH + "/" + myID;
            if (zk.exists(myStatusPath, false) != null) {
                byte[] data = zk.getData(myStatusPath, false, null);
                long lastExecuted = Long.parseLong(new String(data, StandardCharsets.UTF_8));
                
                // RESTORE CHECKPOINT FIRST
                loadCheckpoint();
                
                this.nextReqId = lastExecuted + 1;
                log.info(myID + " recovered. Next ID: " + nextReqId);
            } else {
                zk.create(myStatusPath, "-1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                this.nextReqId = getMinLogIdFromZK();
                loadCheckpoint(); // Just in case
                log.info(myID + " fresh start. Next ID: " + nextReqId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private long getMinLogIdFromZK() {
        try {
            List<String> children = zk.getChildren(LOG_PATH, false);
            if (children.isEmpty()) return 0;
            List<Long> ids = new ArrayList<>();
            for (String c : children) {
                try { ids.add(Long.parseLong(c.substring(LOG_PREFIX.length()))); } catch(NumberFormatException ignored){}
            }
            if (ids.isEmpty()) return 0;
            return Collections.min(ids);
        } catch (Exception e) { return 0; }
    }

    /**
     * FIXED: Includes retry logic to handle ZK disconnects without dropping requests.
     */
    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String request = new String(bytes, StandardCharsets.UTF_8);
        int retries = 10; 
        
        while (retries-- > 0 && running) {
            try {
                String path = zk.create(LOG_PATH + "/" + LOG_PREFIX,
                        request.getBytes(StandardCharsets.UTF_8),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL);

                String seqStr = path.substring(path.lastIndexOf(LOG_PREFIX) + LOG_PREFIX.length());
                long seqId = Long.parseLong(seqStr);
                
                pendingClientRequests.put(seqId, header);
                synchronized (lock) { lock.notifyAll(); }
                return; // Success!

            } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e) {
                // ZK is blinking. Sleep briefly and retry.
                try { Thread.sleep(200); } catch (InterruptedException ignored) {}
            } catch (Exception e) {
                e.printStackTrace();
                break; // Fatal error, give up
            }
        }
        log.severe(myID + " FAILED to propose request after retries: " + request);
    }

    private void processRequestsLoop() {
        while (running) {
            try {
                String expectedPath = LOG_PATH + "/" + LOG_PREFIX + String.format("%010d", nextReqId);
                Stat stat = zk.exists(expectedPath, this);

                if (stat != null) {
                    byte[] data = zk.getData(expectedPath, false, null);
                    executeRequestAndNotify(nextReqId, new String(data, StandardCharsets.UTF_8));
                    
                    updateCheckpoint(nextReqId);
                    
                    if (nextReqId > 0 && nextReqId % MAX_LOG_SIZE == 0) {
                        createCheckpoint(nextReqId);
                    }
                    if (nextReqId % 10 == 0) garbageCollectLogs();
                    
                    nextReqId++;
                } else {
                    long minLog = getMinLogIdFromZK();
                    if (minLog > nextReqId) {
                        log.warning(myID + " detected gap. Jumping " + nextReqId + " -> " + minLog);
                        nextReqId = minLog;
                        continue;
                    }
                    synchronized (lock) { lock.wait(1000); }
                }
            } catch (Exception e) {
                try { Thread.sleep(100); } catch (InterruptedException ignored) {}
            }
        }
    }

    private void executeRequestAndNotify(long reqId, String payload) throws JSONException {
        String command = payload;
        JSONObject jsonReq = null;
        try {
            jsonReq = new JSONObject(payload);
            if(jsonReq.has("REQUEST")) command = jsonReq.getString("REQUEST");
        } catch (JSONException ignored) {
            command = payload;
        }

        String responseMsg = "Executed";
        try {
            if (session != null && !session.isClosed()) session.execute(command);
        } catch (Exception e) {
            responseMsg = "Error";
            e.printStackTrace();
        }

        NIOHeader clientHeader = pendingClientRequests.remove(reqId);
        if (clientHeader != null) {
            try {
                byte[] respBytes;
                if (jsonReq != null) {
                    JSONObject resp = new JSONObject();
                    resp.put("RESPONSE", responseMsg);
                    resp.put("REQUEST", command);
                    respBytes = resp.toString().getBytes(StandardCharsets.UTF_8);
                } else {
                    respBytes = responseMsg.getBytes(StandardCharsets.UTF_8);
                }
                this.clientMessenger.send(clientHeader.sndr, respBytes);
            } catch (IOException e) { e.printStackTrace(); }
        }
    }

    private void updateCheckpoint(long executedId) {
        try {
            zk.setData(STATUS_PATH + "/" + myID, Long.toString(executedId).getBytes(), -1);
        } catch (Exception ignored) {}
    }

    private void createCheckpoint(long checkpointReqId) {
        try {
            JSONObject snapshot = new JSONObject();
            ResultSet tablesRs = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '" + myID + "'");
            for (Row tr : tablesRs) {
                String tableName = tr.getString("table_name");
                if (CHECKPOINT_TABLE.equalsIgnoreCase(tableName)) continue;

                ResultSet rs = session.execute("SELECT * FROM " + tableName);
                JSONArray rowsArray = new JSONArray();
                for (Row r : rs) {
                    JSONObject rowJson = new JSONObject();
                    for (ColumnDefinitions.Definition def : r.getColumnDefinitions()) {
                        Object val = r.getObject(def.getName());
                        rowJson.put(def.getName(), val == null ? JSONObject.NULL : val.toString());
                    }
                    rowsArray.put(rowJson);
                }
                snapshot.put(tableName, rowsArray);
            }

            JSONObject checkpointObj = new JSONObject();
            checkpointObj.put("checkpointReqId", checkpointReqId);
            checkpointObj.put("snapshot", snapshot);

            String stateString = checkpointObj.toString().replace("'", "''");
            session.execute("INSERT INTO " + CHECKPOINT_TABLE + " (replica, state) VALUES ('" + myID + "', '" + stateString + "')");
            
            log.info(myID + " created checkpoint at " + checkpointReqId);
        } catch (Exception e) { e.printStackTrace(); }
    }

    private void loadCheckpoint() {
        try {
            ResultSet rs = session.execute("SELECT state FROM " + CHECKPOINT_TABLE + " WHERE replica = '" + myID + "'");
            Row r = rs.one();
            if (r == null) return;

            JSONObject checkpointObj = new JSONObject(r.getString("state"));
            JSONObject snapshot = checkpointObj.optJSONObject("snapshot");
            if (snapshot == null) return;

            Iterator<String> keys = snapshot.keys();
            while (keys.hasNext()) {
                String tableName = keys.next();
                try { session.execute("TRUNCATE " + tableName); } catch(Exception ignored){}
                
                JSONArray rows = snapshot.getJSONArray(tableName);
                for (int i = 0; i < rows.length(); i++) {
                    String json = rows.getJSONObject(i).toString().replace("'", "''");
                    try { session.execute("INSERT INTO " + tableName + " JSON '" + json + "'"); } catch(Exception ignored){}
                }
            }
            log.info(myID + " loaded checkpoint.");
        } catch (Exception e) { e.printStackTrace(); }
    }

    private void garbageCollectLogs() {
        try {
            List<String> servers = zk.getChildren(STATUS_PATH, false);
            long minExecuted = Long.MAX_VALUE;
            for (String s : servers) {
                try {
                    long val = Long.parseLong(new String(zk.getData(STATUS_PATH + "/" + s, false, null)));
                    if (val < minExecuted) minExecuted = val;
                } catch (Exception ignored) {}
            }
            long threshold = minExecuted - MAX_LOG_SIZE;
            if (threshold < 0) return;

            for (String node : zk.getChildren(LOG_PATH, false)) {
                try {
                    if (Long.parseLong(node.substring(LOG_PREFIX.length())) < threshold) zk.delete(LOG_PATH + "/" + node, -1);
                } catch (Exception ignored) {}
            }
        } catch (Exception ignored) {}
    }

    @Override
    public void process(WatchedEvent event) {
        synchronized (lock) { lock.notifyAll(); }
    }

    // @Override
    // protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {}

    @Override
    public void close() {
        running = false;
        synchronized (lock) { lock.notifyAll(); }
        try {
            if (zk != null) zk.close();
            if (session != null) session.close();
            if (cluster != null) cluster.close();
        } catch (Exception ignored) {}
        super.close();
    }

    public static void main(String[] args) throws IOException {
        new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile
                (args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer
                        .SERVER_PORT_OFFSET), args[1], args.length > 2 ? Util
                .getInetSocketAddressFromString(args[2]) : new
                InetSocketAddress("localhost", 9042));
    }
}