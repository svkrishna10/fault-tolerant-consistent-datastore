package server.faulttolerance;

import com.datastax.driver.core.*;
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.ReplicatedServer;
import server.MyDBSingleServer;
import org.apache.zookeeper.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MyDBFaultTolerantServerZK extends MyDBSingleServer implements Watcher {

    public static final int SLEEP = 100;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    public static final int MAX_LOG_SIZE = 400;
    public static final int DEFAULT_PORT = 2181;
    private static final int CHECKPOINT_INTERVAL = 200; // Checkpoint every 200 requests

    private final ZooKeeper zk;
    private final String electionPath = "/election";
    private final String logPath = "/log";
    private final String checkpointPath = "/checkpoint";
    private final String myID;
    
    protected final MessageNIOTransport<String, String> serverMessenger;
    private final Session session;
    private final Cluster cluster;

    private String myZnodeId;
    private volatile boolean isLeader = false;
    private volatile String currentLeader = null;
    
    private int lastExecutedSeq = -1;
    private int lastCheckpointedSeq = -1;

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID), 
                nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET), 
              isaDB, 
              myID);
        
        this.myID = myID;

        this.serverMessenger = new MessageNIOTransport<String, String>(myID, nodeConfig, 
            new AbstractBytePacketDemultiplexer() {
                @Override
                public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                    handleMessageFromServer(bytes, nioHeader);
                    return true;
                }
            }, true);

        this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        this.session = cluster.connect(myID);

        this.zk = new ZooKeeper("localhost:" + DEFAULT_PORT, 3000, this);

        initializeZK();
        restoreFromCheckpoint(); // CRITICAL: Restore before processing new requests
        runLeaderElection();
        processLog();
        
        Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Server {0} started.", myID);
    }

    private void initializeZK() {
        try {
            if (zk.exists(electionPath, false) == null) {
                try {
                    zk.create(electionPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {}
            }
            if (zk.exists(logPath, false) == null) {
                try {
                    zk.create(logPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {}
            }
            if (zk.exists(checkpointPath, false) == null) {
                try {
                    zk.create(checkpointPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {}
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private synchronized void runLeaderElection() {
        try {
            if (myZnodeId == null) {
                myZnodeId = zk.create(electionPath + "/n_", myID.getBytes(), 
                                      ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            }

            List<String> children = zk.getChildren(electionPath, false);
            Collections.sort(children);

            String leaderNode = children.get(0);
            
            if (myZnodeId.endsWith(leaderNode)) {
                this.isLeader = true;
                this.currentLeader = myID;
            } else {
                this.isLeader = false;
                byte[] data = zk.getData(electionPath + "/" + leaderNode, false, null);
                this.currentLeader = new String(data);
                
                int myIndex = -1;
                for(int i=0; i<children.size(); i++) {
                    if(myZnodeId.endsWith(children.get(i))) {
                        myIndex = i; break;
                    }
                }
                if (myIndex > 0) {
                    zk.exists(electionPath + "/" + children.get(myIndex - 1), this);
                } else {
                    zk.exists(electionPath + "/" + leaderNode, this);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String request = new String(bytes);
        String sender = (header.sndr != null) ? header.sndr.toString() : "unknown";

        if (isLeader) {
            try {
                String payload = sender + "::" + request;
                zk.create(logPath + "/entry-", payload.getBytes(), 
                          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            if (currentLeader != null && !currentLeader.equals(myID)) {
                try {
                    this.serverMessenger.send(currentLeader, bytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        handleMessageFromClient(bytes, header);
    }

    private synchronized void processLog() {
        try {
            List<String> logs = zk.getChildren(logPath, this);
            if (logs.isEmpty()) return;
            
            Collections.sort(logs);

            for (String node : logs) {
                int seq = Integer.parseInt(node.substring(node.indexOf("-") + 1));

                // Only process logs AFTER our last executed sequence
                if (seq > lastExecutedSeq) {
                    byte[] data = zk.getData(logPath + "/" + node, false, null);
                    String payload = new String(data);
                    
                    String sender = null;
                    String cmd = payload;
                    if (payload.contains("::")) {
                        String[] parts = payload.split("::", 2);
                        sender = parts[0];
                        cmd = parts[1];
                    }

                    executeAndReply(cmd, sender, seq);
                    lastExecutedSeq = seq;
                    
                    // Checkpoint periodically (only if leader)
                    if (isLeader && (lastExecutedSeq - lastCheckpointedSeq) >= CHECKPOINT_INTERVAL) {
                        createCheckpoint();
                    }
                }
            }
            
            // Prune old logs after processing (only if leader)
            if (isLeader) {
                pruneLogs();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void executeAndReply(String cmd, String sender, long seq) {
        try {
            String query = cmd;
            JSONObject jsonReq = null;
            try {
                jsonReq = new JSONObject(cmd);
                if (jsonReq.has("REQUEST")) {
                    query = jsonReq.getString("REQUEST");
                }
            } catch (JSONException e) {}

            session.execute(query);

            String responseStr = "[success:" + cmd + "]";
            if (jsonReq != null) {
                jsonReq.put("RESPONSE", responseStr);
                responseStr = jsonReq.toString();
            }

            if (sender != null && !sender.equals("unknown")) {
                this.serverMessenger.send(sender, responseStr.getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createCheckpoint() {
        try {
            // Snapshot the entire table state at this moment
            ResultSet rs = session.execute("SELECT * FROM " + myID + ".grade");
            StringBuilder snapshot = new StringBuilder();
            snapshot.append(lastExecutedSeq).append("|");
            
            boolean first = true;
            for (Row row : rs) {
                int id = row.getInt("id");
                List<Integer> events = row.getList("events", Integer.class);
                
                if (!first) snapshot.append(";");
                first = false;
                
                snapshot.append(id).append(":");
                for (int i = 0; i < events.size(); i++) {
                    snapshot.append(events.get(i));
                    if (i < events.size() - 1) snapshot.append(",");
                }
            }
            
            // Store checkpoint in ZK with the sequence number in the name
            String cpNode = checkpointPath + "/cp-" + String.format("%010d", lastExecutedSeq);
            try {
                zk.create(cpNode, snapshot.toString().getBytes(), 
                         ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                lastCheckpointedSeq = lastExecutedSeq;
                
                Logger.getLogger(this.getClass().getName()).log(Level.INFO, 
                    "Created checkpoint at seq " + lastExecutedSeq + " with data: " + snapshot.toString());
            } catch (KeeperException.NodeExistsException e) {
                // Checkpoint already exists for this sequence
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void restoreFromCheckpoint() {
        try {
            List<String> checkpoints = zk.getChildren(checkpointPath, false);
            if (checkpoints.isEmpty()) {
                lastExecutedSeq = -1;
                lastCheckpointedSeq = -1;
                Logger.getLogger(this.getClass().getName()).log(Level.INFO, 
                    "No checkpoints found, starting from scratch");
                return;
            }
            
            Collections.sort(checkpoints);
            String latestCp = checkpoints.get(checkpoints.size() - 1);
            
            byte[] data = zk.getData(checkpointPath + "/" + latestCp, false, null);
            String snapshot = new String(data);
            
            Logger.getLogger(this.getClass().getName()).log(Level.INFO, 
                "Restoring from checkpoint: " + latestCp + " with data: " + snapshot);
            
            if (snapshot.isEmpty()) {
                lastExecutedSeq = -1;
                lastCheckpointedSeq = -1;
                return;
            }
            
            String[] parts = snapshot.split("\\|", 2);
            int checkpointSeq = Integer.parseInt(parts[0]);
            
            // Clear table first - start fresh from checkpoint
            session.execute("TRUNCATE " + myID + ".grade");
            
            if (parts.length > 1 && !parts[1].isEmpty()) {
                // Restore each row from checkpoint
                String[] rows = parts[1].split(";");
                for (String row : rows) {
                    if (row.trim().isEmpty()) continue;
                    
                    String[] keyVal = row.split(":", 2);
                    if (keyVal.length < 1) continue;
                    
                    int id = Integer.parseInt(keyVal[0]);
                    
                    if (keyVal.length > 1 && !keyVal[1].isEmpty()) {
                        // Has events
                        session.execute("INSERT INTO " + myID + ".grade (id, events) VALUES (" 
                                       + id + ", [" + keyVal[1] + "])");
                    } else {
                        // Empty events list
                        session.execute("INSERT INTO " + myID + ".grade (id, events) VALUES (" 
                                       + id + ", [])");
                    }
                }
            }
            
            // Set these AFTER successfully restoring data
            lastExecutedSeq = checkpointSeq;
            lastCheckpointedSeq = checkpointSeq;
            
            Logger.getLogger(this.getClass().getName()).log(Level.INFO, 
                "Successfully restored from checkpoint at seq " + checkpointSeq);
            
        } catch (Exception e) {
            // If restore fails, start from scratch
            lastExecutedSeq = -1;
            lastCheckpointedSeq = -1;
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, 
                "Failed to restore from checkpoint, starting from scratch", e);
        }
    }

    private void pruneLogs() {
        try {
            List<String> logs = zk.getChildren(logPath, false);
            if (logs.isEmpty()) return;
            
            Collections.sort(logs);
            
            // Calculate how many we can safely delete
            // Keep at least MAX_LOG_SIZE entries, and never delete anything >= lastCheckpointedSeq
            int canDelete = Math.max(0, logs.size() - MAX_LOG_SIZE);
            
            for (int i = 0; i < canDelete; i++) {
                String log = logs.get(i);
                int seq = Integer.parseInt(log.substring(log.indexOf("-") + 1));
                
                // Only delete if it's older than our checkpoint
                if (seq < lastCheckpointedSeq) {
                    try {
                        zk.delete(logPath + "/" + log, -1);
                    } catch (Exception e) {
                        // Ignore - might have been deleted by another server
                    }
                }
            }
        } catch (Exception e) {
            // Ignore exceptions during pruning
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            if (event.getPath().startsWith(logPath)) {
                processLog();
            }
        } else if (event.getType() == Event.EventType.NodeDeleted) {
            if (event.getPath().startsWith(electionPath)) {
                runLeaderElection();
            }
        }
    }

    @Override
    public void close() {
        super.close();
        if (serverMessenger != null) serverMessenger.stop();
        if (session != null) session.close();
        if (cluster != null) cluster.close();
        try {
            if (zk != null) zk.close();
        } catch (Exception e) {}
    }

    public static void main(String[] args) throws IOException {
        new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile
                (args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer
                        .SERVER_PORT_OFFSET), args[1], args.length > 2 ? Util
                .getInetSocketAddressFromString(args[2]) : new
                InetSocketAddress("localhost", 9042));
    }
}