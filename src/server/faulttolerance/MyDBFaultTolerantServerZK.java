package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
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
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ZooKeeper Fault Tolerant Server.
 * Implements Leader Election and Shared Log consensus using ZooKeeper.
 */
public class MyDBFaultTolerantServerZK extends MyDBSingleServer implements Watcher {

    public static final int SLEEP = 100;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    public static final int MAX_LOG_SIZE = 400;
    public static final int DEFAULT_PORT = 2181;

    private final ZooKeeper zk;
    private final String electionPath = "/election";
    private final String logPath = "/log";
    private final String myID;
    
    // Server-to-Server Messenger (Required because MyDBSingleServer doesn't have one)
    protected final MessageNIOTransport<String, String> serverMessenger;

    // Explicit session management to ensure control
    private final Session session;
    private final Cluster cluster;

    private String myZnodeId;
    private volatile boolean isLeader = false;
    private volatile String currentLeader = null;
    
    // Track execution to ensure total ordering
    private int lastExecutedSeq = -1;

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB) throws IOException {
        // Fix 1: Correct super constructor call (ISA, ISA, String)
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID), 
                nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET), 
              isaDB, 
              myID);
        
        this.myID = myID;

        // Fix 2: Initialize serverMessenger for forwarding requests
        this.serverMessenger = new MessageNIOTransport<String, String>(myID, nodeConfig, 
            new AbstractBytePacketDemultiplexer() {
                @Override
                public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                    handleMessageFromServer(bytes, nioHeader);
                    return true;
                }
            }, true);

        // Establish DB Connection
        this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        this.session = cluster.connect(myID);

        // Connect to ZooKeeper
        this.zk = new ZooKeeper("localhost:" + DEFAULT_PORT, 3000, this);

        // Initialize ZK Structure
        initializeZK();

        // Start Leader Election
        runLeaderElection();

        // Start Log Processing (Recovery & Watching)
        processLog();
        
        Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Server {0} started ZK init.", myID);
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Leader Election: Smallest Ephemeral Sequential node wins.
     */
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
                // Get leader ID from data
                byte[] data = zk.getData(electionPath + "/" + leaderNode, false, null);
                this.currentLeader = new String(data);
                
                // Watch the node immediately preceding me (avoid herd effect)
                int myIndex = -1;
                for(int i=0; i<children.size(); i++) {
                    if(myZnodeId.endsWith(children.get(i))) {
                        myIndex = i; break;
                    }
                }
                if (myIndex > 0) {
                    zk.exists(electionPath + "/" + children.get(myIndex - 1), this);
                } else {
                    // Fallback watch leader
                    zk.exists(electionPath + "/" + leaderNode, this);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Handle Client Requests.
     * Logic: Wraps request with Sender ID and writes to ZK Log (if Leader) or Forwards to Leader.
     */
    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String request = new String(bytes);
        String sender = (header.sndr != null) ? header.sndr.toString() : "unknown";

        if (isLeader) {
            try {
                // Prune logs if needed
                pruneLogs();

                // Format: "SENDER_ID:REQUEST_DATA"
                // This allows us to reply to the correct node after consensus
                String payload = sender + "::" + request;
                
                zk.create(logPath + "/entry-", payload.getBytes(), 
                          ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // Forward to Leader
            if (currentLeader != null && !currentLeader.equals(myID)) {
                try {
                    this.serverMessenger.send(currentLeader, bytes);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // Fix 3: Removed @Override since parent MyDBSingleServer does not define this.
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        // Treat forwarded requests exactly like client requests
        handleMessageFromClient(bytes, header);
    }

    /**
     * Core Consensus Loop: Watches ZK Log and executes in order.
     */
    private synchronized void processLog() {
        try {
            List<String> logs = zk.getChildren(logPath, this); // Set watch
            Collections.sort(logs);

            for (String node : logs) {
                // node name format: entry-000000001
                int seq = Integer.parseInt(node.substring(node.indexOf("-") + 1));

                if (seq > lastExecutedSeq) {
                    byte[] data = zk.getData(logPath + "/" + node, false, null);
                    String payload = new String(data);
                    
                    // Parse "SENDER:REQUEST"
                    String sender = null;
                    String cmd = payload;
                    if (payload.contains("::")) {
                        String[] parts = payload.split("::", 2);
                        sender = parts[0];
                        cmd = parts[1];
                    }

                    // EXECUTE
                    executeAndReply(cmd, sender, seq);
                    
                    lastExecutedSeq = seq;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void executeAndReply(String cmd, String sender, long seq) {
        try {
            // Parse JSON if possible to extract SQL
            String query = cmd;
            JSONObject jsonReq = null;
            try {
                jsonReq = new JSONObject(cmd);
                if (jsonReq.has("REQUEST")) {
                    query = jsonReq.getString("REQUEST");
                }
            } catch (JSONException e) { /* Not JSON */ }

            // Execute on Cassandra
            session.execute(query);

            // Construct Response
            // Compatible with AVDBClient expectations
            String responseStr = "[success:" + cmd + "]";
            if (jsonReq != null) {
                jsonReq.put("RESPONSE", responseStr);
                responseStr = jsonReq.toString();
            }

            // Send Response back to sender
            // Only the node that sees the execution needs to reply.
            // If sender is "unknown" or ourselves, handling varies.
            if (sender != null && !sender.equals("unknown")) {
                this.serverMessenger.send(sender, responseStr.getBytes());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void pruneLogs() {
        try {
            List<String> logs = zk.getChildren(logPath, false);
            if (logs.size() > MAX_LOG_SIZE) {
                Collections.sort(logs);
                // Remove oldest, keep last MAX_LOG_SIZE
                for (int i = 0; i < logs.size() - MAX_LOG_SIZE; i++) {
                    zk.delete(logPath + "/" + logs.get(i), -1);
                }
            }
        } catch (Exception e) { /* Ignore race conditions */ }
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

    // Main entry point logic matches prompt requirements
    public static void main(String[] args) throws IOException {
        new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile
                (args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer
                        .SERVER_PORT_OFFSET), args[1], args.length > 2 ? Util
                .getInetSocketAddressFromString(args[2]) : new
                InetSocketAddress("localhost", 9042));
    }
}