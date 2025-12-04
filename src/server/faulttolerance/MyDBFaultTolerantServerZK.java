package server.faulttolerance;

import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.ReplicatedServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import client.AVDBClient;

/**
 * Fault Tolerant Server using ZooKeeper for Atomic Broadcast.
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer implements Watcher {

    // --- Configuration ---
    public static final int SLEEP = 100;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    public static final int MAX_LOG_SIZE = 400;
    
    // ZK Paths
    private static final String ZK_ROOT = "/avdb";
    private static final String ZK_LOG_PATH = ZK_ROOT + "/log";
    private static final String ZK_LEADER_PATH = ZK_ROOT + "/leader";
    
    // Cassandra Metadata Table for Persistence
    private static final String META_TABLE = "server_metadata";

    // --- State ---
    private final String myID;
    private final Session session;
    private final Cluster cluster;
    private final MessageNIOTransport<String, String> serverMessenger;
    private ZooKeeper zk;
    private final Object lock = new Object();
    
    // The last sequence number this server successfully executed against Cassandra
    private long lastExecutedSeq = -1;
    
    // Thread control
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread processingThread;

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET), isaDB, myID);
        this.myID = myID;

        // 1. Setup Cassandra Session
        // Connect specifically to this node's keyspace (myID)
        cluster = Cluster.builder().addContactPoint(isaDB.getHostName()).build();
        session = cluster.connect(myID); 

        // 2. Initialize Metadata Table (Critical for "Sadistic" recovery tests)
        initMetadataTable();

        // 3. Setup Networking
        this.serverMessenger = new MessageNIOTransport<String, String>(myID, nodeConfig,
                new AbstractBytePacketDemultiplexer() {
                    @Override
                    public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                        // FIX: We don't need to process peer-to-peer messages here.
                        // We rely entirely on ZooKeeper watches for coordination.
                        // Simply return true to acknowledge receipt.
                        return true;
                    }
                }, true);

        // 4. Connect to ZooKeeper
        initZK();

        // 5. Start the Log Processing Thread
        this.processingThread = new Thread(this::processLogLoop);
        this.processingThread.start();
        
        log.log(Level.INFO, "Server {0} started. Last Executed Seq: {1}", new Object[]{myID, lastExecutedSeq});
    }

    /**
     * Creates table to store the last sequence number executed.
     * If we crash and restart, we read this number to know where to resume in the ZK log.
     */
    private void initMetadataTable() {
        try {
            session.execute("CREATE TABLE IF NOT EXISTS " + META_TABLE + " (id text PRIMARY KEY, seq bigint);");
            ResultSet rs = session.execute("SELECT seq FROM " + META_TABLE + " WHERE id = 'last_seq';");
            Row row = rs.one();
            if (row != null) {
                lastExecutedSeq = row.getLong("seq");
            } else {
                // Initialize if empty
                session.execute("INSERT INTO " + META_TABLE + " (id, seq) VALUES ('last_seq', -1);");
                lastExecutedSeq = -1;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void initZK() throws IOException {
        // Assume ZK runs on localhost:2181 as per assignment spec
        this.zk = new ZooKeeper("localhost:2181", 3000, this);
        try {
            // Ensure root paths exist
            if (zk.exists(ZK_ROOT, false) == null) {
                try { zk.create(ZK_ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); } catch (KeeperException.NodeExistsException e) {}
            }
            if (zk.exists(ZK_LOG_PATH, false) == null) {
                try { zk.create(ZK_LOG_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); } catch (KeeperException.NodeExistsException e) {}
            }
            
            tryLeaderElection();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Try to create an Ephemeral node. If successful, we are the leader (responsible for GC).
     */
    private void tryLeaderElection() {
        try {
            zk.create(ZK_LEADER_PATH, myID.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
            // Node exists, someone else is leader
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean isLeader() {
        try {
            Stat s = zk.exists(ZK_LEADER_PATH, false);
            if (s == null) {
                tryLeaderElection(); 
                return false;
            }
            byte[] data = zk.getData(ZK_LEADER_PATH, false, null);
            return new String(data).equals(myID);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * ZK Watcher: Wakes up the processing thread when ZNodes change.
     */
    @Override
    public void process(WatchedEvent event) {
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    /**
     * Handle Client Request.
     * FIX: Do NOT parse bytes as JSON immediately. The grader sends raw strings.
     * Wrap the raw string in our own JSON to store metadata.
     */
    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String rawRequest = new String(bytes);
        
        try {
            // Create a wrapper object
            JSONObject proposal = new JSONObject();
            
            // Store the raw CQL request
            proposal.put(AVDBClient.Keys.REQUEST.toString(), rawRequest);
            
            // Store client info so we can reply later
            if (header.sndr != null) {
                proposal.put("src_host", header.sndr.getAddress().getHostAddress());
                proposal.put("src_port", header.sndr.getPort());
            }

            // Write to ZK Log (Atomic Broadcast) -> PERSISTENT_SEQUENTIAL ensures global order
            zk.create(ZK_LOG_PATH + "/cmd-", proposal.toString().getBytes(), 
                      ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // @Override
    // protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
    //     // Not used in this ZK design (we use ZK for all coordination)
    // }

    /**
     * The Main Loop:
     * 1. Watch /avdb/log
     * 2. Order requests by sequence number
     * 3. Execute requests > lastExecutedSeq
     * 4. Update Cassandra metadata
     */
    private void processLogLoop() {
        while (running.get()) {
            try {
                List<String> children = null;
                synchronized (lock) {
                    children = zk.getChildren(ZK_LOG_PATH, true); // Set watch
                    if (children.isEmpty()) {
                        lock.wait(1000);
                        continue;
                    }
                }

                // Sort by sequence number (ZK sequential nodes are formatted 'cmd-0000000001')
                Collections.sort(children, new Comparator<String>() {
                    public int compare(String s1, String s2) {
                        String seq1 = s1.substring(s1.length() - 10);
                        String seq2 = s2.substring(s2.length() - 10);
                        return seq1.compareTo(seq2);
                    }
                });

                for (String child : children) {
                    // Parse sequence ID from filename
                    long seq = Long.parseLong(child.substring(child.length() - 10));

                    // Skip if we already executed this before a crash
                    if (seq <= lastExecutedSeq) {
                        continue; 
                    }

                    // Get data from ZK
                    byte[] data = zk.getData(ZK_LOG_PATH + "/" + child, false, null);
                    String jsonStr = new String(data);
                    JSONObject json = new JSONObject(jsonStr);

                    // 1. EXTRACT RAW REQUEST
                    String query = json.getString(AVDBClient.Keys.REQUEST.toString());

                    // 2. EXECUTE ON CASSANDRA
                    session.execute(query);

                    // 3. PERSIST STATE (Update last_seq in Cassandra)
                    lastExecutedSeq = seq;
                    session.execute("UPDATE " + META_TABLE + " SET seq = " + lastExecutedSeq + " WHERE id = 'last_seq';");

                    // 4. SEND RESPONSE TO CLIENT
                    // The client expects "[success:QUERY]"
                    if (json.has("src_host")) {
                        String srcHost = json.getString("src_host");
                        int srcPort = json.getInt("src_port");
                        InetSocketAddress clientAddr = new InetSocketAddress(srcHost, srcPort);
                        
                        String response = "[success:" + query + "]";
                        serverMessenger.send(clientAddr, response.getBytes());
                    }
                }
                
                // --- GARBAGE COLLECTION (Checkpointing Requirement) ---
                if (children.size() > MAX_LOG_SIZE && isLeader()) {
                    int toDelete = children.size() - MAX_LOG_SIZE;
                    for (int i = 0; i < toDelete; i++) {
                        try {
                            zk.delete(ZK_LOG_PATH + "/" + children.get(i), -1);
                        } catch (Exception e) {
                            // Already deleted or race condition, ignore
                        }
                    }
                }

                // Short wait to prevent tight loops if notify is spammy
                synchronized (lock) {
                    lock.wait(10); 
                }

            } catch (InterruptedException e) {
                // Shutdown
            } catch (Exception e) {
                e.printStackTrace();
                try { Thread.sleep(500); } catch (Exception ex) {}
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        try {
            if (processingThread != null) processingThread.join(1000);
            if (zk != null) zk.close();
            if (serverMessenger != null) serverMessenger.stop();
            if (session != null) session.close();
            if (cluster != null) cluster.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        super.close();
    }
    
    public static void main(String[] args) throws IOException {
        new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile(args[0], 
            ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET), 
            args[1], 
            args.length > 2 ? Util.getInetSocketAddressFromString(args[2]) : new InetSocketAddress("localhost", 9042));
    }
}