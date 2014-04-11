package org.geogit.storage.bdbje;

import static com.sleepycat.je.OperationStatus.NOTFOUND;
import static com.sleepycat.je.OperationStatus.SUCCESS;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.geogit.api.ObjectId;
import org.geogit.repository.RepositoryConnectionException;
import org.geogit.storage.ConfigDatabase;
import org.geogit.storage.GraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public class JEGraphDatabase implements GraphDatabase {

    private static final Logger LOGGER = LoggerFactory.getLogger(JEGraphDatabase.class);

    private EnvironmentBuilder envProvider;

    /**
     * Lazily loaded, do not access it directly but through {@link #createEnvironment()}
     */
    protected Environment env;

    protected Database graphDb;

    @Nullable
    private ExecutorService dbSyncService;

    private ExecutorService writerService;

    private final String envName;

    private final ConfigDatabase configDb;

    @Inject
    public JEGraphDatabase(final ConfigDatabase config, final EnvironmentBuilder envProvider) {
        this(config, envProvider, "objects");
    }

    public JEGraphDatabase(final ConfigDatabase config, final EnvironmentBuilder envProvider,
            final String envName) {
        this.configDb = config;
        this.envProvider = envProvider;
        this.envName = envName;
    }

    @Override
    public void open() {
        if (isOpen()) {
            LOGGER.trace("Environment {} already open", env.getHome());
            return;
        }
        this.graphDb = createDatabase();

        int nWriterThreads = 1;
        writerService = Executors.newFixedThreadPool(nWriterThreads, new ThreadFactoryBuilder()
                .setNameFormat("BDBJE-" + env.getHome().getName() + "-WRITE-THREAD-%d").build());
        if (!graphDb.getConfig().getTransactional()) {
            dbSyncService = Executors.newFixedThreadPool(nWriterThreads, new ThreadFactoryBuilder()
                    .setNameFormat("BDBJE-" + env.getHome().getName() + "-SYNC-THREAD-%d").build());
        }

        LOGGER.debug("Graph database opened at {}. Transactional: {}", env.getHome(), graphDb
                .getConfig().getTransactional());
    }

    protected Database createDatabase() {

        final String databaseName = "GraphDatabase";
        Environment environment;
        try {
            environment = createEnvironment();
        } catch (EnvironmentLockedException e) {
            throw new IllegalStateException(
                    "The repository is already open by another process for writing", e);
        }

        if (!environment.getDatabaseNames().contains(databaseName)) {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            Database openDatabase = environment.openDatabase(null, databaseName, dbConfig);
            openDatabase.close();
            environment.flushLog(true);
            environment.close();
            environment = createEnvironment();
        }

        Database database;
        try {
            LOGGER.debug("Opening GraphDatabase at {}", environment.getHome());

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setCacheMode(CacheMode.MAKE_COLD);
            dbConfig.setKeyPrefixing(false);// can result in a slightly smaller db size

            boolean transactional = environment.getConfig().getTransactional();
            dbConfig.setTransactional(transactional);
            dbConfig.setDeferredWrite(!transactional);

            database = environment.openDatabase(null, databaseName, dbConfig);
        } catch (RuntimeException e) {
            if (environment != null) {
                environment.close();
            }
            throw e;
        }
        this.env = environment;
        return database;

    }

    /**
     * @return creates and returns the environment
     */
    private synchronized Environment createEnvironment()
            throws com.sleepycat.je.EnvironmentLockedException {

        Environment env = envProvider.setRelativePath(this.envName).get();

        return env;
    }

    @Override
    public void configure() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.GRAPH.configure(configDb, "bdbje", "0.1");
    }

    @Override
    public void checkConfig() throws RepositoryConnectionException {
        RepositoryConnectionException.StorageType.GRAPH.verify(configDb, "bdbje", "0.1");
    }

    @Override
    public boolean isOpen() {
        return graphDb != null;
    }

    @Override
    public void close() {
        if (env == null) {
            LOGGER.trace("Database already closed.");
            return;
        }
        final File envHome = env.getHome();
        try {
            LOGGER.debug("Closing graph database at {}", envHome);
            if (writerService != null) {
                writerService.shutdown();
                waitForServiceShutDown(writerService);
            }
            if (graphDb != null) {
                graphDb.close();
                graphDb = null;
            }
            if (dbSyncService != null) {
                dbSyncService.shutdown();
                waitForServiceShutDown(dbSyncService);
            }
            LOGGER.trace("GraphDatabase closed. Closing environment...");
            env.sync();
            env.cleanLog();
        } finally {
            env.close();
            env = null;
        }
        LOGGER.debug("Database {} closed.", envHome);

    }

    private void waitForServiceShutDown(ExecutorService service) {
        try {
            while (!service.isTerminated()) {
                service.awaitTermination(100, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            LOGGER.warn("Error waiting for service to finish", e);
        }
    }

    private class NodeData implements Serializable {
        private static final long serialVersionUID = -7213804957950419813L;

        public List<ObjectId> outgoing;

        public List<ObjectId> incoming;

        public Map<String, String> properties;

        public ObjectId mappedTo;

        public ObjectId id;

        public NodeData(ObjectId id, ImmutableList<ObjectId> parents) {
            this.id = id;
            this.outgoing = new LinkedList<ObjectId>();
            this.outgoing.addAll(parents);
            this.incoming = new LinkedList<ObjectId>();
            this.properties = new HashMap<String, String>();
            this.mappedTo = null;
        }

        public NodeData(ObjectId id) {
            this(id, new ImmutableList.Builder<ObjectId>().build());
        }

        public boolean isSparse() {
            return properties.containsKey(SPARSE_FLAG) ? Boolean.valueOf(properties
                    .get(SPARSE_FLAG)) : false;
        }

        private void writeObject(java.io.ObjectOutputStream out) throws IOException {
            out.writeObject(outgoing);
            out.writeObject(incoming);
            out.writeObject(properties);
            if (mappedTo != null) {
                out.writeBoolean(true);
                out.writeObject(mappedTo);
            } else {
                out.writeBoolean(false);
            }
            if (id != null) {
                out.writeBoolean(true);
                out.writeObject(id);
            } else {
                out.writeBoolean(false);
            }
        }

        @SuppressWarnings("unchecked")
        private void readObject(java.io.ObjectInputStream in) throws IOException,
                ClassNotFoundException {
            outgoing = (List<ObjectId>) in.readObject();
            incoming = (List<ObjectId>) in.readObject();
            properties = (Map<String, String>) in.readObject();
            if (in.readBoolean()) {
                mappedTo = (ObjectId) in.readObject();
            } else {
                mappedTo = null;
            }
            if (in.readBoolean()) {
                id = (ObjectId) in.readObject();
            } else {
                in = null;
            }
        }
    }

    protected NodeData getNodeInternal(final ObjectId id, final boolean failIfNotFound) {
        Preconditions.checkNotNull(id, "id");
        String graphKey = id.toString() + "-graph";
        DatabaseEntry key = new DatabaseEntry(graphKey.getBytes());
        DatabaseEntry data = new DatabaseEntry();

        final LockMode lockMode = LockMode.READ_UNCOMMITTED;
        Transaction transaction = null;
        OperationStatus operationStatus = graphDb.get(transaction, key, data, lockMode);
        if (NOTFOUND.equals(operationStatus)) {
            if (failIfNotFound) {
                throw new IllegalArgumentException("Graph Object does not exist: " + id.toString()
                        + " at " + env.getHome().getAbsolutePath());
            }
            return null;
        }
        final byte[] cData = data.getData();
        NodeData node = null;
        try {
            node = (NodeData) new ObjectInputStream(new ByteArrayInputStream(cData)).readObject();
        } catch (Exception e) {
        }
        return node;
    }

    private boolean putNodeInternal(final ObjectId id, final NodeData node) {
        final Transaction transaction = newTransaction();

        final OperationStatus status;
        try {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            ObjectOutputStream o = new ObjectOutputStream(b);
            o.writeObject(node);
            status = putNodeInternal(id, b.toByteArray(), transaction);
            commit(transaction);
            return SUCCESS.equals(status);
        } catch (Exception e) {
            abort(transaction);
        }
        return false;
    }

    private OperationStatus putNodeInternal(final ObjectId id, final byte[] rawData,
            Transaction transaction) {
        OperationStatus status;
        String graphKey = id.toString() + "-graph";
        final byte[] rawKey = graphKey.getBytes();
        DatabaseEntry key = new DatabaseEntry(rawKey);
        DatabaseEntry data = new DatabaseEntry(rawData);

        status = graphDb.put(transaction, key, data);
        return status;
    }

    private void abort(@Nullable Transaction transaction) {
        if (transaction != null) {
            try {
                transaction.abort();
            } catch (Exception e) {
                LOGGER.error("Error aborting transaction", e);
            }
        }
    }

    private void commit(@Nullable Transaction transaction) {
        if (transaction != null) {
            try {
                transaction.commit();
            } catch (Exception e) {
                LOGGER.error("Error committing transaction", e);
            }
        }
    }

    @Nullable
    private Transaction newTransaction() {
        final boolean transactional = graphDb.getConfig().getTransactional();
        if (transactional) {
            TransactionConfig txConfig = new TransactionConfig();
            txConfig.setReadUncommitted(true);
            Optional<String> durability = configDb.get("bdbje.object_durability");
            if ("safe".equals(durability.orNull())) {
                txConfig.setDurability(Durability.COMMIT_SYNC);
            } else {
                txConfig.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
            }
            Transaction transaction = env.beginTransaction(null, txConfig);
            return transaction;
        }
        return null;
    }

    @Override
    public boolean exists(ObjectId commitId) {
        Preconditions.checkNotNull(commitId, "id");

        String graphKey = commitId.toString() + "-graph";
        DatabaseEntry key = new DatabaseEntry(graphKey.getBytes());
        DatabaseEntry data = new DatabaseEntry();
        // tell db not to retrieve data
        data.setPartial(0, 0, true);

        final LockMode lockMode = LockMode.READ_UNCOMMITTED;
        Transaction transaction = null;
        OperationStatus status = graphDb.get(transaction, key, data, lockMode);
        return SUCCESS == status;
    }

    @Override
    public ImmutableList<ObjectId> getParents(ObjectId commitId) throws IllegalArgumentException {
        Builder<ObjectId> listBuilder = new ImmutableList.Builder<ObjectId>();
        NodeData node = getNodeInternal(commitId, true);
        return listBuilder.addAll(node.outgoing).build();
    }

    @Override
    public ImmutableList<ObjectId> getChildren(ObjectId commitId) throws IllegalArgumentException {
        Builder<ObjectId> listBuilder = new ImmutableList.Builder<ObjectId>();
        NodeData node = getNodeInternal(commitId, true);
        return listBuilder.addAll(node.incoming).build();
    }

    @Override
    public boolean put(ObjectId commitId, ImmutableList<ObjectId> parentIds) {
        NodeData node = getNodeInternal(commitId, false);
        boolean updated = false;
        if (node == null) {
            node = new NodeData(commitId, parentIds);
            updated = true;
        }
        for (ObjectId parent : parentIds) {
            if (!node.outgoing.contains(parent)) {
                node.outgoing.add(parent);
                updated = true;
            }
            NodeData parentNode = getNodeInternal(parent, false);
            if (parentNode == null) {
                parentNode = new NodeData(parent);
                updated = true;
            }
            if (!parentNode.incoming.contains(commitId)) {
                parentNode.incoming.add(commitId);
                updated = true;
            }
            putNodeInternal(parent, parentNode);
        }
        putNodeInternal(commitId, node);
        return updated;
    }

    @Override
    public void map(ObjectId mapped, ObjectId original) {
        NodeData node = getNodeInternal(mapped, true);
        node.mappedTo = original;
        putNodeInternal(mapped, node);
    }

    @Override
    public ObjectId getMapping(ObjectId commitId) {
        NodeData node = getNodeInternal(commitId, true);
        return node.mappedTo;
    }

    @Override
    public int getDepth(ObjectId commitId) {
        int depth = 0;

        Queue<ObjectId> q = Lists.newLinkedList();
        NodeData node = getNodeInternal(commitId, true);
        Iterables.addAll(q, node.outgoing);

        List<ObjectId> next = Lists.newArrayList();
        while (!q.isEmpty()) {
            depth++;
            while (!q.isEmpty()) {
                ObjectId n = q.poll();
                NodeData parentNode = getNodeInternal(n, true);
                List<ObjectId> parents = Lists.newArrayList(parentNode.outgoing);
                if (parents.size() == 0) {
                    return depth;
                }

                Iterables.addAll(next, parents);
            }

            q.addAll(next);
            next.clear();
        }

        return depth;
    }

    @Override
    public void setProperty(ObjectId commitId, String propertyName, String propertyValue) {
        NodeData node = getNodeInternal(commitId, true);
        node.properties.put(propertyName, propertyValue);
        putNodeInternal(commitId, node);
    }

    @Override
    public boolean isSparsePath(ObjectId start, ObjectId end) {
        NodeData node = getNodeInternal(start, true);
        return sparsePath_recursive(node, end, false);
    }

    private boolean sparsePath_recursive(NodeData node, ObjectId end, boolean sparse) {
        if (node.id.equals(end)) {
            return sparse;
        }
        if (node.outgoing.size() > 0) {
            boolean node_sparse = node.isSparse();
            boolean combined_sparse = false;
            for (ObjectId parent : node.outgoing) {
                NodeData parentNode = getNodeInternal(parent, true);
                combined_sparse = combined_sparse
                        || sparsePath_recursive(parentNode, end, sparse || node_sparse);
            }
            return combined_sparse;
        }
        return false;
    }

    private class JEGraphNode extends GraphNode {
        NodeData node;

        public JEGraphNode(NodeData node) {
            this.node = node;
        }

        @Override
        public ObjectId getIdentifier() {
            return node.id;
        }

        @Override
        public List<GraphEdge> getEdges(Direction direction) {
            List<GraphEdge> edges = new LinkedList<GraphEdge>();
            if (direction == Direction.IN || direction == Direction.BOTH) {
                Iterator<ObjectId> nodeEdges = node.incoming.iterator();
                while (nodeEdges.hasNext()) {
                    ObjectId otherNode = nodeEdges.next();
                    edges.add(new GraphEdge(new JEGraphNode(getNodeInternal(otherNode, true)), this));
                }
            }
            if (direction == Direction.OUT || direction == Direction.BOTH) {
                Iterator<ObjectId> nodeEdges = node.outgoing.iterator();
                while (nodeEdges.hasNext()) {
                    ObjectId otherNode = nodeEdges.next();
                    edges.add(new GraphEdge(this, new JEGraphNode(getNodeInternal(otherNode, true))));
                }
            }
            return edges;
        }

    }

    @Override
    public GraphNode getNode(ObjectId id) {
        return new JEGraphNode(getNodeInternal(id, true));
    }

    @Override
    public void truncate() {
        // TODO Auto-generated method stub

    }

}
