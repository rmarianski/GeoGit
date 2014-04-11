/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.blueprints;

import static com.google.common.base.Preconditions.checkState;
import static com.tinkerpop.blueprints.Direction.BOTH;
import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.geogit.api.ObjectId;
import org.geogit.api.Platform;
import org.geogit.api.plumbing.ResolveGeogitDir;
import org.geogit.storage.GraphDatabase;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.branch.LoopPipe.LoopBundle;

/**
 * An abstract implementation of {@link GraphDatabase} on top of the <a
 * href="https://github.com/tinkerpop/blueprints/">blueprints</a> {@link IndexableGraph graph API}.
 * 
 * @param <DB>
 */
public abstract class BlueprintsGraphDatabase<DB extends KeyIndexableGraph> implements
        GraphDatabase {

    protected DB graphDB = null;

    protected String dbPath;

    protected static Map<String, ServiceContainer<?>> databaseServices = new ConcurrentHashMap<String, ServiceContainer<?>>();

    protected final Platform platform;

    private Vertex root;

    static protected class BlueprintsGraphNode extends GraphNode {

        Vertex node;

        public BlueprintsGraphNode(Vertex node) {
            this.node = node;
        }

        @Override
        public ObjectId getIdentifier() {
            return ObjectId.valueOf((String) node.getProperty("identifier"));
        }

        @Override
        public List<GraphEdge> getEdges(Direction direction) {
            Iterator<Edge> nodeEdges;
            switch (direction) {
            case OUT:
                nodeEdges = node.getEdges(OUT, Relationship.PARENT.name()).iterator();
                break;
            case IN:
                nodeEdges = node.getEdges(IN, Relationship.PARENT.name()).iterator();
                break;
            default:
                nodeEdges = node.getEdges(BOTH, Relationship.PARENT.name()).iterator();
            }
            List<GraphEdge> edges = new LinkedList<GraphEdge>();
            while (nodeEdges.hasNext()) {
                Edge nodeEdge = nodeEdges.next();
                edges.add(new GraphEdge(new BlueprintsGraphNode(nodeEdge.getVertex(OUT)),
                        new BlueprintsGraphNode(nodeEdge.getVertex(IN))));
            }
            return edges;
        }

        @Override
        public boolean isSparse() {
            return node.getPropertyKeys().contains(SPARSE_FLAG)
                    && Boolean.valueOf((String) node.getProperty(SPARSE_FLAG));
        }
    }

    /**
     * Container class for the database service to keep track of reference counts.
     */
    static protected class ServiceContainer<DB extends Graph> {
        private DB dbService;

        private int refCount;

        public ServiceContainer(DB dbService) {
            this.dbService = dbService;
            this.refCount = 0;
        }

        public void removeRef() {
            this.refCount--;
        }

        public void addRef() {
            this.refCount++;
        }

        public int getRefCount() {
            return this.refCount;
        }

        public DB getService() {
            return this.dbService;
        }
    }

    public BlueprintsGraphDatabase(Platform platform) {
        this.platform = platform;
    }

    @Override
    public void open() {
        if (isOpen()) {
            return;
        }

        Optional<URL> envHome = new ResolveGeogitDir(platform).call();
        checkState(envHome.isPresent(), "Not inside a geogit directory");

        final URL envUrl = envHome.get();
        if (!"file".equals(envUrl.getProtocol())) {
            throw new UnsupportedOperationException(
                    "This Graph Database works only against file system repositories. "
                            + "Repository location: " + envUrl.toExternalForm());
        }
        File repoDir;
        try {
            repoDir = new File(envUrl.toURI());
        } catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }
        File graph = new File(repoDir, "graph");
        if (!graph.exists() && !graph.mkdir()) {
            throw new IllegalStateException("Cannot create graph directory '"
                    + graph.getAbsolutePath() + "'");
        }

        dbPath = graph.getAbsolutePath() + "/graphDB.db";

        if (databaseServices.containsKey(dbPath)) {
            @SuppressWarnings("unchecked")
            ServiceContainer<DB> serviceContainer = (ServiceContainer<DB>) databaseServices
                    .get(dbPath);
            serviceContainer.addRef();
            graphDB = serviceContainer.getService();
        } else {
            graphDB = getGraphDatabase();
            ServiceContainer<DB> newContainer = new ServiceContainer<DB>(graphDB);
            newContainer.addRef();
            databaseServices.put(dbPath, newContainer);
        }

        if (!graphDB.getIndexedKeys(Vertex.class).contains("identifier")) {
            graphDB.createKeyIndex("identifiers", Vertex.class);
        }
        Iterable<Vertex> results = graphDB.getVertices("identifier", "root");
        try {
            Iterator<Vertex> iter = results.iterator();
            if (iter.hasNext()) {
                root = iter.next();
                this.rollback();
            } else {
                root = graphDB.addVertex(null);
                root.setProperty("identifier", "root");
                this.commit();
            }
        } catch (Exception e) {
            this.rollback();
            throw Throwables.propagate(e);
        }
    }

    /**
     * Constructs the graph database service.
     * 
     * @return the new {@link IndexableGraph}
     */
    protected abstract DB getGraphDatabase();

    /**
     * Destroy the graph database service. This will only happen when the ref count for the database
     * service is 0.
     */
    protected void destroyGraphDatabase() {
        File graphPath = new File(dbPath);
        if (graphPath.exists()) {
            graphDB.shutdown();
        }
        databaseServices.remove(dbPath);
    }

    /**
     * @return true if the database is open, false otherwise
     */
    @Override
    public boolean isOpen() {
        return graphDB != null;
    }

    /**
     * Closes the database.
     */
    @Override
    public synchronized void close() {
        if (isOpen()) {
            @SuppressWarnings("unchecked")
            ServiceContainer<DB> container = (ServiceContainer<DB>) databaseServices.get(dbPath);
            container.removeRef();
            if (container.getRefCount() <= 0) {
                destroyGraphDatabase();
                databaseServices.remove(dbPath);
            }
            graphDB = null;
        }
    }

    /**
     * Determines if the given commit exists in the graph database.
     * 
     * @param commitId the commit id to search for
     * @return true if the commit exists, false otherwise
     */
    @Override
    public boolean exists(ObjectId commitId) {
        try {
            Iterable<Vertex> results = graphDB.getVertices("identifier", commitId.toString());
            Iterator<Vertex> iterator = results.iterator();
            if (iterator.hasNext()) {
                iterator.next();
                return true;
            } else {
                return false;
            }
        } finally {
            this.rollback();
        }
    }

    /**
     * Retrieves all of the parents for the given commit.
     * 
     * @param commitid the commit whose parents should be returned
     * @return a list of the parents of the provided commit
     * @throws IllegalArgumentException
     */
    @Override
    public ImmutableList<ObjectId> getParents(ObjectId commitId) throws IllegalArgumentException {
        try {
            Vertex node = null;
            Iterable<Vertex> results = graphDB.getVertices("identifier", commitId.toString());
            Iterator<Vertex> iterator = results.iterator();
            if (iterator.hasNext()) {
                node = iterator.next();
            }

            Builder<ObjectId> listBuilder = new ImmutableList.Builder<ObjectId>();

            if (node != null) {
                for (Edge edge : node.getEdges(OUT, Relationship.PARENT.name())) {
                    Vertex parentNode = edge.getVertex(IN);
                    listBuilder
                            .add(ObjectId.valueOf(parentNode.<String> getProperty("identifier")));
                }
            }
            return listBuilder.build();
        } finally {
            this.rollback();
        }
    }

    /**
     * Retrieves all of the children for the given commit.
     * 
     * @param commitid the commit whose children should be returned
     * @return a list of the children of the provided commit
     * @throws IllegalArgumentException
     */
    @Override
    public ImmutableList<ObjectId> getChildren(ObjectId commitId) throws IllegalArgumentException {
        try {
            Iterable<Vertex> results = graphDB.getVertices("identifier", commitId.toString());
            Vertex node = null;
            Iterator<Vertex> iterator = results.iterator();
            if (iterator.hasNext()) {
                node = iterator.next();
            }

            Builder<ObjectId> listBuilder = new ImmutableList.Builder<ObjectId>();

            if (node != null) {
                for (Edge child : node.getEdges(IN, Relationship.PARENT.name())) {
                    Vertex childNode = child.getVertex(OUT);
                    listBuilder.add(ObjectId.valueOf(childNode.<String> getProperty("identifier")));
                }
            }
            return listBuilder.build();
        } finally {
            this.rollback();
        }
    }

    @Override
    public GraphNode getNode(ObjectId id) {
        Iterable<Vertex> results = graphDB.getVertices("identifier", id.toString());
        Vertex node = null;
        Iterator<Vertex> iterator = results.iterator();
        if (iterator.hasNext()) {
            node = iterator.next();
        }
        return new BlueprintsGraphNode(node);
    }

    /**
     * Adds a commit to the database with the given parents. If a commit with the same id already
     * exists, it will not be inserted.
     * 
     * @param commitId the commit id to insert
     * @param parentIds the commit ids of the commit's parents
     * @return true if the commit id was inserted, false otherwise
     */
    @Override
    public boolean put(ObjectId commitId, ImmutableList<ObjectId> parentIds) {
        boolean updated = false;
        try {
            // See if it already exists
            Vertex commitNode = getOrAddNode(commitId);

            if (parentIds.isEmpty()) {
                if (!commitNode.getEdges(OUT, Relationship.TOROOT.name()).iterator().hasNext()) {
                    // Attach this node to the root node
                    commitNode.addEdge(Relationship.TOROOT.name(), root);
                    updated = true;
                }
            }

            if (!commitNode.getEdges(OUT, Relationship.PARENT.name()).iterator().hasNext()) {
                // Don't make relationships if they have been created already
                for (ObjectId parent : parentIds) {
                    Vertex parentNode = getOrAddNode(parent);
                    commitNode.addEdge(Relationship.PARENT.name(), parentNode);
                    updated = true;
                }
            }
            this.commit();
        } catch (Exception e) {
            this.rollback();
            throw Throwables.propagate(e);
        }
        return updated;
    }

    /**
     * Maps a commit to another original commit. This is used in sparse repositories.
     * 
     * @param mapped the id of the mapped commit
     * @param original the commit to map to
     */
    @Override
    public void map(final ObjectId mapped, final ObjectId original) {
        Vertex commitNode = null;
        try {
            // See if it already exists
            commitNode = getOrAddNode(mapped);

            Iterator<Edge> mappedTo = commitNode.getEdges(OUT, Relationship.MAPPED_TO.name())
                    .iterator();
            if (mappedTo.hasNext()) {
                // Remove old mapping
                Edge toRemove = mappedTo.next();
                graphDB.removeEdge(toRemove);
            }

            // Don't make relationships if they have been created already
            Vertex originalNode = getOrAddNode(original);
            commitNode.addEdge(Relationship.MAPPED_TO.name(), originalNode);
            this.commit();
        } catch (Exception e) {
            this.rollback();
            throw Throwables.propagate(e);
        }
    }

    /**
     * Gets the id of the commit that this commit is mapped to.
     * 
     * @param commitId the commit to find the mapping of
     * @return the mapped commit id
     */
    public ObjectId getMapping(final ObjectId commitId) {
        try {
            Vertex node = null;
            Iterable<Vertex> results = graphDB.getVertices("identifier", commitId.toString());
            node = results.iterator().next();

            ObjectId mapped = ObjectId.NULL;
            Vertex mappedNode = getMappedNode(node);
            if (mappedNode != null) {
                mapped = ObjectId.valueOf(mappedNode.<String> getProperty("identifier"));
            }
            return mapped;
        } finally {
            this.rollback();
        }
    }

    private Vertex getMappedNode(final Vertex commitNode) {
        if (commitNode != null) {
            Iterable<Edge> mappings = commitNode.getEdges(OUT, Relationship.MAPPED_TO.name());
            if (mappings.iterator().hasNext()) {
                return mappings.iterator().next().getVertex(IN);
            }
        }
        return null;
    }

    /**
     * Gets a node or adds it if it doesn't exist
     * 
     * @param commitId
     * @return
     */
    private Vertex getOrAddNode(ObjectId commitId) {
        final String commitIdStr = commitId.toString();
        Iterable<Vertex> matches = graphDB.getVertices("identifier", commitIdStr);
        Iterator<Vertex> iterator = matches.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            Vertex v = graphDB.addVertex(null);
            v.setProperty("identifier", commitIdStr);
            return v;
        }
    }

    /**
     * Gets the number of ancestors of the commit until it reaches one with no parents, for example
     * the root or an orphaned commit.
     * 
     * @param commitId the commit id to start from
     * @return the depth of the commit
     */
    @Override
    public int getDepth(final ObjectId commitId) {
        try {
            Vertex commitNode = null;
            Iterable<Vertex> results = graphDB.getVertices("identifier", commitId.toString());
            commitNode = results.iterator().next();
            PipeFunction<LoopBundle<Vertex>, Boolean> expandCriterion = new PipeFunction<LoopBundle<Vertex>, Boolean>() {
                @Override
                public Boolean compute(LoopBundle<Vertex> argument) {
                    Iterable<Edge> edges = argument.getObject().getEdges(OUT,
                            Relationship.PARENT.name());
                    return edges.iterator().hasNext();
                }
            };
            PipeFunction<LoopBundle<Vertex>, Boolean> emitCriterion = new PipeFunction<LoopBundle<Vertex>, Boolean>() {
                @Override
                public Boolean compute(LoopBundle<Vertex> argument) {
                    Iterable<Edge> edges = argument.getObject().getEdges(OUT,
                            Relationship.PARENT.name());
                    return !edges.iterator().hasNext();
                }
            };
            @SuppressWarnings("rawtypes")
            PipeFunction<List, List<Edge>> verticesOnly = new PipeFunction<List, List<Edge>>() {
                @Override
                public List<Edge> compute(List argument) {
                    List<Edge> results = new ArrayList<Edge>();
                    for (Object o : argument) {
                        if (o instanceof Edge) {
                            results.add((Edge) o);
                        }
                    }
                    return results;
                }
            };
            GremlinPipeline<Vertex, List<Edge>> pipe = new GremlinPipeline<Vertex, Vertex>()
                    .start(commitNode).as("start").outE(Relationship.PARENT.name()).inV()
                    .loop("start", expandCriterion, emitCriterion).path().transform(verticesOnly);

            if (pipe.hasNext()) {
                int length = Integer.MAX_VALUE;
                for (List<?> path : pipe) {
                    length = Math.min(length, path.size());
                }
                return length;
            } else {
                return 0;
            }
        } finally {
            this.rollback();
        }
    }

    /**
     * Set a property on the provided commit node.
     * 
     * @param commitId the id of the commit
     */
    public void setProperty(ObjectId commitId, String propertyName, String propertyValue) {
        try {
            Iterable<Vertex> results = graphDB.getVertices("identifier", commitId.toString());
            Vertex commitNode = results.iterator().next();
            commitNode.setProperty(propertyName, propertyValue);
            this.commit();
        } catch (Exception e) {
            this.rollback();
        }
    }

    @Override
    public void truncate() {
        try {
            Iterator<Edge> edges = graphDB.getEdges().iterator();
            while (edges.hasNext()) {
                graphDB.removeEdge(edges.next());
            }
            Iterator<Vertex> vertices = graphDB.getVertices().iterator();
            while (vertices.hasNext()) {
                graphDB.removeVertex(vertices.next());
            }
            this.commit();
        } catch (RuntimeException e) {
            this.rollback();
            throw e;
        }
    }

    /**
     * Template method for transactional graph db's to override and implement the commit action
     */
    protected void commit() {
        // Stub for transactional graphdb to use
    }

    /**
     * Template method for transactional graph db's to override and implement the rollback action
     */
    protected void rollback() {
        // Stub for transactional graphdb to use
    }

    @Override
    public String toString() {
        return String.format("%s[path: %s]", getClass().getSimpleName(), dbPath);
    }
}
