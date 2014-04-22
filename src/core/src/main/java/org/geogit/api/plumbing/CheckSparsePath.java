/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.api.plumbing;

import java.util.List;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.ObjectId;
import org.geogit.storage.GraphDatabase;
import org.geogit.storage.GraphDatabase.Direction;
import org.geogit.storage.GraphDatabase.GraphEdge;
import org.geogit.storage.GraphDatabase.GraphNode;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

/**
 * Determines if there are any sparse commits between the start commit and the end commit, not
 * including the end commit.
 */
public class CheckSparsePath extends AbstractGeoGitOp<Boolean> {

    private ObjectId start;

    private ObjectId end;

    private GraphDatabase graphDb;

    /**
     * Construct a new {@code CheckSparsePath} using the specified {@link GraphDatabase}.
     * 
     * @param repository the repository
     */
    @Inject
    public CheckSparsePath(GraphDatabase graphDb) {
        this.graphDb = graphDb;
    }

    /**
     * @param start the start {@link ObjectId}
     */
    public CheckSparsePath setStart(ObjectId start) {
        this.start = start;
        return this;
    }

    /**
     * @param end the end {@link ObjectId}
     */
    public CheckSparsePath setEnd(ObjectId end) {
        this.end = end;
        return this;
    }

    /**
     * Determines if there are any sparse commits between the start commit and the end commit, not
     * including the end commit.
     * 
     * @return true if there are any sparse commits between start and end
     */
    @Override
    protected  Boolean _call() {
        Preconditions.checkState(start != null, "start commit has not been set.");
        Preconditions.checkState(end != null, "end commit has not been set.");

        GraphNode node = graphDb.getNode(start);
        return isSparsePath(node, end, false);
    }

    /**
     * Recursive brute force function to exhaustively search for all paths between the start and end
     * node. This should be optimized to avoid unnecessary work in the future.
     */
    private boolean isSparsePath(GraphNode node, ObjectId end, boolean sparse) {
        if (node.getIdentifier().equals(end)) {
            return sparse;
        }
        List<GraphEdge> outgoing = node.getEdges(Direction.OUT);
        if (outgoing.size() > 0) {
            boolean node_sparse = node.isSparse();
            boolean combined_sparse = false;
            for (GraphEdge parent : outgoing) {
                GraphNode parentNode = parent.getToNode();
                combined_sparse = combined_sparse
                        || isSparsePath(parentNode, end, sparse || node_sparse);
            }
            return combined_sparse;
        }
        return false;
    }
}
