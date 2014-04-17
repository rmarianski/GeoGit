/* Copyright (c) 2014 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api.plumbing.diff;

import org.geogit.api.Bucket;
import org.geogit.api.Node;
import org.geogit.api.NodeRef;
import org.geogit.api.ObjectId;
import org.geogit.api.RevTree;
import org.geogit.api.plumbing.diff.DiffTreeVisitor.Consumer;
import org.geogit.storage.ObjectDatabase;

/**
 * A {@link Consumer} for diffs that computes the number for tree and feature changes between the
 * traversal's two trees.
 * 
 * <p>
 * Use {@link DiffCountConsumer#get() consumer.get()} after {@link DiffTreeVisitor#walk(Consumer)
 * visitor.walk(consumer)} to get the resulting {@link DiffObjectCount}.
 */
public class DiffCountConsumer implements DiffTreeVisitor.Consumer {

    private ObjectDatabase db;

    private DiffObjectCount count = new DiffObjectCount();

    public DiffCountConsumer(ObjectDatabase db) {
        this.db = db;
    }

    public DiffObjectCount get() {
        return count;
    }

    @Override
    public void feature(Node left, Node right) {
        count.addFeatures(1L);
    }

    @Override
    public boolean tree(Node left, Node right) {
        if (left == null || right == null) {
            Node node = left == null ? right : left;
            if (NodeRef.ROOT.equals(node.getName())) {
                // ignore the call on the root tree and follow the traversal
                return true;
            }
            count.addTrees(1L);
            addTreeFeatures(node.getObjectId());
            return false;
        }
        return true;
    }

    @Override
    public boolean bucket(int bucketIndex, int bucketDepth, Bucket left, Bucket right) {
        if (left == null || right == null) {
            Bucket bucket = left == null ? right : left;
            addTreeFeatures(bucket.id());
            return false;
        }
        return true;
    }

    private boolean addTreeFeatures(ObjectId treeId) {
        RevTree tree = db.getTree(treeId);
        count.addFeatures(tree.size());
        int numTrees = tree.numTrees();
        return numTrees > 0;
    }
}