/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api.plumbing;

import javax.annotation.Nullable;

import org.geogit.api.Bounded;
import org.geogit.api.Bucket;
import org.geogit.api.Node;
import org.geogit.api.ObjectId;
import org.geogit.api.RevTree;
import org.geogit.api.plumbing.diff.DiffTreeVisitor;
import org.geogit.api.porcelain.CommitOp;
import org.geogit.storage.ObjectDatabase;
import org.geogit.test.integration.RepositoryTestCase;
import org.junit.Test;

import com.vividsolutions.jts.geom.Envelope;

public class DiffBoundsTest extends RepositoryTestCase {

    @Override
    protected void setUpInternal() throws Exception {
        populate(true, points1, points3, points1_modified);

        points1_modified = feature(pointsType, idP1, "StringProp1_1a", new Integer(1001),
                "POINT(10 20)");
        insertAndAdd(points1_modified);
        geogit.command(CommitOp.class).call();

        points1B_modified = feature(pointsType, idP1, "StringProp1B_1a", new Integer(2000),
                "POINT(10 220)");
        insertAndAdd(points1B_modified);
        geogit.command(CommitOp.class).call();
    }

    @Test
    public void testDiffBetweenDifferentTrees() {
        String oldRefSpec = "HEAD~3";
        String newRefSpec = "HEAD";

        Envelope diffBounds = geogit.command(DiffBounds.class).setOldVersion(oldRefSpec)
                .setNewVersion(newRefSpec).call();

        assertEquals(diffBounds.getMinX(), 1.0, 0.0);
        assertEquals(diffBounds.getMinY(), 1.0, 0.0);
        assertEquals(diffBounds.getMaxX(), 10.0, 0.0);
        assertEquals(diffBounds.getMaxY(), 220.0, 0.0);

        testDiffTreeVisitor(oldRefSpec, newRefSpec, diffBounds);
    }

    private void testDiffTreeVisitor(String oldRefSpec, String newRefSpec, Envelope expected) {
        ObjectId leftId = geogit.command(ResolveTreeish.class).setTreeish(oldRefSpec).call().get();
        ObjectId rightId = geogit.command(ResolveTreeish.class).setTreeish(newRefSpec).call().get();
        RevTree left = geogit.command(RevObjectParse.class).setObjectId(leftId).call(RevTree.class)
                .get();
        RevTree right = geogit.command(RevObjectParse.class).setObjectId(rightId)
                .call(RevTree.class).get();

        ObjectDatabase leftSource = getRepository().objectDatabase();
        ObjectDatabase rightSource = getRepository().objectDatabase();

        DiffTreeVisitor visitor = new DiffTreeVisitor(left, right, leftSource, rightSource);
        BoundsWalk boundsCalc = new BoundsWalk();
        visitor.walk(boundsCalc);
        Envelope env = boundsCalc.result;
        assertEquals(expected, env);
    }

    @Test
    public void testDiffBetweenIdenticalTrees() {
        String oldRefSpec = "HEAD";
        String newRefSpec = "HEAD";
        Envelope diffBounds = geogit.command(DiffBounds.class).setOldVersion(oldRefSpec)
                .setNewVersion(newRefSpec).call();
        assertTrue(diffBounds.isNull());

        testDiffTreeVisitor(oldRefSpec, newRefSpec, diffBounds);
    }

    private static class BoundsWalk implements DiffTreeVisitor.Consumer {

        Envelope result = new Envelope();

        private Envelope leftEnv = new Envelope();

        private Envelope rightEnv = new Envelope();

        @Override
        public void feature(@Nullable Node left, @Nullable Node right) {
            setEnv(left, leftEnv);
            setEnv(right, rightEnv);
            if (!leftEnv.equals(rightEnv)) {
                result.expandToInclude(leftEnv);
                result.expandToInclude(rightEnv);
            }
        }

        @Override
        public boolean tree(@Nullable Node left, @Nullable Node right) {
            setEnv(left, leftEnv);
            setEnv(right, rightEnv);
            if (leftEnv.isNull() && rightEnv.isNull()) {
                return false;
            }

            if (leftEnv.isNull()) {
                result.expandToInclude(rightEnv);
                return false;
            } else if (rightEnv.isNull()) {
                result.expandToInclude(leftEnv);
                return false;
            }
            return true;
        }

        @Override
        public boolean bucket(final int bucketIndex, final int bucketDepth, @Nullable Bucket left,
                @Nullable Bucket right) {
            setEnv(left, leftEnv);
            setEnv(right, rightEnv);
            if (leftEnv.isNull() && rightEnv.isNull()) {
                return false;
            }

            if (leftEnv.isNull()) {
                result.expandToInclude(rightEnv);
                return false;
            } else if (rightEnv.isNull()) {
                result.expandToInclude(leftEnv);
                return false;
            }
            return true;
        }

        private void setEnv(@Nullable Bounded bounded, Envelope env) {
            env.setToNull();
            if (bounded != null) {
                bounded.expand(env);
            }
        }

    }
}
