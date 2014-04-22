/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.api.plumbing;

import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkArgument;

import javax.annotation.Nullable;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.Bounded;
import org.geogit.api.Bucket;
import org.geogit.api.Node;
import org.geogit.api.ObjectId;
import org.geogit.api.Ref;
import org.geogit.api.RevTree;
import org.geogit.api.plumbing.diff.DiffTreeVisitor;
import org.geogit.storage.ObjectDatabase;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.vividsolutions.jts.geom.Envelope;

/**
 * Computes the bounds of the difference between the two trees instead of the actual diffs.
 * 
 */

public class DiffBounds extends AbstractGeoGitOp<Envelope> {

    private String oldVersion;

    private String newVersion;

    private boolean cached;

    public DiffBounds setOldVersion(String oldVersion) {
        this.oldVersion = oldVersion;
        return this;
    }

    public DiffBounds setNewVersion(String newVersion) {
        this.newVersion = newVersion;
        return this;
    }

    public DiffBounds setCompareIndex(boolean cached) {
        this.cached = cached;
        return this;
    }

    @Override
    protected  Envelope _call() {
        checkArgument(cached && oldVersion == null || !cached, String.format(
                "compare index allows only one revision to check against, got %s / %s", oldVersion,
                newVersion));

        checkArgument(newVersion == null || oldVersion != null,
                "If new rev spec is specified then old rev spec is mandatory");

        final String leftRefSpec = fromNullable(oldVersion).or(Ref.HEAD);
        final String rightRefSpec = fromNullable(newVersion).or(
                cached ? Ref.STAGE_HEAD : Ref.WORK_HEAD);

        RevTree left = resolveTree(leftRefSpec);
        RevTree right = resolveTree(rightRefSpec);

        ObjectDatabase leftSource = resolveSafeDb(leftRefSpec);
        ObjectDatabase rightSource = resolveSafeDb(rightRefSpec);
        DiffTreeVisitor visitor = new DiffTreeVisitor(left, right, leftSource, rightSource);
        BoundsWalk walk = new BoundsWalk();
        visitor.walk(walk);
        Envelope diffBounds = walk.result;
        return diffBounds;
    }

    /**
     * If {@code refSpec} can easily be determined to be on the object database (e.g. its a ref),
     * then returns the repository object database, otherwise the staging database, just to be safe
     */
    private ObjectDatabase resolveSafeDb(String refSpec) {
        Optional<Ref> ref = command(RefParse.class).setName(refSpec).call();
        return ref.isPresent() ? objectDatabase() : stagingDatabase();
    }

    private RevTree resolveTree(String refSpec) {

        Optional<ObjectId> id = command(ResolveTreeish.class).setTreeish(refSpec).call();
        Preconditions.checkState(id.isPresent(), "%s did not resolve to a tree", refSpec);

        return stagingDatabase().getTree(id.get());
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
