/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api.plumbing.merge;

import java.util.List;

import org.geogit.api.AbstractGeoGitOp;

public class ConflictsWriteOp extends AbstractGeoGitOp<Void> {

    private List<Conflict> conflicts;

    @Override
    protected  Void _call() {
        for (Conflict conflict : conflicts) {
            stagingDatabase().addConflict(null, conflict);
        }
        return null;

    }

    public ConflictsWriteOp setConflicts(List<Conflict> conflicts) {
        this.conflicts = conflicts;
        return this;
    }

}
