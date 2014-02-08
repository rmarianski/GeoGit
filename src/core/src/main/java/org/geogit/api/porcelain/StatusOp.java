/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api.porcelain;

import java.util.Iterator;
import java.util.List;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.plumbing.DiffIndex;
import org.geogit.api.plumbing.DiffWorkTree;
import org.geogit.api.plumbing.diff.DiffEntry;
import org.geogit.api.plumbing.merge.Conflict;
import org.geogit.api.plumbing.merge.ConflictsReadOp;
import org.geogit.di.CanRunDuringConflict;
import org.geogit.repository.StagingArea;
import org.geogit.repository.WorkingTree;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

@CanRunDuringConflict
public class StatusOp extends AbstractGeoGitOp<StatusSummary> {

    @Override
    public StatusSummary call() {
        WorkingTree workTree = getWorkTree();
        StagingArea index = getIndex();

        final long countStaged = index.countStaged(null).getCount();
        final int countConflicted = index.countConflicted(null);
        final long countUnstaged = workTree.countUnstaged(null).getCount();

        final Iterator<DiffEntry> empty = Iterators.<DiffEntry> emptyIterator();

        Supplier<Iterator<DiffEntry>> stagedEntries = Suppliers.ofInstance(empty);
        Supplier<Iterator<DiffEntry>> unstagedEntries = Suppliers.ofInstance(empty);

        List<Conflict> conflicts = ImmutableList.of();

        if (countStaged > 0) {
            stagedEntries = command(DiffIndex.class).setReportTrees(true);
        }
        if (countConflicted > 0) {
            conflicts = command(ConflictsReadOp.class).call();
        }
        if (countUnstaged > 0) {
            unstagedEntries = command(DiffWorkTree.class).setReportTrees(true);
        }
        StatusSummary summary = new StatusSummary();
        summary.setCountStaged(countStaged);
        summary.setCountUnstaged(countUnstaged);
        summary.setConflictsCount(countConflicted);
        summary.setStaged(stagedEntries);
        summary.setUnstaged(unstagedEntries);
        summary.setConflicts(conflicts);
        return summary;
    }
}
