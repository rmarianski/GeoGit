/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api.porcelain;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.geogit.api.plumbing.diff.DiffEntry;
import org.geogit.api.plumbing.merge.Conflict;

import com.google.common.base.Supplier;

public class StatusSummary {

    private List<Conflict> conflicts;

    Supplier<Iterator<DiffEntry>> staged;

    private Supplier<Iterator<DiffEntry>> unstaged;

    private long countStaged, countUnstaged;

    private int countConflicts;

    public List<Conflict> getConflicts() {
        return conflicts;
    }

    void setConflicts(List<Conflict> conflicts) {
        this.conflicts = conflicts;
    }

    public Supplier<Iterator<DiffEntry>> getStaged() {
        return staged;
    }

    void setStaged(Supplier<Iterator<DiffEntry>> stagedEntries) {
        this.staged = stagedEntries;
    }

    public Supplier<Iterator<DiffEntry>> getUnstaged() {
        return unstaged;
    }

    void setUnstaged(Supplier<Iterator<DiffEntry>> unstagedEntries) {
        this.unstaged = unstagedEntries;
    }

    public StatusSummary() {
        this.conflicts = new ArrayList<Conflict>();
        this.staged = null;
        this.unstaged = null;

    }

    public long getCountStaged() {
        return countStaged;
    }

    void setCountStaged(long countStaged) {
        this.countStaged = countStaged;
    }

    public long getCountUnstaged() {
        return countUnstaged;
    }

    void setCountUnstaged(long unstagedCount) {
        this.countUnstaged = unstagedCount;
    }

    public int getCountConflicts() {
        return countConflicts;
    }

    void setConflictsCount(int conflictsCount) {
        this.countConflicts = conflictsCount;
    }

}
