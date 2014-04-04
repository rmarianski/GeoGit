/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api.plumbing.merge;

import java.net.URL;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.plumbing.ResolveGeogitDir;

import com.google.common.base.Optional;

public class ConflictsCheckOp extends AbstractGeoGitOp<Boolean> {
    @Override
    public Boolean call() {
        final Optional<URL> repoUrl = getCommandLocator().command(ResolveGeogitDir.class).call();
        Boolean hasConflicts = Boolean.FALSE;

        if (repoUrl.isPresent()) {
            boolean conflicts = getIndex().getDatabase().hasConflicts(null);
            hasConflicts = Boolean.valueOf(conflicts);
        }
        return hasConflicts;
    }
}
