/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api.plumbing;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.repository.Repository;

/**
 * Resolves the current repository
 * 
 */
public class ResolveRepository extends AbstractGeoGitOp<Repository> {

    @Override
    public Repository call() {
        return repository();
    }
}
