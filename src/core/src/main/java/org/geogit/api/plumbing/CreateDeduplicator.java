/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api.plumbing;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.storage.DeduplicationService;
import org.geogit.storage.Deduplicator;

public class CreateDeduplicator extends AbstractGeoGitOp<Deduplicator> {

    @Override
    protected  Deduplicator _call() {
        DeduplicationService deduplicationService;
        deduplicationService = context.deduplicationService();
        return deduplicationService.createDeduplicator();
    }
}
