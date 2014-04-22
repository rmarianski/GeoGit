/* Copyright (c) 2014 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.metrics;

import org.geogit.di.Decorator;
import org.geogit.repository.Repository;
import org.geogit.repository.Repository.RepositoryListener;

/**
 * Shuts down the metrics service at repository close() event
 */
class RepositoryDecorator implements Decorator {

    private HeapMemoryMetricsService service;

    private RepositoryListener listener;

    public RepositoryDecorator(HeapMemoryMetricsService service) {
        this.service = service;
    }

    @Override
    public boolean canDecorate(Object instance) {
        return instance instanceof Repository;
    }

    @Override
    public <I> I decorate(I subject) {
        if (listener == null) {
            listener = new RepositoryListener() {

                @Override
                public void opened(Repository repo) {
                    service.startAndWait();
                }

                @Override
                public void closed() {
                    service.stop();
                }
            };
            ((Repository) subject).addListener(listener);
        }
        return subject;
    }
}
