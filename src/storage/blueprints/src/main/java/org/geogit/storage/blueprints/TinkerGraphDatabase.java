/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.blueprints;

import org.geogit.api.Platform;
import org.geogit.repository.RepositoryConnectionException;
import org.geogit.storage.ConfigDatabase;
import org.geogit.storage.SynchronizedGraphDatabase;

import com.google.inject.Inject;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;

/**
 * Provides an implementation of a GeoGit Graph Database using TinkerGraph.
 */
public class TinkerGraphDatabase extends SynchronizedGraphDatabase {
    @Inject
    public TinkerGraphDatabase(Platform platform, ConfigDatabase configDB) {
        super(new Impl(platform, configDB));
    }

    static class Impl extends BlueprintsGraphDatabase<TinkerGraph> {
        private final ConfigDatabase configDB;

        /**
         * Constructs a new {@code TinkerGraphDatabase} using the given platform.
         * 
         * @param platform the platform to use.
         */
        public Impl(final Platform platform, final ConfigDatabase configDB) {
            super(platform);
            this.configDB = configDB;
        }

        @Override
        protected TinkerGraph getGraphDatabase() {
            TinkerGraph tinkerGraph = new TinkerGraph(dbPath, TinkerGraph.FileType.GML);

            // HACK: force the gml file to be created. If it didn't exist, the constructor created
            // the directory but not the file, and subsequent attempts to open the graph (from
            // another process) results in a FileNotFountException
            tinkerGraph.shutdown();
            tinkerGraph = new TinkerGraph(dbPath, TinkerGraph.FileType.GML);
            return tinkerGraph;
        }

        @Override
        public void configure() throws RepositoryConnectionException {
            RepositoryConnectionException.StorageType.GRAPH.configure(configDB, "tinkergraph",
                    "0.1");
        }

        @Override
        public void checkConfig() throws RepositoryConnectionException {
            RepositoryConnectionException.StorageType.GRAPH.verify(configDB, "tinkergraph", "0.1");
        }

        @Override
        protected void commit() {
            // shutDown() on a file based tinker graph only saves the graph to the file
            graphDB.shutdown();
        }

        @Override
        protected void rollback() {
            // no-op
        }
    }
}
