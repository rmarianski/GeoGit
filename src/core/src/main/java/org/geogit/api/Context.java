/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.api;

import org.geogit.api.porcelain.InitOp;
import org.geogit.di.PluginDefaults;
import org.geogit.repository.Repository;
import org.geogit.repository.StagingArea;
import org.geogit.repository.WorkingTree;
import org.geogit.storage.ConfigDatabase;
import org.geogit.storage.DeduplicationService;
import org.geogit.storage.GraphDatabase;
import org.geogit.storage.ObjectDatabase;
import org.geogit.storage.RefDatabase;
import org.geogit.storage.StagingDatabase;

/**
 * A context object for a single repository, provides access to the different repository objects,
 * and a factory method for commands.
 */
public interface Context {

    /**
     * Finds and returns an instance of a command of the specified class.
     * 
     * @param commandClass the kind of command to locate and instantiate
     * @return a new instance of the requested command class, with its dependencies resolved
     */
    public <T extends AbstractGeoGitOp<?>> T command(Class<T> commandClass);

    public WorkingTree workingTree();

    /**
     * @return
     */
    public StagingArea index();

    /**
     * @return
     */
    public RefDatabase refDatabase();

    public Platform platform();

    public ObjectDatabase objectDatabase();

    public StagingDatabase stagingDatabase();

    public ConfigDatabase configDatabase();

    public GraphDatabase graphDatabase();

    /**
     * @deprecated commands should not access the repository instance but from its components as
     *             given by the other methods in this interface
     */
    @Deprecated
    public Repository repository();

    public DeduplicationService deduplicationService();

    /**
     * @TODO find a better way of accessing plugins and defaults. This method is currently here for
     *       the sake of {@link InitOp} and to get rid of the {@code getInstance(Class anyClass)}
     *       method in Injector
     */
    public PluginDefaults pluginDefaults();

}