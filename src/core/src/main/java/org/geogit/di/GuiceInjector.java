/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.di;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.Context;
import org.geogit.api.Platform;
import org.geogit.repository.Repository;
import org.geogit.repository.StagingArea;
import org.geogit.repository.WorkingTree;
import org.geogit.storage.ConfigDatabase;
import org.geogit.storage.DeduplicationService;
import org.geogit.storage.GraphDatabase;
import org.geogit.storage.ObjectDatabase;
import org.geogit.storage.RefDatabase;
import org.geogit.storage.StagingDatabase;

import com.google.inject.Inject;
import com.google.inject.Provider;

/**
 * Provides a method for finding and creating instances of GeoGit operations.
 * 
 * @see Context
 * @see AbstractGeoGitOp
 */
public class GuiceInjector implements Context {

    private com.google.inject.Injector guiceInjector;

    /**
     * Constructs a new {@code GuiceCommandLocator} with the given {@link Context}.
     * 
     * @param injector the injector which has commands bound to it
     */
    @Inject
    public GuiceInjector(com.google.inject.Injector injector) {
        this.guiceInjector = injector;
    }

    /**
     * Finds and returns an instance of a command of the specified class.
     * 
     * @param commandClass the kind of command to locate and instantiate
     * @return a new instance of the requested command class, with its dependencies resolved
     */
    @Override
    public <T extends AbstractGeoGitOp<?>> T command(Class<T> commandClass) {
        T command = getInstance(commandClass);
        command.setContext(this);
        command = getDecoratedInstance(command);
        return command;
    }

    private <T> T getInstance(final Class<T> type) {
        Provider<T> provider = guiceInjector.getProvider(type);
        T instance = provider.get();
        return instance;
    }

    private <T> T getDecoratedInstance(final Class<T> type) {
        T undecorated = getInstance(type);
        return getDecoratedInstance(undecorated);
    }

    private <T> T getDecoratedInstance(T undecorated) {
        DecoratorProvider decoratorProvider = guiceInjector.getInstance(DecoratorProvider.class);
        T decoratedInstance = decoratorProvider.get(undecorated);
        return decoratedInstance;
    }

    @Override
    public WorkingTree workingTree() {
        return getDecoratedInstance(WorkingTree.class);
    }

    @Override
    public StagingArea index() {
        return getDecoratedInstance(StagingArea.class);
    }

    @Override
    public RefDatabase refDatabase() {
        return getDecoratedInstance(RefDatabase.class);
    }

    @Override
    public Platform platform() {
        return getDecoratedInstance(Platform.class);
    }

    @Override
    public ObjectDatabase objectDatabase() {
        return getDecoratedInstance(ObjectDatabase.class);
    }

    @Override
    public StagingDatabase stagingDatabase() {
        return getDecoratedInstance(StagingDatabase.class);
    }

    @Override
    public ConfigDatabase configDatabase() {
        return getDecoratedInstance(ConfigDatabase.class);
    }

    @Override
    public GraphDatabase graphDatabase() {
        return getDecoratedInstance(GraphDatabase.class);
    }

    @Deprecated
    @Override
    public Repository repository() {
        return getDecoratedInstance(Repository.class);
    }

    @Override
    public DeduplicationService deduplicationService() {
        return getDecoratedInstance(DeduplicationService.class);
    }

    @Override
    public PluginDefaults pluginDefaults() {
        return getDecoratedInstance(PluginDefaults.class);
    }
}
