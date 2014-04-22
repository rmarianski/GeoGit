/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geogit.repository.Repository;
import org.geogit.repository.StagingArea;
import org.geogit.repository.WorkingTree;
import org.geogit.storage.ConfigDatabase;
import org.geogit.storage.GraphDatabase;
import org.geogit.storage.ObjectDatabase;
import org.geogit.storage.RefDatabase;
import org.geogit.storage.StagingDatabase;

/**
 * Provides a base implementation for internal GeoGit operations.
 * 
 * @param <T> the type of the result of the execution of the command
 */
public abstract class AbstractGeoGitOp<T> {

    private static final ProgressListener NULL_PROGRESS_LISTENER = new DefaultProgressListener();

    private ProgressListener progressListener = NULL_PROGRESS_LISTENER;

    private List<CommandListener> listeners;

    protected Injector injector;

    private Map<Serializable, Serializable> metadata;

    public static interface CommandListener {
        public void preCall(AbstractGeoGitOp<?> command);

        public void postCall(AbstractGeoGitOp<?> command, Object result, boolean success);
    }

    /**
     * Constructs a new abstract operation.
     */
    public AbstractGeoGitOp() {
        //
    }

    public void addListener(CommandListener l) {
        if (listeners == null) {
            listeners = new ArrayList<AbstractGeoGitOp.CommandListener>(2);
        }
        listeners.add(l);
    }

    /**
     * @return a content holder for client code data that can be used by decorators/interceptors
     */
    public Map<Serializable, Serializable> getClientData() {
        if (metadata == null) {
            metadata = new HashMap<Serializable, Serializable>();
        }
        return metadata;
    }

    /**
     * Finds and returns an instance of a command of the specified class.
     * 
     * @param commandClass the kind of command to locate and instantiate
     * @return a new instance of the requested command class, with its dependencies resolved
     */
    public <C extends AbstractGeoGitOp<?>> C command(Class<C> commandClass) {
        return injector.command(commandClass);
    }

    /**
     * @param locator the command locator to use when finding commands
     */
    public AbstractGeoGitOp<?> setInjector(Injector locator) {
        this.injector = locator;
        return this;
    }

    /**
     * @param listener the progress listener to use
     * @return {@code this}
     */
    public AbstractGeoGitOp<T> setProgressListener(final ProgressListener listener) {
        this.progressListener = listener == null ? NULL_PROGRESS_LISTENER : listener;
        return this;
    }

    /**
     * @return the progress listener that is currently set
     */
    protected ProgressListener getProgressListener() {
        return progressListener;
    }

    /**
     * Constructs a new progress listener based on a specified sub progress amount.
     * 
     * @param amount amount of progress
     * @return the newly constructed progress listener
     */
    protected ProgressListener subProgress(float amount) {
        return new SubProgressListener(getProgressListener(), amount);
    }

    /**
     * Subclasses shall implement to do the real work.
     * 
     * @see java.util.concurrent.Callable#call()
     */
    public final T call() {
        notifyPre();
        try {
            T result = _call();
            notifyPost(result, true);
            return result;
        } catch (RuntimeException e) {
            notifyPost(null, false);
            throw e;
        }
    }

    protected abstract T _call();

    private void notifyPre() {
        if (listeners == null) {
            return;
        }
        for (CommandListener l : listeners) {
            l.preCall(this);
        }
    }

    private void notifyPost(T result, boolean success) {
        if (listeners == null) {
            return;
        }
        for (CommandListener l : listeners) {
            l.postCall(this, result, success);
        }
    }


    /**
     * Shortcut for {@link Injector#workingTree() getCommandLocator().getWorkingTree()}
     */
    protected WorkingTree workingTree() {
        return injector.workingTree();
    }

    /**
     * Shortcut for {@link Injector#index() getCommandLocator().getIndex()}
     */
    protected StagingArea index() {
        return injector.index();
    }

    /**
     * Shortcut for {@link Injector#refDatabase() getCommandLocator().getRefDatabase()}
     */
    protected RefDatabase refDatabase() {
        return injector.refDatabase();
    }

    protected Platform platform() {
        return injector.platform();
    }

    protected ObjectDatabase objectDatabase() {
        return injector.objectDatabase();
    }

    protected StagingDatabase stagingDatabase() {
        return injector.stagingDatabase();
    }

    protected ConfigDatabase configDatabase() {
        return injector.configDatabase();
    }

    protected GraphDatabase graphDatabase() {
        return injector.graphDatabase();
    }

    public Repository repository() {
        return injector.repository();
    }
}
