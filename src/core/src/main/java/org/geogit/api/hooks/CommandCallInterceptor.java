/* Copyright (c) 2011 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the LGPL 2.1 license, available at the root
 * application directory.
 */
package org.geogit.api.hooks;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.plumbing.ResolveGeogitDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;

/**
 * An interceptor for the call() method in GeoGit operations that allow hooks
 * 
 */
public class CommandCallInterceptor implements MethodInterceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandCallInterceptor.class);

    public enum INSTANT {
        PRE, POST
    };

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        AbstractGeoGitOp<?> operation = (AbstractGeoGitOp<?>) invocation.getThis();
        runHook(operation, INSTANT.PRE);
        Object ret = invocation.proceed();
        try {
            runHook(operation, INSTANT.POST);
        } catch (CannotRunGeogitOperationException e) {
            // this exception should not be thrown in a post-execution hook, but just in case, we
            // swallow it and ignore it
            LOGGER.warn(
                    "Post-command hook for command {} threw an exception that will not be propagated",
                    operation.getClass().getName(), e);
        }
        return ret;

    }

    private void runHook(AbstractGeoGitOp<?> operation, INSTANT instant)
            throws CannotRunGeogitOperationException {
        Optional<File> hook = getHook(operation, instant);
        if (hook.isPresent()) {
            Scripting.executeScript(hook.get(), operation);
        }
    }

    /**
     * Returns the hook file corresponding to a given operation and instant, in case it exists in
     * the current GeoGit hooks folder If several hooks files exist for the specified operation and
     * instant, the first one that is found is returned.
     * 
     * @param operation the operation that triggers the hook
     * @param instant the instant
     * @return the hook file
     */
    @SuppressWarnings("unchecked")
    public Optional<File> getHook(AbstractGeoGitOp<?> operation, INSTANT instant) {

        Optional<URL> url = operation.command(ResolveGeogitDir.class).call();
        if (!url.isPresent() || !"file".equals(url.get().getProtocol())) {
            // Hooks not in a filesystem are not supported
            return Optional.absent();
        }
        File repoDir;
        try {
            repoDir = new File(url.get().toURI());
        } catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }
        File hooks = new File(repoDir, "hooks");
        if (!hooks.exists()) {
            return Optional.absent();
        }

        Class<? extends AbstractGeoGitOp<?>> clazz = (Class<? extends AbstractGeoGitOp<?>>) operation
                .getClass().getSuperclass();
        Optional<String> name = Hookables.getFilename(clazz);
        if (name.isPresent()) {
            String hookName = new String(instant.name() + "_" + name.get()).toLowerCase();
            File[] files = hooks.listFiles();
            for (File file : files) {
                String filename = file.getName();
                if (hookName.equals(filename) || filename.startsWith(hookName + ".")) {
                    if (!filename.endsWith("sample")) {
                        return Optional.of(file);
                    }
                }
            }
        }

        return Optional.absent();

    }

}
