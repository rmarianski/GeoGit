/* Copyright (c) 2011 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the LGPL 2.1 license, available at the root
 * application directory.
 */
package org.geogit.api.hooks;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.ServiceLoader;

import javax.annotation.Nullable;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.plumbing.ResolveGeogitDir;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * A class for managing GeoGit operations that can be hooked and the filenames of the corresponding
 * hooks. It also includes additional related utilities.
 * 
 */
public class Hookables {

    private static final ImmutableList<CommandHook> classPathHooks;
    static {
        classPathHooks = Hookables.loadClasspathHooks();
    }

    /**
     * Returns the filename to be used for a script corresponding to the hook for a given GeoGit
     * operation. Returns {@link Optional.absent} if the specified operation does not allows hooks
     * 
     * @param class the operation
     * @return the string to be used as filename for storing the script files for the corresponding
     *         hook
     */
    public static Optional<String> getFilename(Class<? extends AbstractGeoGitOp> clazz) {
        Hookable annotation = clazz.getAnnotation(Hookable.class);
        if (annotation != null) {
            return Optional.of(annotation.name());
        } else {
            return Optional.absent();
        }
    }

    public static ImmutableList<CommandHook> loadClasspathHooks() {
        ServiceLoader<CommandHook> loader = ServiceLoader.load(CommandHook.class);
        ImmutableList<CommandHook> SPIHooks = ImmutableList.copyOf(loader.iterator());
        return SPIHooks;
    }

    public static boolean hasClasspathHooks(Class<? extends AbstractGeoGitOp<?>> commandClass) {
        for (CommandHook hook : classPathHooks) {
            if (hook.appliesTo(commandClass)) {
                return true;
            }
        }
        return false;
    }

    public static List<CommandHook> findHooksFor(AbstractGeoGitOp<?> operation) {

        final File hooksDir = findHooksDirectory(operation);
        if (hooksDir == null) {
            return ImmutableList.of();
        }

        final Class<? extends AbstractGeoGitOp> clazz = operation.getClass();
        final Optional<String> name = Hookables.getFilename(clazz);
        if (!name.isPresent()) {
            return ImmutableList.of();
        }

        List<CommandHook> hooks = Lists.newLinkedList();
        for (CommandHook hook : classPathHooks) {
            if (hook.appliesTo(clazz)) {
                hooks.add(hook);
            }
        }
        if (name.isPresent()) {
            String preHookName = "pre_" + name.get().toLowerCase();
            String postHookName = "post_" + name.get().toLowerCase();
            File[] files = hooksDir.listFiles();
            for (File file : files) {
                String filename = file.getName();
                if (isHook(filename, preHookName)) {
                    hooks.add(Scripting.createScriptHook(file, true));
                }
                if (isHook(filename, postHookName)) {
                    hooks.add(Scripting.createScriptHook(file, false));
                }
            }
        }
        return hooks;

    }

    @Nullable
    private static File findHooksDirectory(AbstractGeoGitOp<?> operation) {
        Optional<URL> url = operation.command(ResolveGeogitDir.class).call();
        if (!url.isPresent() || !"file".equals(url.get().getProtocol())) {
            // Hooks not in a filesystem are not supported
            return null;
        }
        File repoDir;
        try {
            repoDir = new File(url.get().toURI());
        } catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }
        File hooksDir = new File(repoDir, "hooks");
        if (!hooksDir.exists()) {
            return null;
        }
        return hooksDir;
    }

    private static boolean isHook(final String filename, final String hookNamePrefix) {
        if (hookNamePrefix.equals(filename) || filename.startsWith(hookNamePrefix + ".")) {
            if (!filename.endsWith("sample")) {
                return true;
            }
        }
        return false;
    }

}
