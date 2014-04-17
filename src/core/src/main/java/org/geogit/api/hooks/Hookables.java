/* Copyright (c) 2011 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the LGPL 2.1 license, available at the root
 * application directory.
 */
package org.geogit.api.hooks;

import java.io.File;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;

import javax.annotation.Nullable;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.plumbing.ResolveGeogitDir;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.matcher.Matcher;
import com.google.inject.matcher.Matchers;

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
     * returns a matcher that matches classes representing GeoGit operations that allow hooks
     * 
     * @return
     */
    public static Matcher<AnnotatedElement> classMatcher() {
        return Matchers.annotatedWith(Hookable.class);
        // return matcher;
    }

    /**
     * returns a matcher that matches the call method from a GeoGit operation, excluding synthetic
     * methods
     * 
     * @return
     */
    public static Matcher<? super Method> methodMatcher() {
        try {
            return new Matcher<Method>() {

                Method method = AbstractGeoGitOp.class.getMethod("call");

                @Override
                public boolean matches(Method t) {
                    if (!t.isSynthetic()) {
                        if (method.getName().equals(t.getName())) {
                            if (Arrays.equals(method.getParameterTypes(), t.getParameterTypes())) {
                                return true;
                            }
                        }
                    }
                    return false;
                }

                @Override
                public Matcher<Method> and(Matcher<? super Method> other) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Matcher<Method> or(Matcher<? super Method> other) {
                    throw new UnsupportedOperationException();
                }
            };
        } catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }

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
            if (hook.targetCommand().isAssignableFrom(clazz)) {
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
