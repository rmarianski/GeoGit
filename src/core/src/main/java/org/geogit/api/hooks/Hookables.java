/* Copyright (c) 2011 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the LGPL 2.1 license, available at the root
 * application directory.
 */
package org.geogit.api.hooks;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.geogit.api.AbstractGeoGitOp;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.matcher.Matcher;
import com.google.inject.matcher.Matchers;

/**
 * A class for managing GeoGit operations that can be hooked and the filenames of the corresponding
 * hooks. It also includes additional related utilities.
 * 
 */
public class Hookables {

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
    public static Optional<String> getFilename(Class<? extends AbstractGeoGitOp<?>> clazz) {
        Hookable annotation = clazz.getAnnotation(Hookable.class);
        if (annotation != null) {
            return Optional.of(annotation.name());
        } else {
            return Optional.absent();
        }

    }

}
