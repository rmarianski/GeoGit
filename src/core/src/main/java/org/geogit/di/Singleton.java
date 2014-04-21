package org.geogit.di;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;

/**
 * Identifies a type that the injector only instantiates once. Not inherited.
 */
@Documented
@Retention(RUNTIME)
@Inherited
public @interface Singleton {
}