/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.test.integration.sqlite;

import org.geogit.api.Platform;
import org.geogit.api.TestPlatform;
import org.geogit.di.GeogitModule;
import org.geogit.storage.sqlite.Xerial;
import org.geogit.storage.sqlite.XerialSQLiteModule;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;

/**
 * Test utility class.
 * 
 * @author Justin Deoliveira, Boundless
 * 
 */
public class XerialTests {

    /**
     * Creates the injector to enable xerial sqlite storage.
     */
    public static Injector injector(final TestPlatform platform) {
        return Guice.createInjector(Modules.override(new GeogitModule()).with(
                new XerialSQLiteModule(), new AbstractModule() {
                    @Override
                    protected void configure() {
                        Xerial.turnSynchronizationOff();
                        bind(Platform.class).toInstance(platform);
                    }
                }));
    }
}
