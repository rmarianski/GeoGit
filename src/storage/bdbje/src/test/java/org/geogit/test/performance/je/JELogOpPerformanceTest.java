/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.test.performance.je;

import org.geogit.api.Context;
import org.geogit.di.GeogitModule;
import org.geogit.test.integration.je.JETestStorageModule;
import org.geogit.test.performance.LogOpPerformanceTest;

import com.google.inject.Guice;
import com.google.inject.util.Modules;

public class JELogOpPerformanceTest extends LogOpPerformanceTest {
    @Override
    protected Context createInjector() {
        return Guice.createInjector(
                Modules.override(new GeogitModule()).with(new JETestStorageModule())).getInstance(
                Context.class);
    }
}
