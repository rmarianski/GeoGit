/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.test.integration.je;

import org.geogit.di.GeogitModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;

public class JEFindCommonAncestorTest extends org.geogit.test.integration.FindCommonAncestorTest {
    @Override
    protected Injector createInjector() {
        return Guice.createInjector(Modules.override(new GeogitModule()).with(
                new JETestStorageModule()));
    }
}
