/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.test.integration.je.repository;

import org.geogit.api.Context;
import org.geogit.di.GeogitModule;
import org.geogit.test.integration.je.JETestStorageModule;

import com.google.inject.Guice;
import com.google.inject.util.Modules;

public class JEIndexTest extends org.geogit.test.integration.repository.IndexTest {
    @Override
    protected Context createInjector() {
        return Guice.createInjector(
                Modules.override(new GeogitModule()).with(new JETestStorageModule())).getInstance(
                Context.class);
    }
}
