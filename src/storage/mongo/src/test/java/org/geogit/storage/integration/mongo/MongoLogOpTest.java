/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.integration.mongo;

import org.geogit.api.Context;
import org.geogit.di.GeogitModule;

import com.google.inject.Guice;
import com.google.inject.util.Modules;

public class MongoLogOpTest extends org.geogit.test.integration.LogOpTest {
    @Override
    protected Context createInjector() {
        return Guice.createInjector(
                Modules.override(new GeogitModule()).with(new MongoTestStorageModule()))
                .getInstance(Context.class);
    }
}
