/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.test.integration;

import org.geogit.api.Context;
import org.geogit.api.ContextBuilder;
import org.geogit.api.MemoryModule;
import org.geogit.api.Platform;
import org.geogit.di.GeogitModule;
import org.geogit.repository.Hints;

import com.google.inject.Guice;
import com.google.inject.util.Modules;

public class TestContextBuilder extends ContextBuilder {

    Platform platform;

    public TestContextBuilder(Platform platform) {
        this.platform = platform;
    }

    @Override
    public Context build(Hints hints) {
        return Guice.createInjector(
                Modules.override(new GeogitModule()).with(new MemoryModule(platform),
                        new HintsModule(hints))).getInstance(Context.class);
    }

}
