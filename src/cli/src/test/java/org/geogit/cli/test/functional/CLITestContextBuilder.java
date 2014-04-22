/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.cli.test.functional;

import org.geogit.api.Context;
import org.geogit.api.ContextBuilder;
import org.geogit.api.TestPlatform;
import org.geogit.cli.CLIContextBuilder;
import org.geogit.di.GeogitModule;
import org.geogit.di.PluginsModule;
import org.geogit.di.caching.CachingModule;
import org.geogit.repository.Hints;

import com.google.inject.Guice;
import com.google.inject.util.Modules;

public class CLITestContextBuilder extends ContextBuilder {

    private TestPlatform platform;

    public CLITestContextBuilder(TestPlatform platform) {
        this.platform = platform;
    }

    @Override
    public Context build(Hints hints) {
        FunctionalTestModule functionalTestModule = new FunctionalTestModule(platform.clone());

        Context context = Guice.createInjector(
                Modules.override(new GeogitModule()).with(new PluginsModule(),
                        new CLIContextBuilder.DefaultPlugins(), functionalTestModule,
                        new HintsModule(hints), new CachingModule())).getInstance(Context.class);
        return context;
    }

}
