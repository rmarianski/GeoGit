/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.cli.test.functional;

import org.geogit.api.InjectorBuilder;
import org.geogit.api.TestPlatform;
import org.geogit.cli.CLIInjectorBuilder;
import org.geogit.di.GeogitModule;
import org.geogit.di.PluginsModule;
import org.geogit.di.caching.CachingModule;
import org.geogit.repository.Hints;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;

public class CLITestInjectorBuilder extends InjectorBuilder {

    private TestPlatform platform;

    public CLITestInjectorBuilder(TestPlatform platform) {
        this.platform = platform;
    }

    @Override
    public Injector build(Hints hints) {
        FunctionalTestModule functionalTestModule = new FunctionalTestModule(platform.clone());

        Injector injector = Guice.createInjector(Modules.override(new GeogitModule()).with(
                new PluginsModule(), new CLIInjectorBuilder.DefaultPlugins(), functionalTestModule,
                new HintsModule(hints), new CachingModule()));
        return injector;
    }

}
