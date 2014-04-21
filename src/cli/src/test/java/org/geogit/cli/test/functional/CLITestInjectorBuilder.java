/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.cli.test.functional;

import org.geogit.api.Injector;
import org.geogit.api.InjectorBuilder;
import org.geogit.api.TestPlatform;
import org.geogit.di.GeogitModule;
import org.geogit.repository.Hints;
import org.geogit.test.integration.je.JETestStorageModule;

import com.google.inject.Guice;
import com.google.inject.util.Modules;

public class CLITestInjectorBuilder extends InjectorBuilder {

    private TestPlatform platform;

    public CLITestInjectorBuilder(TestPlatform platform) {
        this.platform = platform;
    }

    @Override
    public Injector build(Hints hints) {
        JETestStorageModule jeStorageModule = new JETestStorageModule();
        FunctionalTestModule functionalTestModule = new FunctionalTestModule(platform.clone());

        Injector injector = Guice.createInjector(
                Modules.override(new GeogitModule()).with(jeStorageModule, functionalTestModule,
                        new HintsModule(hints))).getInstance(org.geogit.api.Injector.class);
        return injector;
    }

}
