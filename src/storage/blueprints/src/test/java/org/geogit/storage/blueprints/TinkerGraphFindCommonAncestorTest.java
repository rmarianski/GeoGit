/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.blueprints;

import org.geogit.api.Context;
import org.geogit.di.GeogitModule;

import com.google.inject.Guice;
import com.google.inject.util.Modules;

public class TinkerGraphFindCommonAncestorTest extends
        org.geogit.test.integration.FindCommonAncestorTest {
    @Override
    protected Context createInjector() {
        return Guice.createInjector(
                Modules.override(new GeogitModule()).with(new TinkerGraphTestModule()))
                .getInstance(Context.class);
    }
}
