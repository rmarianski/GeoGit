/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.integration.mongo;

import org.geogit.api.Context;
import org.geogit.di.GeogitModule;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.util.Modules;

public class MongoRevTreeBuilderTest extends
        org.geogit.test.integration.RevTreeBuilderTest {
    @Override
    protected Context createInjector() {
        return Guice.createInjector(
                Modules.override(new GeogitModule()).with(new MongoTestStorageModule()))
                .getInstance(Context.class);
    }

    @Test
    // $codepro.audit.disable unnecessaryOverride
    public void testPutIterate() throws Exception {
        super.testPutIterate();
    }

    @Test
    // $codepro.audit.disable unnecessaryOverride
    public void testPutRandomGet() throws Exception {
        super.testPutRandomGet();
    }

    public static void main(String... args) {
        MongoRevTreeBuilderTest test = new MongoRevTreeBuilderTest();
        try {
            test.setUp();
            test.testPutRandomGet();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }
}
