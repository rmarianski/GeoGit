/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.integration.mongo;

import java.io.File;

import org.geogit.api.Context;
import org.geogit.api.Platform;
import org.geogit.api.TestPlatform;
import org.geogit.di.GeogitModule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.util.Modules;

public class MongoCommitOpTest extends org.geogit.test.integration.CommitOpTest {
    @Rule
    public TemporaryFolder mockWorkingDirTempFolder = new TemporaryFolder();

    @Override
    protected Context createInjector() {
        File workingDirectory;
        try {
            workingDirectory = mockWorkingDirTempFolder.getRoot();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        Platform testPlatform = new TestPlatform(workingDirectory);
        return Guice.createInjector(
                Modules.override(new GeogitModule()).with(new MongoTestStorageModule(),
                        new TestModule(testPlatform))).getInstance(Context.class);
    }

}
