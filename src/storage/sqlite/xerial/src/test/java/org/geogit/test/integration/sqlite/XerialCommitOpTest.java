/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.test.integration.sqlite;

import static org.geogit.test.integration.sqlite.XerialTests.injector;

import org.geogit.api.Injector;
import org.geogit.api.TestPlatform;
import org.geogit.test.integration.CommitOpTest;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;


public class XerialCommitOpTest extends CommitOpTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Override
    protected Injector createInjector() {
        return injector(new TestPlatform(temp.getRoot()));
    }
}
