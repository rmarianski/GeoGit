/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.test.integration.sqlite;

import static org.geogit.test.integration.sqlite.XerialTests.injector;

import org.geogit.api.Context;
import org.geogit.api.TestPlatform;
import org.geogit.test.integration.DiffOpTest;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;


public class XerialDiffOpTest extends DiffOpTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Override
    protected Context createInjector() {
        return injector(new TestPlatform(temp.getRoot()));
    }
}
