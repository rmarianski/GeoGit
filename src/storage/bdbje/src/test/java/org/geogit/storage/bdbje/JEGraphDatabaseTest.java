/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.storage.bdbje;

import java.io.File;

import org.geogit.api.Platform;
import org.geogit.api.TestPlatform;
import org.geogit.storage.ConfigDatabase;
import org.geogit.storage.GraphDatabase;
import org.geogit.storage.GraphDatabaseTest;
import org.geogit.storage.fs.IniFileConfigDatabase;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class JEGraphDatabaseTest extends GraphDatabaseTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    // instance variable so its reused as if it were the singleton in the guice config
    private EnvironmentBuilder envProvider;

    @Override
    protected GraphDatabase createDatabase(Platform platform) throws Exception {
        File root = folder.getRoot();
        folder.newFolder(".geogit");
        File home = folder.newFolder("home");
        this.platform = new TestPlatform(root);
        this.platform.setUserHome(home);

        envProvider = new EnvironmentBuilder(platform);

        ConfigDatabase configDB = new IniFileConfigDatabase(platform);
        return new JEGraphDatabase(configDB, envProvider);
    }
}