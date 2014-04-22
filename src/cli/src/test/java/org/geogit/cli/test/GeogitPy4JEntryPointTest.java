package org.geogit.cli.test;

import static org.geogit.cli.test.functional.GlobalState.geogitCLI;
import static org.geogit.cli.test.functional.GlobalState.insert;
import static org.geogit.cli.test.functional.GlobalState.platform;
import static org.geogit.cli.test.functional.GlobalState.setupGeogit;
import static org.geogit.cli.test.functional.GlobalState.tempFolder;
import static org.geogit.cli.test.functional.TestFeatures.points1;
import static org.geogit.cli.test.functional.TestFeatures.points1_modified;
import static org.geogit.cli.test.functional.TestFeatures.points2;
import static org.geogit.cli.test.functional.TestFeatures.points3;
import static org.geogit.cli.test.functional.TestFeatures.setupFeatures;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.geogit.api.GlobalContextBuilder;
import org.geogit.api.TestPlatform;
import org.geogit.api.porcelain.AddOp;
import org.geogit.api.porcelain.CommitOp;
import org.geogit.cli.GeogitPy4JEntryPoint;
import org.geogit.cli.test.functional.CLITestContextBuilder;
import org.geogit.cli.test.functional.GlobalState;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import py4j.GatewayServer;

public class GeogitPy4JEntryPointTest {

    @Before
    public void setUpDirectories() throws IOException {
        tempFolder = new TemporaryFolder();
        tempFolder.create();
        File homeDirectory = tempFolder.newFolder("fakeHomeDir").getCanonicalFile();
        File currentDirectory = tempFolder.newFolder("testrepo").getCanonicalFile();
        GlobalState.platform = new TestPlatform(currentDirectory, homeDirectory);
        GlobalContextBuilder.builder = new CLITestContextBuilder(platform);
    }

    @Test
    public void testPy4JentryPoint() throws Exception {
        setupGeogit();
        setupFeatures();
        String repoFolder = platform.pwd().getAbsolutePath();
        GeogitPy4JEntryPoint py4j = new GeogitPy4JEntryPoint();
        GatewayServer gatewayServer = new GatewayServer(py4j);
        gatewayServer.start();
        py4j.runCommand(repoFolder, new String[] { "init" });
        py4j.runCommand(repoFolder, "config user.name name".split(" "));
        py4j.runCommand(repoFolder, "config user.email email@email.com".split(" "));
        insert(points1);
        insert(points2);
        insert(points3);
        geogitCLI.getGeogit().command(AddOp.class).call();
        geogitCLI.getGeogit().command(CommitOp.class).setMessage("message").call();
        py4j.runCommand(repoFolder, new String[] { "log" });
        String output = py4j.nextOutputPage();
        assertTrue(output.contains("message"));
        assertTrue(output.contains("name"));
        assertTrue(output.contains("email@email.com"));
        insert(points1_modified);
        py4j.runCommand(repoFolder, new String[] { "add" });
        py4j.runCommand(repoFolder, new String[] { "commit", "-m", "a commit message" });
        py4j.runCommand(repoFolder, new String[] { "log" });
        output = py4j.nextOutputPage();
        System.out.println(output);
        assertTrue(output.contains("a commit message"));

        gatewayServer.shutdown();
    }
}
