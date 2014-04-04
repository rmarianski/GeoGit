package org.geogit.test.integration;

import java.io.File;

import org.geogit.api.RevCommit;
import org.geogit.api.hooks.CannotRunGeogitOperationException;
import org.geogit.api.hooks.Scripting;
import org.geogit.api.porcelain.CommitOp;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class HooksTest extends RepositoryTestCase {

    @Override
    protected void setUpInternal() throws Exception {
        File hooksFolder = new File(geogit.getPlatform().pwd(), ".geogit/hooks");
        File[] files = hooksFolder.listFiles();
        for (File file : files) {
            file.delete();
            assertFalse(file.exists());
        }
    }

    @Test
    public void testHookWithError() throws Exception {
        CharSequence wrongHookCode = "this is a syntactically wrong sentence";
        File hooksFolder = new File(geogit.getPlatform().pwd(), ".geogit/hooks");
        File commitPreHookFile = new File(hooksFolder, "pre_commit.js");

        Files.write(wrongHookCode, commitPreHookFile, Charsets.UTF_8);

        insertAndAdd(points1);
        geogit.command(CommitOp.class).setMessage("A message").call();

    }

    @Test
    public void testHook() throws Exception {
        // a hook that only accepts commit messages longer with at least 4 words, and converts
        // message to lower case
        CharSequence commitPreHookCode = "exception = Packages.org.geogit.api.hooks.CannotRunGeogitOperationException;\n"
                + "msg = params.get(\"message\");\n"
                + "if (msg.length() < 30){\n"
                + "\tthrow new exception(\"Commit messages must have at least 30 letters\");\n}"
                + "params.put(\"message\", msg.toLowerCase());";

        File hooksFolder = new File(geogit.getPlatform().pwd(), ".geogit/hooks");
        File commitPreHookFile = new File(hooksFolder, "pre_commit.js");

        Files.write(commitPreHookCode, commitPreHookFile, Charsets.UTF_8);

        insertAndAdd(points1);
        try {
            geogit.command(CommitOp.class).setMessage("A short message").call();
            fail();
        } catch (Exception e) {
            assertEquals("Commit messages must have at least 30 letters", e.getMessage());
        }

        String longMessage = "THIS IS A LONG UPPERCASE COMMIT MESSAGE";
        RevCommit commit = geogit.command(CommitOp.class).setMessage(longMessage).call();
        assertEquals(longMessage.toLowerCase(), commit.getMessage());

    }

    @Test
    public void testExecutableScriptFileHook() throws Exception {
        File hooksFolder = new File(geogit.getPlatform().pwd(), ".geogit/hooks");
        File commitPreHookFile;
        String commitPreHookCode;
        // a hook that returns non-zero
        if (Scripting.isWindows()) {
            commitPreHookCode = "exit 1";
        } else {
            commitPreHookCode = "#!/bin/sh\nexit 1";
        }
        commitPreHookFile = new File(hooksFolder, "pre_commit.bat");
        Files.write(commitPreHookCode, commitPreHookFile, Charsets.UTF_8);
        commitPreHookFile.setExecutable(true);

        insertAndAdd(points1);
        try {
            geogit.command(CommitOp.class).setMessage("Message").call();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof CannotRunGeogitOperationException);
        }

        // a hook that returns zero
        if (Scripting.isWindows()) {
            commitPreHookCode = "exit 0";
        } else {
            commitPreHookCode = "#!/bin/sh\nexit 0";
        }
        commitPreHookFile = new File(hooksFolder, "pre_commit.bat");
        Files.write(commitPreHookCode, commitPreHookFile, Charsets.UTF_8);
        commitPreHookFile.setExecutable(true);

        geogit.command(CommitOp.class).setMessage("Message").call();

    }

    @Test
    public void testFailingPostPostProcessHook() throws Exception {
        CharSequence postHookCode = "exception = Packages.org.geogit.api.hooks.CannotRunGeogitOperationException;\n"
                + "throw new exception();";
        File hooksFolder = new File(geogit.getPlatform().pwd(), ".geogit/hooks");
        File commitPostHookFile = new File(hooksFolder, "post_commit.js");

        Files.write(postHookCode, commitPostHookFile, Charsets.UTF_8);

        insertAndAdd(points1);
        geogit.command(CommitOp.class).setMessage("A message").call();

    }

}
