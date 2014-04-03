/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.cli.porcelain;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import jline.console.ConsoleReader;

import org.geogit.api.ObjectId;
import org.geogit.api.plumbing.diff.DiffEntry;
import org.geogit.api.plumbing.diff.DiffEntry.ChangeType;
import org.geogit.api.porcelain.DiffOp;
import org.geogit.api.porcelain.FetchResult.ChangedRef;
import org.geogit.api.porcelain.FetchResult.ChangedRef.ChangeTypes;
import org.geogit.api.porcelain.PullOp;
import org.geogit.api.porcelain.PullResult;
import org.geogit.api.porcelain.SynchronizationException;
import org.geogit.cli.AbstractCommand;
import org.geogit.cli.CLICommand;
import org.geogit.cli.CommandFailedException;
import org.geogit.cli.GeogitCLI;
import org.geogit.cli.annotation.RemotesReadOnly;
import org.geogit.cli.annotation.StagingDatabaseReadOnly;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * Incorporates changes from a remote repository into the current branch.
 * <p>
 * More precisely, {@code geogit pull} runs {@code geogit fetch} with the given parameters and calls
 * {@code geogit merge} to merge the retrieved branch heads into the current branch. With
 * {@code --rebase}, it runs {@code geogit rebase} instead of {@code geogit merge}.
 * <p>
 * CLI proxy for {@link PullOp}
 * <p>
 * Usage:
 * <ul>
 * <li> {@code geogit pull [options] [<repository> [<refspec>...]]}
 * </ul>
 * 
 * @see PullOp
 */
@RemotesReadOnly
@Parameters(commandNames = "pull", commandDescription = "Fetch from and merge with another repository or a local branch")
public class Pull extends AbstractCommand implements CLICommand {

    @Parameter(names = "--all", description = "Fetch all remotes.")
    private boolean all = false;

    @Parameter(names = "--rebase", description = "Rebase the current branch on top of the upstream branch after fetching.")
    private boolean rebase = false;

    @Parameter(names = { "--depth" }, description = "Depth of the pull.")
    private int depth = 0;

    @Parameter(names = { "--fulldepth" }, description = "Pull the full history from the repository.")
    private boolean fulldepth = false;

    @Parameter(description = "[<repository> [<refspec>...]]")
    private List<String> args;

    /**
     * Executes the pull command using the provided options.
     */
    @Override
    public void runInternal(GeogitCLI cli) throws IOException {
        checkParameter(depth > 0 ? !fulldepth : true,
                "Cannot specify a depth and full depth.  Use --depth <depth> or --fulldepth.");

        if (depth > 0 || fulldepth) {
            if (!cli.getGeogit().getRepository().getDepth().isPresent()) {
                throw new CommandFailedException(
                        "Depth operations can only be used on a shallow clone.");
            }
        }

        PullOp pull = cli.getGeogit().command(PullOp.class);
        pull.setProgressListener(cli.getProgressListener());
        pull.setAll(all).setRebase(rebase).setFullDepth(fulldepth);
        pull.setDepth(depth);

        if (args != null) {
            if (args.size() > 0) {
                pull.setRemote(args.get(0));
            }
            for (int i = 1; i < args.size(); i++) {
                pull.addRefSpec(args.get(i));
            }
        }

        try {
            PullResult result = pull.call();

            ConsoleReader console = cli.getConsole();

            for (Entry<String, List<ChangedRef>> entry : result.getFetchResult().getChangedRefs()
                    .entrySet()) {
                console.println("From " + entry.getKey());

                for (ChangedRef ref : entry.getValue()) {
                    String line;
                    if (ref.getType() == ChangeTypes.CHANGED_REF) {
                        line = "   " + ref.getOldRef().getObjectId().toString().substring(0, 7)
                                + ".." + ref.getNewRef().getObjectId().toString().substring(0, 7)
                                + "     " + ref.getNewRef().localName() + " -> "
                                + ref.getNewRef().getName();
                    } else if (ref.getType() == ChangeTypes.ADDED_REF) {
                        line = " * [new branch]     " + ref.getNewRef().localName() + " -> "
                                + ref.getNewRef().getName();
                    } else if (ref.getType() == ChangeTypes.REMOVED_REF) {
                        line = " x [deleted]        (none) -> " + ref.getOldRef().getName();
                    } else {
                        line = "   [deepened]       " + ref.getNewRef().localName();
                    }
                    console.println(line);
                }
            }

            if (result.getOldRef() != null && result.getNewRef() != null
                    && result.getOldRef().equals(result.getNewRef())) {
                console.println("Already up to date.");
            } else if (result.getOldRef() == null && result.getNewRef() == null) {
                console.println("Nothing to pull.");
            } else {
                Iterator<DiffEntry> iter;
                if (result.getOldRef() == null) {
                    console.println("From " + result.getRemoteName());
                    console.println(" * [new branch]     " + result.getNewRef().localName()
                            + " -> " + result.getNewRef().getName());

                    iter = cli.getGeogit().command(DiffOp.class)
                            .setNewVersion(result.getNewRef().getObjectId())
                            .setOldVersion(ObjectId.NULL).call();
                } else {
                    iter = cli.getGeogit().command(DiffOp.class)
                            .setNewVersion(result.getNewRef().getObjectId())
                            .setOldVersion(result.getOldRef().getObjectId()).call();
                }

                int added = 0;
                int removed = 0;
                int modified = 0;
                while (iter.hasNext()) {
                    DiffEntry entry = iter.next();
                    if (entry.changeType() == ChangeType.ADDED) {
                        added++;
                    } else if (entry.changeType() == ChangeType.MODIFIED) {
                        modified++;
                    } else if (entry.changeType() == ChangeType.REMOVED) {
                        removed++;
                    }
                }
                console.println("Features Added: " + added + " Removed: " + removed + " Modified: "
                        + modified);
            }
        } catch (SynchronizationException e) {
            switch (e.statusCode) {
            case HISTORY_TOO_SHALLOW:
            default:
                throw new CommandFailedException("Unable to pull, the remote history is shallow.",
                        e);
            }
        }

    }
}
