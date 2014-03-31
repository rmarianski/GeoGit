/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.cli.porcelain;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import jline.console.ConsoleReader;

import org.geogit.api.Ref;
import org.geogit.api.porcelain.FetchOp;
import org.geogit.api.porcelain.FetchResult;
import org.geogit.api.porcelain.FetchResult.ChangedRef;
import org.geogit.api.porcelain.FetchResult.ChangedRef.ChangeTypes;
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
 * Fetches named heads or tags from one or more other repositories, along with the objects necessary
 * to complete them.
 * <p>
 * {@code geogit fetch} can fetch from either a single named repository, or from several
 * repositories at once.
 * <p>
 * CLI proxy for {@link FetchOp}
 * <p>
 * Usage:
 * <ul>
 * <li> {@code geogit fetch [<options>] [<repository>...]}
 * </ul>
 * 
 * @see FetchOp
 */
@StagingDatabaseReadOnly
@RemotesReadOnly
@Parameters(commandNames = "fetch", commandDescription = "Download objects and refs from another repository")
public class Fetch extends AbstractCommand implements CLICommand {

    @Parameter(names = "--all", description = "Fetch from all remotes.")
    private boolean all = false;

    @Parameter(names = { "-p", "--prune" }, description = "After fetching, remove any remote-tracking branches which no longer exist on the remote.")
    private boolean prune = false;

    @Parameter(names = { "--depth" }, description = "Depth of the fetch.")
    private int depth = 0;

    @Parameter(names = { "--fulldepth" }, description = "Fetch the full history from the repository.")
    private boolean fulldepth = false;

    @Parameter(description = "[<repository>...]")
    private List<String> args;

    /**
     * Executes the fetch command using the provided options.
     */
    @Override
    public void runInternal(GeogitCLI cli) throws IOException {
        checkParameter(depth > 0 ? !fulldepth : true,
                "Cannot specify a depth and full depth.  Use --depth <depth> or --fulldepth.");

        if (depth > 0 || fulldepth) {
            checkParameter(cli.getGeogit().getRepository().getDepth().isPresent(),
                    "Depth operations can only be used on a shallow clone.");
        }

        FetchResult result;
        try {
            FetchOp fetch = cli.getGeogit().command(FetchOp.class);
            fetch.setProgressListener(cli.getProgressListener());
            fetch.setAll(all).setPrune(prune).setFullDepth(fulldepth);
            fetch.setDepth(depth);

            if (args != null) {
                for (String repo : args) {
                    fetch.addRemote(repo);
                }
            }

            result = fetch.call();
        } catch (SynchronizationException e) {
            switch (e.statusCode) {
            case HISTORY_TOO_SHALLOW:
            default:
                throw new CommandFailedException("Unable to fetch, the remote history is shallow.",
                        e);
            }
        } catch (IllegalArgumentException iae) {
            throw new CommandFailedException(iae.getMessage(), iae);
        } catch (IllegalStateException ise) {
            throw new CommandFailedException(ise.getMessage(), ise);
        }

        ConsoleReader console = cli.getConsole();
        if (result.getChangedRefs().entrySet().size() > 0) {
            for (Entry<String, List<ChangedRef>> entry : result.getChangedRefs().entrySet()) {
                console.println("From " + entry.getKey());

                for (ChangedRef ref : entry.getValue()) {
                    String line;
                    if (ref.getType() == ChangeTypes.CHANGED_REF) {
                        line = "   " + ref.getOldRef().getObjectId().toString().substring(0, 7)
                                + ".." + ref.getNewRef().getObjectId().toString().substring(0, 7)
                                + "     " + ref.getOldRef().localName() + " -> "
                                + ref.getOldRef().getName();
                    } else if (ref.getType() == ChangeTypes.ADDED_REF) {
                        String reftype = (ref.getNewRef().getName().startsWith(Ref.TAGS_PREFIX)) ? "tag" : "branch";
                        line = " * [new " + reftype + "]     " + ref.getNewRef().localName() + " -> "
                                + ref.getNewRef().getName();
                    } else if (ref.getType() == ChangeTypes.REMOVED_REF) {
                        line = " x [deleted]        (none) -> " + ref.getOldRef().getName();
                    } else {
                        line = "   [deepened]       " + ref.getNewRef().localName();
                    }
                    console.println(line);
                }
            }
        } else {
            console.println("Already up to date.");
        }
    }
}
