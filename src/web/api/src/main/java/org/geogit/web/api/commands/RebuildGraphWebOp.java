/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.web.api.commands;

import org.geogit.api.Context;
import org.geogit.api.ObjectId;
import org.geogit.api.plumbing.RebuildGraphOp;
import org.geogit.web.api.AbstractWebAPICommand;
import org.geogit.web.api.CommandContext;
import org.geogit.web.api.CommandResponse;
import org.geogit.web.api.ResponseWriter;

import com.google.common.collect.ImmutableList;

/**
 * Interface for the rebuild graph operation in GeoGit.
 * 
 * Web interface for {@link RebuildGraphOp}
 */

public class RebuildGraphWebOp extends AbstractWebAPICommand {

    private boolean quiet = false;

    /**
     * Mutator for the quiet variable
     * 
     * @param quiet - If true, limit the output of the command.
     */
    public void setQuiet(boolean quiet) {
        this.quiet = quiet;
    }

    /**
     * Runs the command and builds the appropriate response.
     * 
     * @param context - the context to use for this command
     */
    @Override
    public void run(CommandContext context) {
        final Context geogit = this.getCommandLocator(context);

        final ImmutableList<ObjectId> updatedObjects = geogit.command(RebuildGraphOp.class).call();

        context.setResponseContent(new CommandResponse() {
            @Override
            public void write(ResponseWriter out) throws Exception {
                out.start();
                out.writeRebuildGraphResponse(updatedObjects, quiet);
                out.finish();
            }
        });
    }
}
