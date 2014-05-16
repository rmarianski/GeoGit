/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.web.api.commands;

import org.geogit.api.Context;
import org.geogit.api.ObjectId;
import org.geogit.api.RevCommit;
import org.geogit.api.RevFeature;
import org.geogit.api.RevFeatureType;
import org.geogit.api.RevObject;
import org.geogit.api.RevTag;
import org.geogit.api.RevTree;
import org.geogit.web.api.AbstractWebAPICommand;
import org.geogit.web.api.CommandContext;
import org.geogit.web.api.CommandResponse;
import org.geogit.web.api.CommandSpecException;
import org.geogit.web.api.ResponseWriter;

import com.google.common.base.Preconditions;

/**
 * Provides a way of getting object descriptions from GeoGit.
 * 
 * Note: This class does not use the internal CatObject implementation.
 */

public class CatWebOp extends AbstractWebAPICommand {

    private ObjectId object;

    /**
     * Mutator for the object variable
     * 
     * @param object - the object you want to view
     */
    public void setObjectId(ObjectId object) {
        this.object = object;
    }

    /**
     * Runs the command and builds the appropriate response
     * 
     * @param context - the context to use for this command
     * 
     * @throws CommandSpecException
     */
    @Override
    public void run(CommandContext context) {
        Preconditions.checkArgument(object != null && !object.equals(ObjectId.NULL));
        final Context geogit = this.getCommandLocator(context);

        Preconditions.checkState(geogit.stagingDatabase().exists(object));
        final RevObject revObject = geogit.stagingDatabase().get(object);
        switch (revObject.getType()) {
        case COMMIT:
            context.setResponseContent(new CommandResponse() {
                @Override
                public void write(ResponseWriter out) throws Exception {
                    out.start();
                    out.writeCommit((RevCommit) revObject, "commit", null, null, null);
                    out.finish();
                }
            });
            break;
        case TREE:
            context.setResponseContent(new CommandResponse() {
                @Override
                public void write(ResponseWriter out) throws Exception {
                    out.start();
                    out.writeTree((RevTree) revObject, "tree");
                    out.finish();
                }
            });
            break;
        case FEATURE:
            context.setResponseContent(new CommandResponse() {
                @Override
                public void write(ResponseWriter out) throws Exception {
                    out.start();
                    out.writeFeature((RevFeature) revObject, "feature");
                    out.finish();
                }
            });
            break;
        case FEATURETYPE:
            context.setResponseContent(new CommandResponse() {
                @Override
                public void write(ResponseWriter out) throws Exception {
                    out.start();
                    out.writeFeatureType((RevFeatureType) revObject, "featuretype");
                    out.finish();
                }
            });
            break;
        case TAG:
            context.setResponseContent(new CommandResponse() {
                @Override
                public void write(ResponseWriter out) throws Exception {
                    out.start();
                    out.writeTag((RevTag) revObject, "tag");
                    out.finish();
                }
            });
            break;
        }
    }

}
