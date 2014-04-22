/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.web.api.commands;

import org.geogit.api.Context;
import org.geogit.api.GeogitTransaction;
import org.geogit.api.ObjectId;
import org.geogit.api.RevCommit;
import org.geogit.api.plumbing.FindCommonAncestor;
import org.geogit.api.plumbing.TransactionEnd;
import org.geogit.api.plumbing.merge.MergeScenarioReport;
import org.geogit.api.plumbing.merge.ReportMergeScenarioOp;
import org.geogit.api.porcelain.MergeConflictsException;
import org.geogit.api.porcelain.RebaseConflictsException;
import org.geogit.web.api.AbstractWebAPICommand;
import org.geogit.web.api.CommandContext;
import org.geogit.web.api.CommandResponse;
import org.geogit.web.api.CommandSpecException;
import org.geogit.web.api.ResponseWriter;

import com.google.common.base.Optional;

/**
 * Interface for the TransactionEnd operation in GeoGit.
 * 
 * Web interface for {@link TransactionEnd}
 */

public class EndTransaction extends AbstractWebAPICommand {

    private boolean cancel;

    /**
     * Mutator for the cancel variable
     * 
     * @param cancel - true to abort all changes made in this transaction
     */
    public void setCancel(boolean cancel) {
        this.cancel = cancel;
    }

    /**
     * Runs the command and builds the appropriate response.
     * 
     * @param context - the context to use for this command
     * 
     * @throws CommandSpecException
     */
    @Override
    public void run(CommandContext context) {
        if (this.getTransactionId() == null) {
            throw new CommandSpecException("There isn't a transaction to end.");
        }

        final Context transaction = this.getCommandLocator(context);

        TransactionEnd endTransaction = context.getGeoGIT().command(TransactionEnd.class);
        try {
            final boolean closed = endTransaction.setCancel(cancel)
                    .setTransaction((GeogitTransaction) transaction).call();

            context.setResponseContent(new CommandResponse() {
                @Override
                public void write(ResponseWriter out) throws Exception {
                    out.start();
                    if (closed) {
                        out.writeTransactionId(null);
                    } else {
                        out.writeTransactionId(getTransactionId());
                    }
                    out.finish();
                }
            });
        } catch (MergeConflictsException m) {
            final RevCommit ours = context.getGeoGIT().getRepository().getCommit(m.getOurs());
            final RevCommit theirs = context.getGeoGIT().getRepository().getCommit(m.getTheirs());
            final Optional<ObjectId> ancestor = transaction.command(FindCommonAncestor.class)
                    .setLeft(ours).setRight(theirs).call();
            context.setResponseContent(new CommandResponse() {
                final MergeScenarioReport report = transaction.command(ReportMergeScenarioOp.class)
                        .setMergeIntoCommit(ours).setToMergeCommit(theirs).call();

                @Override
                public void write(ResponseWriter out) throws Exception {
                    out.start();
                    Optional<RevCommit> mergeCommit = Optional.absent();
                    out.writeMergeResponse(mergeCommit, report, transaction, ours.getId(),
                            theirs.getId(), ancestor.get());
                    out.finish();
                }
            });
        } catch (RebaseConflictsException r) {
            // TODO: Handle rebase exception
        }
    }
}
