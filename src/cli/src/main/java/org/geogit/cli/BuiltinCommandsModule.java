/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.cli;

import org.geogit.cli.plumbing.Cat;
import org.geogit.cli.plumbing.DiffTree;
import org.geogit.cli.plumbing.Insert;
import org.geogit.cli.plumbing.LsTree;
import org.geogit.cli.plumbing.MergeBase;
import org.geogit.cli.plumbing.RebuildGraph;
import org.geogit.cli.plumbing.RevList;
import org.geogit.cli.plumbing.RevParse;
import org.geogit.cli.plumbing.ShowRef;
import org.geogit.cli.plumbing.WalkGraph;
import org.geogit.cli.porcelain.Add;
import org.geogit.cli.porcelain.Apply;
import org.geogit.cli.porcelain.Blame;
import org.geogit.cli.porcelain.Branch;
import org.geogit.cli.porcelain.Checkout;
import org.geogit.cli.porcelain.CherryPick;
import org.geogit.cli.porcelain.Clean;
import org.geogit.cli.porcelain.Clone;
import org.geogit.cli.porcelain.Commit;
import org.geogit.cli.porcelain.Config;
import org.geogit.cli.porcelain.Conflicts;
import org.geogit.cli.porcelain.Diff;
import org.geogit.cli.porcelain.Fetch;
import org.geogit.cli.porcelain.FormatPatch;
import org.geogit.cli.porcelain.Help;
import org.geogit.cli.porcelain.Init;
import org.geogit.cli.porcelain.Log;
import org.geogit.cli.porcelain.Ls;
import org.geogit.cli.porcelain.Merge;
import org.geogit.cli.porcelain.Pull;
import org.geogit.cli.porcelain.Push;
import org.geogit.cli.porcelain.Rebase;
import org.geogit.cli.porcelain.RemoteExtension;
import org.geogit.cli.porcelain.Remove;
import org.geogit.cli.porcelain.Reset;
import org.geogit.cli.porcelain.Revert;
import org.geogit.cli.porcelain.Show;
import org.geogit.cli.porcelain.Squash;
import org.geogit.cli.porcelain.Status;
import org.geogit.cli.porcelain.Tag;
import org.geogit.cli.porcelain.Version;

import com.google.inject.AbstractModule;

/**
 * Guice module providing builtin commands for the {@link GeogitCLI CLI} app.
 * 
 * @see Add
 * @see Apply
 * @see Branch
 * @see Cat
 * @see Checkout
 * @see CherryPick
 * @see Clean
 * @see Commit
 * @see Config
 * @see Conflicts
 * @see Diff
 * @see FormatPatch
 * @see Help
 * @see Init
 * @see Merge
 * @see Log
 * @see RemoteExtension
 * @see Remove
 * @see Status
 * @see Rebase
 * @see Reset
 * @see Clone
 * @see Push
 * @see Pull
 * @see Show
 * @see Fetch
 * @see Version
 * @see RebuildGraph
 */
public class BuiltinCommandsModule extends AbstractModule implements CLIModule {

    @Override
    protected void configure() {
        bind(RevParse.class);
        bind(Add.class);
        bind(Apply.class);
        bind(Blame.class);
        bind(Branch.class);
        bind(Cat.class);
        bind(Checkout.class);
        bind(CherryPick.class);
        bind(Clean.class);
        bind(Commit.class);
        bind(Config.class);
        bind(Conflicts.class);
        bind(Diff.class);
        bind(DiffTree.class);
        bind(FormatPatch.class);
        bind(Help.class);
        bind(Init.class);
        bind(Insert.class);
        bind(Log.class);
        bind(Ls.class);
        bind(LsTree.class);
        bind(Merge.class);
        bind(Log.class);
        bind(MergeBase.class);
        bind(RemoteExtension.class);
        bind(Remove.class);
        bind(Status.class);
        bind(Rebase.class);
        bind(Reset.class);
        bind(Revert.class);
        bind(RevList.class);
        bind(Show.class);
        bind(ShowRef.class);
        bind(Clone.class);
        bind(Fetch.class);
        bind(Push.class);
        bind(Pull.class);
        bind(Squash.class);
        bind(Tag.class);
        bind(WalkGraph.class);
        bind(Version.class);
        bind(RebuildGraph.class);
    }

}
