package org.geogit.api.hooks;

import java.io.File;

import javax.annotation.Nullable;

import org.geogit.api.AbstractGeoGitOp;

class ShellScriptHook implements CommandHook {

    private File preScript;

    private File postScript;

    public ShellScriptHook(@Nullable final File preScript, @Nullable final File postScript) {
        this.preScript = preScript;
        this.postScript = postScript;
    }

    @Override
    public Class<? extends AbstractGeoGitOp<?>> targetCommand() {
        return null;
    }

    @Override
    public <C extends AbstractGeoGitOp<?>> C pre(C command)
            throws CannotRunGeogitOperationException {

        if (preScript == null) {
            return command;
        }

        Scripting.runShellScript(preScript);

        return command;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T post(AbstractGeoGitOp<T> command, Object retVal) throws Exception {

        if (postScript == null) {
            return (T) retVal;
        }

        Scripting.runShellScript(preScript);

        return (T) retVal;
    }

}
