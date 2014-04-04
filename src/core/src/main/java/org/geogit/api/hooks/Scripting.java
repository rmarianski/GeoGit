/* Copyright (c) 2011 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the LGPL 2.1 license, available at the root
 * application directory.
 */
package org.geogit.api.hooks;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.plumbing.ResolveRepository;
import org.geogit.repository.Repository;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Utilities to execute scripts representing hooks for GeoGit operations
 * 
 */
public class Scripting {

    private static final String PARAMS = "params";

    private static final String GEOGIT = "geogit";

    private static ScriptEngineManager factory = new ScriptEngineManager();

    /**
     * Runs a script
     * 
     * @param scriptFile the script file to run
     * @param operation the operation triggering the script, to provide context for the script. This
     *        object might get modified if the script modifies it to alter how the command is called
     *        (for instance, changing the commit message in a commit operation)
     * @throws CannotRunGeogitOperationException
     */
    @SuppressWarnings("unchecked")
    public static void executeScript(File scriptFile, AbstractGeoGitOp<?> operation)
            throws CannotRunGeogitOperationException {
        Preconditions.checkArgument(scriptFile.exists(), "Wrong script file %s",
                scriptFile.getPath());
        String filename = scriptFile.getName();
        String ext = filename.substring(filename.lastIndexOf('.') + 1);
        ScriptEngine engine = factory.getEngineByExtension(ext);
        if (engine != null) {
            try {
                Map<String, Object> params = getParamMap(operation);
                engine.put(PARAMS, params);
                Repository repo = operation.command(ResolveRepository.class).call();
                engine.put(GEOGIT, new GeoGitAPI(repo));
                engine.eval(new FileReader(scriptFile));
                Object map = engine.get(PARAMS);
                setParamMap((Map<String, Object>) map, operation);
            } catch (ScriptException e) {
                Throwable cause = Throwables.getRootCause(e);
                // TODO: improve this hack to check exception type
                if (cause.toString().contains(
                        CannotRunGeogitOperationException.class.getSimpleName())) {
                    String msg = cause.getMessage();
                    msg = msg.substring(
                            CannotRunGeogitOperationException.class.getName().length() + 2,
                            msg.lastIndexOf("(")).trim();
                    throw new CannotRunGeogitOperationException(msg);
                } else {
                    // we ignore all exceptions caused by malformed scripts. We consider them as if
                    // there was no script for this hook
                    return;
                }
            } catch (Exception e) {
                return;
            }
        } else {
            // try running the script directly as an executable file
            List<String> list = Lists.newArrayList();
            ProcessBuilder pb = new ProcessBuilder(list);
            if (isWindows()) {
                list.add("cmd.exe");
                list.add("/C");
                list.add(scriptFile.getPath());
            } else {
                if (scriptFile.canExecute()) {
                    list.add(scriptFile.getPath());
                } else {
                    return;
                }
            }

            try {
                final Process process = pb.start();
                final StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream());
                final StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream());
                errorGobbler.start();
                outputGobbler.start();
                int exitCode = process.waitFor();
                if (exitCode != 0) {
                    // the script exited with non-zero code, so we indicate it throwing the
                    // corresponding exception.
                    // TODO: get message?
                    throw new CannotRunGeogitOperationException(
                            "Hook script exited with non-zero error code");
                }
            } catch (IOException e) {
                return; // can't run scripts, so there is nothing that blocks running the
                        // command, and we can return
            } catch (InterruptedException e) {
                return; // can't run scripts, so there is nothing that blocks running the
                        // command, and we can return

            }

        }

    }

    /**
     * Method for getting values of parameters, including private fields. This is to be used from
     * scripting languages to create hooks for available commands. TODO: Review this and maybe
     * change this way of accessing values
     * 
     * @param operation
     * 
     * @param param the name of the parameter
     * @return the value of the parameter
     */
    public static Map<String, Object> getParamMap(AbstractGeoGitOp<?> operation) {
        Map<String, Object> map = Maps.newHashMap();
        try {
            Field[] fields = operation.getClass().getSuperclass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                map.put(field.getName(), field.get(operation));
            }
        } catch (SecurityException e) {
            return map;
        } catch (IllegalArgumentException e) {
            return map;
        } catch (IllegalAccessException e) {
            return map;
        }
        return map;

    }

    /**
     * Method to set fields in the operation object. This is to be used to communicate with script
     * hooks, so the operation can be modified in the hook, changing the values of its fields.
     * Entries corresponding to inexistent fields are ignored
     * 
     * @param operation
     * 
     * @param a map of new field values. Keys are field names
     */
    public static void setParamMap(Map<String, Object> map, AbstractGeoGitOp<?> operation) {
        try {
            Field[] fields = operation.getClass().getSuperclass().getDeclaredFields();
            Set<String> keys = map.keySet();
            for (Field field : fields) {
                if (keys.contains(field.getName())) {
                    field.setAccessible(true);
                    field.set(operation, map.get(field.getName()));
                }
            }
        } catch (Exception e) {
            // if the script contains wrong variables, and it causes exceptions, or there
            // is any other problem, we just ignore it, and the original command will be executed
        }

    }

    public static boolean isWindows() {
        final String os = System.getProperty("os.name").toLowerCase();
        return (os.indexOf("win") >= 0);
    }

}

class StreamGobbler extends Thread {

    InputStream is;

    StreamGobbler(final InputStream is) {
        this.is = is;
    }

    @Override
    public void run() {
        try {
            final InputStreamReader isr = new InputStreamReader(is);
            final BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (final IOException ioe) {
        }
    }
}
