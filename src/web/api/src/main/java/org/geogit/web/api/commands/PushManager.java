/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.web.api.commands;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.geogit.api.GeoGIT;
import org.geogit.api.ObjectId;
import org.geogit.api.Ref;
import org.geogit.api.RevCommit;
import org.geogit.api.RevObject;
import org.geogit.api.SymRef;
import org.geogit.api.plumbing.RefParse;
import org.geogit.api.plumbing.UpdateRef;

import com.google.common.base.Optional;

/**
 * Provides a safety net for remote pushes. This class keeps track of all objects that are being
 * pushed from a remote repository. As objects are being pushed to the server, they will be stored
 * in the Index database. If every object is successfully transfered, a message will be sent to the
 * PushManager to transfer all of those objects to the repository database. This prevents the
 * repository from getting corrupted if a push fails halfway through.
 * 
 */
public class PushManager {

    private Map<String, List<ObjectId>> incomingData;

    private static PushManager instance = new PushManager();

    private PushManager() {
        incomingData = new HashMap<String, List<ObjectId>>();
    }

    /**
     * @return the singleton instance of the {@code PushManager}
     */
    public static PushManager get() {
        return instance;
    }

    /**
     * Begins tracking incoming objects from the specified ip address.
     * 
     * @param ipAddress the remote machine that is pushing objects
     */
    public void connectionBegin(String ipAddress) {
        if (incomingData.containsKey(ipAddress)) {
            incomingData.remove(ipAddress);
        }
        if (incomingData.size() > 0) {
            // Fail?
        }
        List<ObjectId> newList = new LinkedList<ObjectId>();
        incomingData.put(ipAddress, newList);
    }

    /**
     * This is called when the machine at the specified ip address is finished pushing objects to
     * the server. This causes all of those objects to be moved from the index database to the
     * object database.
     * 
     * @param geogit the geogit of the local repository
     * @param ipAddress the remote machine that is pushing objects
     */
    public void connectionSucceeded(GeoGIT geogit, String ipAddress, String refspec,
            ObjectId newCommit) {
        // Add objects to the repository
        if (incomingData.containsKey(ipAddress)) {
            List<ObjectId> objectsToMove = incomingData.remove(ipAddress);
            for (ObjectId oid : objectsToMove) {
                RevObject toAdd = geogit.getRepository().stagingDatabase().get(oid);
                geogit.getRepository().objectDatabase().put(toAdd);
            }
            Optional<Ref> oldRef = geogit.command(RefParse.class).setName(refspec).call();
            Optional<Ref> headRef = geogit.command(RefParse.class).setName(Ref.HEAD).call();
            String refName = refspec;
            if (oldRef.isPresent()) {
                if (oldRef.get().getObjectId().equals(newCommit)) {
                    return;
                }
                refName = oldRef.get().getName();
            }
            if (headRef.isPresent() && headRef.get() instanceof SymRef) {
                if (((SymRef) headRef.get()).getTarget().equals(refName)) {
                    RevCommit commit = geogit.getRepository().getCommit(newCommit);
                    geogit.command(UpdateRef.class).setName(Ref.WORK_HEAD)
                            .setNewValue(commit.getTreeId()).call();
                    geogit.command(UpdateRef.class).setName(Ref.STAGE_HEAD)
                            .setNewValue(commit.getTreeId()).call();
                }
            }

            geogit.command(UpdateRef.class).setName(refName).setNewValue(newCommit).call();
        } else {
            throw new RuntimeException("Tried to end a connection that didn't exist.");
        }
    }

    /**
     * Determines if a given object has already been pushed.
     * 
     * @param ipAddress the remote machine that is pushing objects
     * @param oid the id of the object
     * @return {@code true} if the object has already been pushed and is being tracked by the
     *         {@code PushManager}
     */
    public boolean alreadyPushed(String ipAddress, ObjectId oid) {
        if (incomingData.containsKey(ipAddress)) {
            return incomingData.get(ipAddress).contains(oid);
        }
        return false;
    }

    /**
     * Tells the {@code PushManager} that an object has been added to the index database and should
     * be tracked for the given connection.
     * 
     * @param ipAddress the remote machine that is pushing objects
     * @param oid the id of the object
     */
    public void addObject(String ipAddress, ObjectId oid) {
        if (incomingData.containsKey(ipAddress)) {
            incomingData.get(ipAddress).add(oid);
        } else {
            throw new RuntimeException(
                    "Tried to push an object without first opening a connection.");
        }
    }
}
