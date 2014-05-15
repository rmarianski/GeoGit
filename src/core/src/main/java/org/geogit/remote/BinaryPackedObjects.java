/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.remote;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.annotation.Nullable;

import org.geogit.api.ObjectId;
import org.geogit.api.RevCommit;
import org.geogit.api.RevObject;
import org.geogit.repository.PostOrderIterator;
import org.geogit.storage.BulkOpListener;
import org.geogit.storage.Deduplicator;
import org.geogit.storage.ObjectDatabase;
import org.geogit.storage.ObjectReader;
import org.geogit.storage.ObjectSerializingFactory;
import org.geogit.storage.datastream.DataStreamSerializationFactory;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

public final class BinaryPackedObjects {

    private final ObjectSerializingFactory factory;

    private final ObjectReader<RevObject> objectReader;

    private final int CAP = 100;

    private final ObjectDatabase database;

    public BinaryPackedObjects(ObjectDatabase database) {
        this.database = database;
        this.factory = new DataStreamSerializationFactory();
        this.objectReader = factory.createObjectReader();
    }

    public void write(OutputStream out, List<ObjectId> want, List<ObjectId> have,
            boolean traverseCommits, Deduplicator deduplicator) throws IOException {
        write(out, want, have, new HashSet<ObjectId>(), DEFAULT_CALLBACK, traverseCommits, deduplicator);
    }

    public void write(OutputStream out, List<ObjectId> want, List<ObjectId> have,
            Set<ObjectId> sent, Callback callback, boolean traverseCommits, Deduplicator deduplicator) throws IOException {
        for (ObjectId i : want) {
            if (!database.exists(i)) {
                throw new NoSuchElementException("Wanted id: " + i + " is not known");
            }
        }

        ImmutableList<ObjectId> needsPrevisit = traverseCommits ? scanForPrevisitList(want, have, deduplicator)
                : ImmutableList.copyOf(have);
        deduplicator.reset();
        ImmutableList<ObjectId> previsitResults = reachableContentIds(needsPrevisit, deduplicator);
        deduplicator.reset();

        int commitsSent = 0;
        Iterator<RevObject> objects = PostOrderIterator.range(want, new ArrayList<ObjectId>(
                previsitResults), database, traverseCommits, deduplicator);
        while (objects.hasNext() && commitsSent < CAP) {
            RevObject object = objects.next();

            out.write(object.getId().getRawValue());
            factory.createObjectWriter(object.getType()).write(object, out);
            callback.callback(object);
        }
    }

    /**
     * Find commits which should be previsited to avoid resending objects that are already on the
     * receiving end. A commit should be previsited if:
     * <ul>
     * <li>It is not going to be visited, and
     * <li>It is the immediate ancestor of a commit which is going to be previsited.
     * </ul>
     * 
     */
    private ImmutableList<ObjectId> scanForPrevisitList(List<ObjectId> want, List<ObjectId> have, Deduplicator deduplicator) {
        /*
         * @note Implementation note: To find the previsit list, we just iterate over all the
         * commits that will be visited according to our want and have lists. Any parents of commits
         * in this traversal which are part of the 'have' list will be in the previsit list.
         */
        Iterator<RevCommit> willBeVisited = Iterators.filter( //
                PostOrderIterator.rangeOfCommits(want, have, database, deduplicator), //
                RevCommit.class);
        ImmutableSet.Builder<ObjectId> builder = ImmutableSet.builder();

        while (willBeVisited.hasNext()) {
            RevCommit next = willBeVisited.next();
            List<ObjectId> parents = new ArrayList<ObjectId>(next.getParentIds());
            parents.retainAll(have);
            builder.addAll(parents);
        }

        return ImmutableList.copyOf(builder.build());
    }

    private ImmutableList<ObjectId> reachableContentIds(ImmutableList<ObjectId> needsPrevisit, Deduplicator deduplicator) {
        Function<RevObject, ObjectId> getIdTransformer = new Function<RevObject, ObjectId>() {
            @Override
            @Nullable
            public ObjectId apply(@Nullable RevObject input) {
                return input == null ? null : input.getId();
            }
        };

        Iterator<ObjectId> reachable = Iterators.transform( //
                PostOrderIterator.contentsOf(needsPrevisit, database, deduplicator), //
                getIdTransformer);
        return ImmutableList.copyOf(reachable);
    }

    public void ingest(final InputStream in) {
        ingest(in, DEFAULT_CALLBACK);
    }

    public void ingest(final InputStream in, final Callback callback) {
        Iterator<RevObject> objects = streamToObjects(in);

        BulkOpListener listener = new BulkOpListener() {
            @Override
            public void inserted(ObjectId objectId, @Nullable Integer storageSizeBytes) {
                callback.callback(database.get(objectId));
            }
        };
        
        database.putAll(objects, listener);
    }

    
    private Iterator<RevObject> streamToObjects(final InputStream in) {
        return new AbstractIterator<RevObject>() {
            @Override
            protected RevObject computeNext() {
                try {
                    ObjectId id = readObjectId(in);
                    RevObject revObj = objectReader.read(id, in);
                    return revObj;
                } catch (EOFException eof) {
                    return endOfData();
                } catch (IOException e) {
                    Throwables.propagate(e);
                }
                throw new IllegalStateException("stream should have been fully consumed");
            }
        };
    }

    private ObjectId readObjectId(final InputStream in) throws IOException {
        final int len = ObjectId.NUM_BYTES;
        byte[] rawBytes = new byte[len];
        int amount = 0;
        int offset = 0;
        while ((amount = in.read(rawBytes, offset, len - offset)) != 0) {
            if (amount < 0)
                throw new EOFException("Came to end of input");
            offset += amount;
            if (offset == len)
                break;
        }
        ObjectId id = ObjectId.createNoClone(rawBytes);
        return id;
    }

    public static interface Callback {
        public abstract void callback(RevObject object);
    }

    private static final Callback DEFAULT_CALLBACK = new Callback() {
        @Override
        public void callback(RevObject object) {
            // empty body
        }
    };

}
