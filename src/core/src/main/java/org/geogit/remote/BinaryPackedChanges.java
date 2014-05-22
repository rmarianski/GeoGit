/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.remote;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.geogit.api.NodeRef;
import org.geogit.api.ObjectId;
import org.geogit.api.RevObject;
import org.geogit.api.RevObject.TYPE;
import org.geogit.api.plumbing.diff.DiffEntry;
import org.geogit.repository.Repository;
import org.geogit.storage.ObjectDatabase;
import org.geogit.storage.datastream.DataStreamSerializationFactory;
import org.geogit.storage.datastream.FormatCommon;

import com.google.common.base.Throwables;

/**
 * Provides a method of packing a set of changes and the affected objects to and from a binary
 * stream.
 */
public final class BinaryPackedChanges {

    private static final DataStreamSerializationFactory serializer = DataStreamSerializationFactory.INSTANCE;

    private final Repository repository;

    private boolean filtered;

    private static enum CHUNK_TYPE {
        DIFF_ENTRY {
            @Override
            public int value() {
                return 0;
            }
        },
        OBJECT_AND_DIFF_ENTRY {
            @Override
            public int value() {
                return 1;
            }
        },
        METADATA_OBJECT_AND_DIFF_ENTRY {
            @Override
            public int value() {
                return 2;
            }
        },
        FILTER_FLAG {
            @Override
            public int value() {
                return 3;
            }
        };

        public abstract int value();

    };

    /**
     * Constructs a new {@code BinaryPackedChanges} instance using the provided {@link Repository}.
     * 
     * @param repository the repository to save objects to, or read objects from, depending on the
     *        operation
     */
    public BinaryPackedChanges(Repository repository) {
        this.repository = repository;
        filtered = false;
    }

    public boolean wasFiltered() {
        return filtered;
    }

    /**
     * Writes the set of changes to the provided output stream.
     * 
     * @param out the stream to write to
     * @param changes the changes to write
     * @throws IOException
     */
    public void write(OutputStream out, Iterator<DiffEntry> changes) throws IOException {
        write(out, changes, DEFAULT_CALLBACK);
    }

    /**
     * Writes the set of changes to the provided output stream, calling the provided callback for
     * each item.
     * 
     * @param out the stream to write to
     * @param changes the changes to write
     * @param callback the callback function to call for each element written
     * @return the state of the operation at the conclusion of writing
     * @throws IOException
     */
    public void write(OutputStream out, Iterator<DiffEntry> changes, Callback callback)
            throws IOException {

        final ObjectDatabase objectDatabase = repository.objectDatabase();

        // avoids sending the same metadata object multiple times
        Set<ObjectId> writtenMetadataIds = new HashSet<ObjectId>();

        // buffer to avoid ObjectId cloning its internal state for each object
        byte[] oidbuffer = new byte[ObjectId.NUM_BYTES];

        while (changes.hasNext()) {
            DiffEntry diff = changes.next();

            if (diff.isDelete()) {
                out.write(CHUNK_TYPE.DIFF_ENTRY.value());
            } else {
                // its a change or an addition, new object is guaranteed to be present
                NodeRef newObject = diff.getNewObject();
                ObjectId metadataId = newObject.getMetadataId();
                if (writtenMetadataIds.contains(metadataId)) {
                    out.write(CHUNK_TYPE.OBJECT_AND_DIFF_ENTRY.value());
                } else {
                    out.write(CHUNK_TYPE.METADATA_OBJECT_AND_DIFF_ENTRY.value());
                    RevObject metadata = objectDatabase.get(metadataId);
                    writeObjectId(metadataId, out, oidbuffer);
                    serializer.createObjectWriter(metadata.getType()).write(metadata, out);
                    writtenMetadataIds.add(metadataId);
                }

                ObjectId objectId = newObject.objectId();
                RevObject object = objectDatabase.get(objectId);
                writeObjectId(objectId, out, oidbuffer);
                serializer.createObjectWriter(object.getType()).write(object, out);
            }
            DataOutput dataOut = new DataOutputStream(out);
            FormatCommon.writeDiff(diff, dataOut);
            callback.callback(diff);

        }
        // signal the end of changes
        out.write(CHUNK_TYPE.FILTER_FLAG.value());
        if (changes instanceof FilteredDiffIterator
                && ((FilteredDiffIterator) changes).wasFiltered()) {
            out.write(1);
        } else {
            out.write(0);
        }
    }

    private void writeObjectId(ObjectId objectId, OutputStream out, byte[] oidbuffer)
            throws IOException {
        objectId.getRawValue(oidbuffer);
        out.write(oidbuffer);
    }

    /**
     * Read in the changes from the provided input stream. The input stream represents the output of
     * another {@code BinaryPackedChanges} instance.
     * 
     * @param in the stream to read from
     */
    public void ingest(final InputStream in) {
        ingest(in, DEFAULT_CALLBACK);
    }

    /**
     * Read in the changes from the provided input stream and call the provided callback for each
     * change. The input stream represents the output of another {@code BinaryPackedChanges}
     * instance.
     * 
     * @param in the stream to read from
     * @param callback the callback to call for each item
     */
    public void ingest(final InputStream in, Callback callback) {
        while (true) {
            try {
                ingestOne(in, callback);
            } catch (EOFException e) {
                break;
            } catch (IOException e) {
                Throwables.propagate(e);
            }
        }
    }

    /**
     * Reads in a single change from the provided input stream.
     * 
     * @param in the stream to read from
     * @param callback the callback to call on the resulting item
     * @throws IOException
     */
    private void ingestOne(final InputStream in, Callback callback) throws IOException {
        int chunkType = in.read();
        if (chunkType == CHUNK_TYPE.FILTER_FLAG.value()) {
            int changesFiltered = in.read();
            if (changesFiltered != 0) {
                filtered = true;
            }
            throw new EOFException();
        }
        if (chunkType == CHUNK_TYPE.METADATA_OBJECT_AND_DIFF_ENTRY.value()) {
            ObjectId id = readObjectId(in);
            RevObject revObj = serializer.createObjectReader().read(id, in);

            if (!repository.objectDatabase().exists(id)) {
                repository.objectDatabase().put(revObj);
            }
        }
        if (chunkType != CHUNK_TYPE.DIFF_ENTRY.value()) {
            ObjectId id = readObjectId(in);
            RevObject revObj = serializer.createObjectReader().read(id, in);

            if (!repository.objectDatabase().exists(id)) {
                repository.objectDatabase().put(revObj);
            }
        }
        DataInput dataIn = new DataInputStream(in);
        DiffEntry diff = FormatCommon.readDiff(dataIn);
        callback.callback(diff);
    }

    /**
     * Reads an {@link ObjectId} from the provided input stream.
     * 
     * @param in the stream to read from
     * @return the {@code ObjectId} that was read
     * @throws IOException
     */
    private ObjectId readObjectId(final InputStream in) throws IOException {
        byte[] rawBytes = new byte[20];
        int amount = 0;
        int len = 20;
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

    /**
     * Interface for callback methods to be used by the read and write operations.
     */
    public static interface Callback {
        public abstract void callback(DiffEntry diff);
    }

    private static final Callback DEFAULT_CALLBACK = new Callback() {
        @Override
        public void callback(DiffEntry diff) {
            // do nothing
        }
    };
}
