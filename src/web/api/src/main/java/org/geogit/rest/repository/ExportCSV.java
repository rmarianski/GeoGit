package org.geogit.rest.repository;

import static org.geogit.rest.repository.RESTUtils.getGeogit;

import java.io.File;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringEscapeUtils;
import org.geogit.api.GeoGIT;
import org.geogit.api.NodeRef;
import org.geogit.api.ObjectId;
import org.geogit.api.RevCommit;
import org.geogit.api.RevFeature;
import org.geogit.api.RevFeatureType;
import org.geogit.api.RevObject;
import org.geogit.api.plumbing.FindTreeChild;
import org.geogit.api.plumbing.ParseTimestamp;
import org.geogit.api.plumbing.RevObjectParse;
import org.geogit.api.plumbing.RevParse;
import org.geogit.api.plumbing.diff.DiffEntry;
import org.geogit.api.porcelain.DiffOp;
import org.geogit.api.porcelain.LogOp;
import org.geogit.storage.FieldType;
import org.geogit.web.api.CommandSpecException;
import org.geotools.util.Range;
import org.opengis.feature.type.PropertyDescriptor;
import org.restlet.Context;
import org.restlet.data.Form;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.FileRepresentation;
import org.restlet.resource.Resource;
import org.restlet.resource.Variant;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

public class ExportCSV extends Resource {

    @Override
    public void init(Context context, Request request, Response response) {
        super.init(context, request, response);
        List<Variant> variants = getVariants();
        variants.add(new ExportCSVRepresentation(null, new MediaType("text/csv",
                "Comma-separated Values"), 300));
        Form headers = new Form();
        headers.add("Content-Disposition",
                String.format("attachment; filename=\"%s\"", "DiffExport"));
        // this is a backdoor to setting arbitrary headers not supported
        // by the restlet formalized API
        getResponse().getAttributes().put("org.restlet.http.headers", headers);
    }

    private class ExportCSVRepresentation extends FileRepresentation {

        public ExportCSVRepresentation(File file, MediaType mediaType, int timeToLive) {
            super(createFile(), mediaType, timeToLive);
            // TODO Auto-generated constructor stub
        }
    }

    protected File createFile() {
        GeoGIT geogit = getGeogit(getRequest()).get();
        Form options = getRequest().getResourceRef().getQueryAsForm();
        String path = options.getFirstValue("path", null);
        String sinceTime = options.getFirstValue("sinceTime", null);
        String untilTime = options.getFirstValue("untilTime", null);
        String branch = options.getFirstValue("until", null);
        LogOp op = geogit.command(LogOp.class);
        if (path != null) {
            op.addPath(path);
        } else {
            throw new CommandSpecException(
                    "A path must be specified for the feature type to export.");
        }
        if (sinceTime != null || untilTime != null) {
            Date since = new Date(0);
            Date until = new Date();
            if (sinceTime != null) {
                since = new Date(geogit.command(ParseTimestamp.class).setString(sinceTime).call());
            }
            if (untilTime != null) {
                until = new Date(geogit.command(ParseTimestamp.class).setString(untilTime).call());
            }
            op.setTimeRange(new Range<Date>(Date.class, since, until));
        }
        if (branch != null) {
            Optional<ObjectId> until;
            until = geogit.command(RevParse.class).setRefSpec(branch).call();
            Preconditions.checkArgument(until.isPresent(), "Object not found '%s'", branch);
            op.setUntil(until.get());
        }
        try {
            File tempFile = File.createTempFile("DiffExport", "csv");
            String response = "ChangeType,CommitId,Parent CommitIds,Author Name,Author Email,Author Commit Time,Committer Name,Committer Email,Committer Commit Time,Commit Message";
            Files.append(response, tempFile, Charsets.UTF_8);
            response = "";
            final Iterator<RevCommit> log = op.call();
            // This is the feature type object
            Optional<NodeRef> ref = geogit.command(FindTreeChild.class).setChildPath(path)
                    .setParent(geogit.getRepository().getWorkingTree().getTree()).call();
            Optional<RevObject> type = Optional.absent();
            if (ref.isPresent()) {
                type = geogit.command(RevObjectParse.class)
                        .setRefSpec(ref.get().getMetadataId().toString()).call();
            } else {
                throw new CommandSpecException("Couldn't resolve the given path.");
            }
            if (type.isPresent() && type.get() instanceof RevFeatureType) {
                RevFeatureType featureType = (RevFeatureType) type.get();
                Collection<PropertyDescriptor> attribs = featureType.type().getDescriptors();
                int attributeLength = attribs.size();
                for (PropertyDescriptor attrib : attribs) {
                    response += "," + StringEscapeUtils.escapeCsv(attrib.getName().toString());
                }
                response += '\n';
                Files.append(response, tempFile, Charsets.UTF_8);
                response = "";
                RevCommit commit = null;

                while (log.hasNext()) {
                    commit = log.next();
                    String parentId = commit.getParentIds().size() >= 1 ? commit.getParentIds()
                            .get(0).toString() : ObjectId.NULL.toString();
                    Iterator<DiffEntry> diff = geogit.command(DiffOp.class).setOldVersion(parentId)
                            .setNewVersion(commit.getId().toString()).setFilter(path).call();
                    while (diff.hasNext()) {
                        DiffEntry entry = diff.next();
                        response += entry.changeType().toString() + ",";
                        response += commit.getId().toString() + ",";
                        response += parentId;
                        if (commit.getParentIds().size() > 1) {
                            for (int index = 1; index < commit.getParentIds().size(); index++) {
                                response += " " + commit.getParentIds().get(index).toString();
                            }
                        }
                        response += ",";
                        if (commit.getAuthor().getName().isPresent()) {
                            response += StringEscapeUtils.escapeCsv(commit.getAuthor().getName()
                                    .get());
                        }
                        response += ",";
                        if (commit.getAuthor().getEmail().isPresent()) {
                            response += StringEscapeUtils.escapeCsv(commit.getAuthor().getEmail()
                                    .get());
                        }
                        response += ","
                                + new SimpleDateFormat("MM/dd/yyyy HH:mm:ss z").format(new Date(
                                        commit.getAuthor().getTimestamp())) + ",";
                        if (commit.getCommitter().getName().isPresent()) {
                            response += StringEscapeUtils.escapeCsv(commit.getCommitter().getName()
                                    .get());
                        }
                        response += ",";
                        if (commit.getCommitter().getEmail().isPresent()) {
                            response += StringEscapeUtils.escapeCsv(commit.getCommitter()
                                    .getEmail().get());
                        }
                        response += ","
                                + new SimpleDateFormat("MM/dd/yyyy HH:mm:ss z").format(new Date(
                                        commit.getCommitter().getTimestamp())) + ",";
                        String message = StringEscapeUtils.escapeCsv(commit.getMessage());
                        response += message;
                        if (entry.newObjectId() == ObjectId.NULL) {
                            // Feature was removed so we need to fill out blank attribute values
                            for (int index = 0; index < attributeLength; index++) {
                                response += ",";
                            }
                        } else {
                            // Feature was added or modified so we need to write out the
                            // attribute
                            // values from the feature
                            Optional<RevObject> feature = geogit.command(RevObjectParse.class)
                                    .setObjectId(entry.newObjectId()).call();
                            RevFeature revFeature = (RevFeature) feature.get();
                            List<Optional<Object>> values = revFeature.getValues();
                            for (int index = 0; index < values.size(); index++) {
                                Optional<Object> value = values.get(index);
                                PropertyDescriptor attrib = (PropertyDescriptor) attribs.toArray()[index];
                                String stringValue = "";
                                if (value.isPresent()) {
                                    FieldType attributeType = FieldType.forBinding(attrib.getType()
                                            .getBinding());
                                    switch (attributeType) {
                                    case DATE:
                                        stringValue = new SimpleDateFormat("MM/dd/yyyy z")
                                                .format((java.sql.Date) value.get());
                                        break;
                                    case DATETIME:
                                        stringValue = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss z")
                                                .format((Date) value.get());
                                        break;
                                    case TIME:
                                        stringValue = new SimpleDateFormat("HH:mm:ss z")
                                                .format((Time) value.get());
                                        break;
                                    case TIMESTAMP:
                                        stringValue = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss z")
                                                .format((Timestamp) value.get());
                                        break;
                                    default:
                                        stringValue = StringEscapeUtils.escapeCsv(value.get()
                                                .toString());
                                    }
                                    response += "," + stringValue;
                                } else {
                                    response += ",";
                                }
                            }
                        }
                        response += '\n';
                        Files.append(response, tempFile, Charsets.UTF_8);
                        response = "";
                    }
                }
                return tempFile;
            } else {
                // Couldn't resolve FeatureType
                throw new CommandSpecException("Couldn't resolve the given path to a feature type.");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
