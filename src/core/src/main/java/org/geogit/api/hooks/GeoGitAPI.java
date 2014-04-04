package org.geogit.api.hooks;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.FeatureBuilder;
import org.geogit.api.NodeRef;
import org.geogit.api.RevFeature;
import org.geogit.api.RevFeatureType;
import org.geogit.api.plumbing.ResolveFeatureType;
import org.geogit.api.plumbing.RevObjectParse;
import org.geogit.api.plumbing.diff.DiffEntry;
import org.geogit.api.plumbing.diff.DiffEntry.ChangeType;
import org.geogit.api.porcelain.DiffOp;
import org.geogit.repository.Repository;
import org.opengis.feature.Feature;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

/**
 * This class contains a facade for some of the most common operations to perform on a GeoGit
 * repository. It is designed to be used mainly from hooks written in a scripting language supported
 * by GeoGit, to give an easier and more detailed access to data and elements in the repository
 * 
 */
public class GeoGitAPI {

    private Repository repository;

    /**
     * @param repo the command locator to use when finding commands
     */
    @Inject
    public GeoGitAPI(Repository repository) {
        this.repository = repository;
    }

    /**
     * A convenience method to throw an exception indicating that the operation after a hook script
     * should not be executed
     * 
     * @throws CannotRunGeogitOperationException
     */
    public void throwHookException(String msg) throws CannotRunGeogitOperationException {
        throw new CannotRunGeogitOperationException(msg);
    }

    /**
     * Returns an array with the features that are staged and ready to be commited. If noDeletions
     * is true, it doesn't include features to be removed, only those ones to be added or modified,
     * so it can be used to check the new data that will get commited into the repository
     * 
     * @return
     */
    public Feature[] getFeaturesToCommit(String path, boolean noDeletions) {
        DiffOp diffOp = repository.command(DiffOp.class);
        diffOp.setCompareIndex(true);
        diffOp.setFilter(path);
        Iterator<DiffEntry> diffs = diffOp.call();
        List<Feature> list = Lists.newArrayList();
        while (diffs.hasNext()) {
            DiffEntry diff = diffs.next();
            if (!diff.changeType().equals(ChangeType.REMOVED) || !noDeletions) {
                RevFeature revFeature = repository.command(RevObjectParse.class)
                        .setObjectId(diff.newObjectId()).call(RevFeature.class).get();
                RevFeatureType revFeatureType = repository.command(RevObjectParse.class)
                        .setObjectId(diff.getNewObject().getMetadataId())
                        .call(RevFeatureType.class).get();
                FeatureBuilder builder = new FeatureBuilder(revFeatureType);
                list.add(builder.build(diff.getNewObject().name(), revFeature));
            }
        }
        return list.toArray(new Feature[0]);
    }

    public Feature[] getUnstagedFeatures(String path, boolean noDeletions) {
        Iterator<DiffEntry> diffs = repository.getWorkingTree().getUnstaged(path);
        List<Feature> list = Lists.newArrayList();
        while (diffs.hasNext()) {
            DiffEntry diff = diffs.next();
            if (!diff.changeType().equals(ChangeType.REMOVED) || !noDeletions) {
                RevFeature revFeature = repository.command(RevObjectParse.class)
                        .setObjectId(diff.newObjectId()).call(RevFeature.class).get();
                RevFeatureType revFeatureType = repository.command(RevObjectParse.class)
                        .setObjectId(diff.getNewObject().getMetadataId())
                        .call(RevFeatureType.class).get();
                FeatureBuilder builder = new FeatureBuilder(revFeatureType);
                list.add(builder.build(diff.getNewObject().name(), revFeature));
            }
        }
        return list.toArray(new Feature[0]);
    }

    /**
     * Returns a feature from the Head of the repository, given its full path
     * 
     * Returns null if the given path doesn't resolve to a feature
     * 
     * @param path the path to the feature to return
     */
    public Feature getFeatureFromHead(String path) {
        String name = NodeRef.nodeFromPath(path);
        String refSpec = "HEAD:" + path;
        Optional<RevFeature> revFeature = repository.command(RevObjectParse.class)
                .setRefSpec(refSpec).call(RevFeature.class);
        if (revFeature.isPresent()) {
            RevFeatureType revFeatureType = repository.command(ResolveFeatureType.class)
                    .setRefSpec(refSpec).call().get();
            FeatureBuilder builder = new FeatureBuilder(revFeatureType);
            return builder.build(name, revFeature.get());
        } else {
            return null;
        }
    }

    /**
     * Returns a feature from the working tree of the repository, given its full path
     * 
     * Returns null if the given path doesn't resolve to a feature
     * 
     * @param path the path to the feature to return
     */
    public Feature getFeatureFromWorkingTree(String path) {
        String name = NodeRef.nodeFromPath(path);
        String refSpec = "WORK_HEAD:" + path;
        Optional<RevFeature> revFeature = repository.command(RevObjectParse.class)
                .setRefSpec(refSpec).call(RevFeature.class);
        if (revFeature.isPresent()) {
            RevFeatureType revFeatureType = repository.command(ResolveFeatureType.class)
                    .setRefSpec(refSpec).call().get();
            FeatureBuilder builder = new FeatureBuilder(revFeatureType);
            return builder.build(name, revFeature.get());
        } else {
            return null;
        }
    }

    public Object run(String className, Map<String, Object> params) throws ClassNotFoundException {
        Class<AbstractGeoGitOp> clazz = (Class<AbstractGeoGitOp>) Class.forName(className);
        AbstractGeoGitOp operation = repository.command(clazz);
        Map<String, Object> oldParams = Scripting.getParamMap(operation);
        Scripting.setParamMap(params, operation);
        return operation.call();
    }

}
