package org.geogit.rest.repository;

import org.restlet.Router;

public class RepositoryRouter extends Router {

    public RepositoryRouter() {
        attach("/manifest", ManifestResource.class);
        attach("/objects/{id}", new ObjectFinder());
        attach("/batchobjects", new BatchedObjectResource());
        attach("/sendobject", SendObjectResource.class);
        attach("/exists", ObjectExistsResource.class);
        attach("/beginpush", BeginPush.class);
        attach("/endpush", EndPush.class);
        attach("/getdepth", DepthResource.class);
        attach("/getparents", ParentResource.class);
        attach("/affectedfeatures", AffectedFeaturesResource.class);
        attach("/filteredchanges", new FilteredChangesResource());
        attach("/applychanges", new ApplyChangesResource());
        attach("/mergefeature", MergeFeatureResource.class);
    }
}
