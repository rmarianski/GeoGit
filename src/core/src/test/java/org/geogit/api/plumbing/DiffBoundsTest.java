/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api.plumbing;

import java.util.ArrayList;
import java.util.List;

import org.geogit.api.RevCommit;
import org.geogit.api.porcelain.CommitOp;
import org.geogit.test.integration.RepositoryTestCase;
import org.junit.Test;
import org.opengis.feature.Feature;
import org.opengis.geometry.BoundingBox;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;

public class DiffBoundsTest extends RepositoryTestCase {

    private Feature l1Modified;

    private Feature l2Modified;

    private RevCommit points1_modified_commit;

    @Override
    protected void setUpInternal() throws Exception {
        // create one commit per feature
        ArrayList<RevCommit> commits = Lists.newArrayList(populate(true, points1, points3,
                points1_modified));
        this.points1_modified_commit = commits.get(2);

        Feature p1ModifiedAgain = feature(pointsType, idP1, "StringProp1_1a", new Integer(1001),
                "POINT(10 20)");// used to be POINT(1 2)
        insertAndAdd(p1ModifiedAgain);
        commits.add(geogit.command(CommitOp.class).call());

        points1B_modified = feature(pointsType, idP1, "StringProp1B_1a", new Integer(2000),
                "POINT(10 220)");
        insertAndAdd(points1B_modified);
        commits.add(geogit.command(CommitOp.class).call());

        l1Modified = feature(linesType, idL1, "StringProp2_1", new Integer(1000),
                "LINESTRING (1 1, -2 -2)");// used to be LINESTRING (1 1, 2 2)

        l2Modified = feature(linesType, idL2, "StringProp2_2", new Integer(2000),
                "LINESTRING (3 3, 4 4)");// used to be LINESTRING (3 3, 4 4)
    }

    @Test
    public void testDiffBetweenDifferentTrees() {
        String oldRefSpec = "HEAD~3";
        String newRefSpec = "HEAD";

        Envelope diffBounds = geogit.command(DiffBounds.class).setOldVersion(oldRefSpec)
                .setNewVersion(newRefSpec).call();

        assertEquals(diffBounds.getMinX(), 1.0, 0.0);
        assertEquals(diffBounds.getMinY(), 1.0, 0.0);
        assertEquals(diffBounds.getMaxX(), 10.0, 0.0);
        assertEquals(diffBounds.getMaxY(), 220.0, 0.0);
    }

    @Test
    public void testDiffBetweenIdenticalTrees() {
        String oldRefSpec = "HEAD";
        String newRefSpec = "HEAD";
        Envelope diffBounds = geogit.command(DiffBounds.class).setOldVersion(oldRefSpec)
                .setNewVersion(newRefSpec).call();
        assertTrue(diffBounds.isNull());
    }

    @Test
    public void testPathFiltering() throws Exception {
        insertAndAdd(l1Modified);
        geogit.command(CommitOp.class).call();
        insert(l2Modified);

        testPathFiltering("HEAD~3", "HEAD", l1Modified.getBounds(), linesName);
        testPathFiltering("HEAD", "WORK_HEAD", l2Modified.getBounds(), linesName);
        testPathFiltering("HEAD~3", "HEAD~2", new Envelope(), linesName);
        testPathFiltering("HEAD~3", "HEAD~2", new Envelope(), linesName);

        String head = points1_modified_commit.getId().toString();

        Envelope expected = new Envelope((Envelope) points1.getBounds());
        expected.expandToInclude((Envelope) points1_modified.getBounds());
        testPathFiltering(head + "^", head, expected, pointsName);
        testPathFiltering(head + "^", head, new Envelope(), linesName);
        testPathFiltering("HEAD^", "HEAD", new Envelope(), pointsName);
    }

    private void testPathFiltering(String oldVersion, String newVersion,
            BoundingBox expectedBounds, String... pathFilters) {

        Envelope expected = new Envelope((Envelope) expectedBounds);
        testPathFiltering(oldVersion, newVersion, expected, pathFilters);
    }

    private void testPathFiltering(String oldVersion, String newVersion, Envelope expected,
            String... pathFilters) {

        List<String> filter = ImmutableList.<String> copyOf(pathFilters);

        Envelope actual = geogit.command(DiffBounds.class).setOldVersion(oldVersion)
                .setNewVersion(newVersion).setPathFilters(filter).call();

        assertEquals(expected, actual);
    }
}
