/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.geotools.cli.porcelain;

import java.io.IOException;
import java.util.List;

import org.geogit.api.ProgressListener;
import org.geogit.cli.CLICommand;
import org.geogit.cli.CommandFailedException;
import org.geogit.cli.GeogitCLI;
import org.geogit.cli.InvalidParameterException;
import org.geogit.cli.annotation.ObjectDatabaseReadOnly;
import org.geogit.geotools.plumbing.GeoToolsOpException;
import org.geogit.geotools.plumbing.ImportOp;
import org.geotools.data.DataStore;
import org.opengis.feature.type.AttributeDescriptor;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

/**
 * Imports features from one or more shapefiles.
 * 
 * PostGIS CLI proxy for {@link ImportOp}
 * 
 * @see ImportOp
 */
@ObjectDatabaseReadOnly
@Parameters(commandNames = "import", commandDescription = "Import Shapefile")
public class ShpImport extends AbstractShpCommand implements CLICommand {

    /**
     * Shapefiles to import.
     */
    @Parameter(description = "<shapefile> [<shapefile>]...")
    List<String> shapeFile;

    /**
     * do not replace or delete features
     */
    @Parameter(names = { "--add" }, description = "Do not replace or delete features on the destination path, but just add new ones")
    boolean add;

    /**
     * Set the path default feature type to the the feature type of imported features, and modify
     * existing features to match it
     */
    @Parameter(names = { "--alter" }, description = "Set the path default feature type to the the feature type of imported features, and modify existing features to match it")
    boolean alter;

    /**
     * Destination path to add features to. Only allowed when importing a single table
     */
    @Parameter(names = { "-d", "--dest" }, description = "Path to import to")
    String destTable;

    /**
     * The attribute to use to create the feature Id
     */
    @Parameter(names = { "--fid-attrib" }, description = "Use the specified attribute to create the feature Id")
    String fidAttribute;

    /**
     * Executes the import command using the provided options.
     */
    @Override
    protected void runInternal(GeogitCLI cli) throws IOException {
        checkParameter(shapeFile != null && !shapeFile.isEmpty(), "No shapefile specified");

        for (String shp : shapeFile) {

            DataStore dataStore = null;
            try {
                dataStore = getDataStore(shp);
            } catch (InvalidParameterException e) {
                cli.getConsole().println(
                        "The shapefile '" + shp + "' could not be found, skipping...");
                continue;
            }
            if (fidAttribute != null) {
                AttributeDescriptor attrib = dataStore.getSchema(dataStore.getNames().get(0))
                        .getDescriptor(fidAttribute);
                if (attrib == null) {
                    throw new InvalidParameterException(
                            "The specified attribute does not exist in the selected shapefile");
                }
            }

            try {
                cli.getConsole().println("Importing from shapefile " + shp);

                ProgressListener progressListener = cli.getProgressListener();
                ImportOp command = cli.getGeogit().command(ImportOp.class).setAll(true)
                        .setTable(null).setAlter(alter).setOverwrite(!add)
                        .setDestinationPath(destTable).setDataStore(dataStore)
                        .setFidAttribute(fidAttribute);

                // force the import not to use paging due to a bug in the shapefile datastore
                command.setUsePaging(false);
                
                command.setProgressListener(progressListener).call();

                cli.getConsole().println(shp + " imported successfully.");
            } catch (GeoToolsOpException e) {
                switch (e.statusCode) {
                case NO_FEATURES_FOUND:
                    throw new CommandFailedException("No features were found in the shapefile.", e);
                case UNABLE_TO_GET_NAMES:
                    throw new CommandFailedException(
                            "Unable to get feature types from the shapefile.", e);
                case UNABLE_TO_GET_FEATURES:
                    throw new CommandFailedException("Unable to get features from the shapefile.",
                            e);
                case UNABLE_TO_INSERT:
                    throw new CommandFailedException(
                            "Unable to insert features into the working tree.", e);
                default:
                    throw new CommandFailedException("Import failed with exception: "
                            + e.statusCode.name(), e);
                }
            } finally {
                dataStore.dispose();
                cli.getConsole().flush();
            }

        }
    }
}
