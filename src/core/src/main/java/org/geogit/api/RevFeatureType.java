/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api;

import java.util.ArrayList;
import java.util.List;

import org.geogit.api.plumbing.HashObject;
import org.geotools.feature.simple.SimpleFeatureTypeImpl;
import org.geotools.feature.type.GeometryDescriptorImpl;
import org.geotools.feature.type.GeometryTypeImpl;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.GeometryType;
import org.opengis.feature.type.Name;
import org.opengis.feature.type.PropertyDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * A binary representation of the state of a Feature Type.
 */
public class RevFeatureType extends AbstractRevObject {

    private final FeatureType featureType;

    private ImmutableList<PropertyDescriptor> sortedDescriptors;

    public static RevFeatureType build(FeatureType type) {
        RevFeatureType unnamed = new RevFeatureType(type);
        ObjectId id = new HashObject().setObject(unnamed).call();
        return new RevFeatureType(id, type);
    }

    /**
     * Constructs a new {@code RevFeatureType} from the given {@link FeatureType}.
     * 
     * @param featureType the feature type to use
     */
    private RevFeatureType(FeatureType featureType) {
        this(ObjectId.NULL, featureType);
    }

    /**
     * Constructs a new {@code RevFeatureType} from the given {@link ObjectId} and
     * {@link FeatureType}.
     * 
     * @param id the object id to use for this feature type
     * @param featureType the feature type to use
     */
    public RevFeatureType(ObjectId id, FeatureType featureType) {
        super(id);
        SimpleFeatureType sft = (SimpleFeatureType) featureType;
        List<AttributeDescriptor> descriptors = Lists.newArrayList();
        for (AttributeDescriptor descriptor : sft.getAttributeDescriptors()) {
            if (descriptor instanceof GeometryDescriptor
                    && ((GeometryDescriptor) descriptor).getCoordinateReferenceSystem() == DefaultGeographicCRS.WGS84) {
                // GeoTools treats DefaultGeographic.WGS84 as a special case when calling the
                // CRS.toSRS() method, and that causes inconsistent behaviour. To compensate that,
                // we replace any instance of it with a CRS built using the EPSG:4326 authority,
                // which works consistently when storing it and later recovering it from the
                // database.
                try {
                    CoordinateReferenceSystem crs = CRS.decode("EPSG:4326", true);
                    GeometryDescriptor gd = ((GeometryDescriptor) descriptor);
                    GeometryType type = gd.getType();
                    GeometryType newType = new GeometryTypeImpl(type.getName(), type.getBinding(),
                            crs, type.isIdentified(), type.isAbstract(), type.getRestrictions(),
                            type.getSuper(), type.getDescription());
                    GeometryDescriptor newDescriptor = new GeometryDescriptorImpl(newType,
                            gd.getName(), gd.getMinOccurs(), gd.getMaxOccurs(), gd.isNillable(),
                            gd.getDefaultValue());
                    descriptors.add(newDescriptor);
                } catch (NoSuchAuthorityCodeException e) {
                } catch (FactoryException e) {
                }
            } else {
                descriptors.add(descriptor);
            }
        }
        GeometryDescriptor gd = featureType.getGeometryDescriptor();
        if (gd.getCoordinateReferenceSystem() == DefaultGeographicCRS.WGS84) {
            try {
                CoordinateReferenceSystem crs = CRS.decode("EPSG:4326", true);
                GeometryType type = gd.getType();
                GeometryType newType = new GeometryTypeImpl(type.getName(), type.getBinding(), crs,
                        type.isIdentified(), type.isAbstract(), type.getRestrictions(),
                        type.getSuper(), type.getDescription());
                gd = new GeometryDescriptorImpl(newType, gd.getName(), gd.getMinOccurs(),
                        gd.getMaxOccurs(), gd.isNillable(), gd.getDefaultValue());
            } catch (NoSuchAuthorityCodeException e) {
            } catch (FactoryException e) {
            }
        }
        this.featureType = new SimpleFeatureTypeImpl(featureType.getName(), descriptors, gd,
                featureType.isAbstract(), featureType.getRestrictions(), featureType.getSuper(),
                featureType.getDescription());
        ArrayList<PropertyDescriptor> propertyDescriptors = Lists.newArrayList();
        for (AttributeDescriptor descriptor : descriptors) {
            propertyDescriptors.add(descriptor);
        }
        sortedDescriptors = ImmutableList.copyOf(propertyDescriptors);

    }

    @Override
    public TYPE getType() {
        return TYPE.FEATURETYPE;
    }

    public FeatureType type() {
        return featureType;
    }

    /**
     * @return the sorted {@link PropertyDescriptor}s of the feature type
     */
    public ImmutableList<PropertyDescriptor> sortedDescriptors() {
        return sortedDescriptors;
    }

    /**
     * @return the name of the feature type
     */
    public Name getName() {
        Name name = type().getName();
        return name;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("FeatureType[");
        builder.append(getId().toString());
        builder.append("; ");
        boolean first = true;
        for (PropertyDescriptor desc : sortedDescriptors()) {
            if (first) {
                first = false;
            } else {
                builder.append(", ");
            }
            builder.append(desc.getName().getLocalPart());
            builder.append(": ");
            builder.append(desc.getType().getBinding().getSimpleName());
        }
        builder.append(']');
        return builder.toString();
    }
}
