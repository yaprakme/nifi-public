// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.nifi.processors.hadoop.util;

import java.util.List;
import java.util.ArrayList;
import org.apache.nifi.components.AllowableValue;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;

public enum CompressionType
{
    NONE("No compression"), 
    DEFAULT("Default ZLIB compression"), 
    BZIP("BZIP compression"), 
    GZIP("GZIP compression"), 
    LZ4("LZ4 compression"), 
    LZO("LZO compression - it assumes LD_LIBRARY_PATH has been set and jar is available"), 
    SNAPPY("Snappy compression"), 
    AUTOMATIC("Will attempt to automatically detect the compression codec.");
    
    private final String description;
    
    private CompressionType(final String description) {
        this.description = description;
    }
    
    private String getDescription() {
        return this.description;
    }
    
    @Override
    public String toString() {
        switch (this) {
            case NONE: {
                return "NONE";
            }
            case DEFAULT: {
                return DefaultCodec.class.getName();
            }
            case BZIP: {
                return BZip2Codec.class.getName();
            }
            case GZIP: {
                return GzipCodec.class.getName();
            }
            case LZ4: {
                return Lz4Codec.class.getName();
            }
            case LZO: {
                return "com.hadoop.compression.lzo.LzoCodec";
            }
            case SNAPPY: {
                return SnappyCodec.class.getName();
            }
            case AUTOMATIC: {
                return "Automatically Detected";
            }
            default: {
                return null;
            }
        }
    }
    
    public static AllowableValue[] allowableValues() {
        final List<AllowableValue> values = new ArrayList<AllowableValue>();
        for (final CompressionType type : values()) {
            values.add(new AllowableValue(type.name(), type.name(), type.getDescription()));
        }
        return values.toArray(new AllowableValue[values.size()]);
    }
}
