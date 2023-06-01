package org.apache.flink.fs;

import cn.ucloud.ufile.fs.UFileFileSystem;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;

import java.io.IOException;
import java.net.URI;

/**
 * @author jiangchao
 * @Description:
 * @date 2023/6/1 15:38
 */
public class US3FileSystemFactory implements FileSystemFactory {
    private Configuration flinkConfig;
    private org.apache.hadoop.conf.Configuration hadoopConfig;

    private static final String[] FLINK_CONFIG_PREFIXES = {"fs.us3."};


    @Override
    public void configure(Configuration config) {
        flinkConfig = config;
        hadoopConfig = null;
    }

    @Override
    public String getScheme() {
        return "us3";
    }

    @Override
    public FileSystem create(URI uri) throws IOException {
        this.hadoopConfig = getHadoopConfiguration();
        final String scheme = uri.getScheme();
        final String authority = uri.getAuthority();

        if (scheme == null && authority == null) {
            uri = org.apache.hadoop.fs.FileSystem.getDefaultUri(hadoopConfig);
        } else if (scheme != null && authority == null) {
            URI defaultUri = org.apache.hadoop.fs.FileSystem.getDefaultUri(hadoopConfig);
            if (scheme.equals(defaultUri.getScheme()) && defaultUri.getAuthority() != null) {
                uri = defaultUri;
            }
        }
        final UFileFileSystem fs = new UFileFileSystem();
        fs.initialize(uri,hadoopConfig);

        return new  FlinkUS3FileSystem(fs);
    }

    @VisibleForTesting
    org.apache.hadoop.conf.Configuration getHadoopConfiguration() {

        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        if (flinkConfig == null) {
            return conf;
        }

        // read all configuration with prefix 'FLINK_CONFIG_PREFIXES'
        for (String key : flinkConfig.keySet()) {
            for (String prefix : FLINK_CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String value = flinkConfig.getString(key, null);
                    conf.set(key, value);
                }
            }
        }
        return conf;
    }
}
