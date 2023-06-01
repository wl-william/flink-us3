package org.apache.flink.fs;

import cn.ucloud.ufile.fs.UFileFileSystem;
import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;


/**
 * @author jiangchao
 * @Description:
 * @date 2023/6/1 15:39
 */
public class FlinkUS3FileSystem extends HadoopFileSystem {


    public FlinkUS3FileSystem(UFileFileSystem hadoopFileSystem) {
        super(hadoopFileSystem);
    }

    @Override
    public FileSystemKind getKind() {
        return FileSystemKind.OBJECT_STORE;
    }



}
