/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.core.Shape;
import edu.umn.cs.spatialHadoop.nasa.HDFRecordReader;

/**
 * This input format is used to write any spatial file. It automatically decides
 * the format of the file and instantiates the correct record reader based
 * on its file format.
 * 
 * Notice: The key has to be Rectangle (not Partition) because HDF files
 * generate a key of NASADataset which is not Partition. The actual instance
 * of the key is still returned as a Partition if the input file is partitioned.
 * @author Ahmed Eldawy
 *
 */
public class SpatialInputFormat2<K extends Rectangle, V extends Shape>
    extends FileInputFormat<K, Iterable<V>> {
  
  /**
   * Used to check whether files are compressed or not. Some compressed files
   * (e.g., gz) are not splittable.
   */
  private CompressionCodecFactory compressionCodecs = null;

  @SuppressWarnings("unchecked")
  @Override
  public RecordReader<K, Iterable<V>> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    // Create compressionCodecs to be used by isSplitable method
    if (compressionCodecs == null)
      compressionCodecs = new CompressionCodecFactory(job);
    
    if (split instanceof FileSplit) {
      FileSplit fsplit = (FileSplit) split;
      String fname = fsplit.getPath().getName().toLowerCase();
      if (fname.endsWith(".hdf")) {
        // HDF File. Create HDFRecordReader
        return (RecordReader)new HDFRecordReader(job, fsplit,
            job.get(HDFRecordReader.DatasetName),
            job.getBoolean(HDFRecordReader.SkipFillValue, true));
      }
      if (fname.endsWith(".rtree")) {
        // File is locally indexed as RTree
        return (RecordReader)new RTreeRecordReader2<V>(job, (FileSplit)split, reporter);
      }
      return (RecordReader)new SpatialRecordReader2<V>(job, (FileSplit)split, reporter);
    } else {
      throw new RuntimeException("Cannot handle splits of type "+split.getClass());
    }
  }
  
  
}
