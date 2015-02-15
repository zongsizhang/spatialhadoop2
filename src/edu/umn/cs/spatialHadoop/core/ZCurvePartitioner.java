/***********************************************************************
* Copyright (c) 2015 by Regents of the University of Minnesota.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which 
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*
*************************************************************************/
package edu.umn.cs.spatialHadoop.core;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.umn.cs.spatialHadoop.OperationsParams;
import edu.umn.cs.spatialHadoop.operations.FileMBR;
import edu.umn.cs.spatialHadoop.operations.Sampler;
import edu.umn.cs.spatialHadoop.util.FileUtil;

/**
 * Partition the space based on Z-curve.
 * @author Ahmed Eldawy
 *
 */
public class ZCurvePartitioner extends Partitioner {
  private static final Log LOG = LogFactory.getLog(ZCurvePartitioner.class);

  /**MBR of the input file*/
  private Rectangle mbr;
  /**Upper bound of all partitions*/
  private long[] zSplits;

  private static final int Resolution = Integer.MAX_VALUE;
  
  /**
   * A default constructor to be able to dynamically instantiate it
   * and deserialize it
   */
  public ZCurvePartitioner() {
  }

  /**
   * Constructs a new grid partitioner which is used for indexing
   * @param inPath
   * @param job
   * @throws IOException 
   */
  public static ZCurvePartitioner createIndexingPartitioner(Path inPath,
      Path outPath, JobConf job) throws IOException {
    final Rectangle inMBR = (Rectangle) OperationsParams.getShape(job, "mbr");
    // Determine number of partitions
    long inSize = FileUtil.getPathSize(inPath.getFileSystem(job), inPath);
    FileSystem outFS = outPath.getFileSystem(job);
    long outBlockSize = outFS.getDefaultBlockSize(outPath);
    int partitions = (int) (inSize / outBlockSize);
    LOG.info("Z-cruve to partition the space into "+partitions+" partitions");
    
    // Sample of the input file and each point is mapped to a Z-value
    final Vector<Long> vsample = new Vector<Long>();
    
    float sample_ratio = job.getFloat(SpatialSite.SAMPLE_RATIO, 0.01f);
    long sample_size = job.getLong(SpatialSite.SAMPLE_SIZE, 100 * 1024 * 1024);
    
    LOG.info("Reading a sample of "+(int)Math.round(sample_ratio*100) + "%");
    ResultCollector<Point> resultCollector = new ResultCollector<Point>(){
      @Override
      public void collect(Point value) {
        vsample.add(computeZ(inMBR, value.x, value.y));
      }
    };
    OperationsParams params2 = new OperationsParams(job);
    params2.setFloat("ratio", sample_ratio);
    params2.setLong("size", sample_size);
    params2.setClass("outshape", Point.class, Shape.class);
    Sampler.sample(new Path[] {inPath}, resultCollector, params2);
    LOG.info("Finished reading a sample of "+vsample.size()+" records");
    
    // Apply the STR algorithm in two rounds
    // 1- First round, sort points by X and split into the given columns
    Collections.sort(vsample);
    
    ZCurvePartitioner p = new ZCurvePartitioner();
    p.mbr = new Rectangle(inMBR);
    p.zSplits = new long[partitions];
    long maxZ = computeZ(inMBR, inMBR.x2, inMBR.y2);
    for (int i = 0; i < partitions; i++) {
      int quantile = (i + 1) * vsample.size() / partitions;
      p.zSplits[i] = quantile == vsample.size() ? maxZ : vsample.get(quantile);
    }
    
    return p;
  }
  
  /**
   * Computes the Z-order of a point relative to a containing rectangle
   * @param mbr
   * @param x
   * @param y
   * @return
   */
  public static long computeZ(Rectangle mbr, double x, double y) {
    int ix = (int) ((x - mbr.x1) * Resolution / mbr.getWidth());
    int iy = (int) ((y - mbr.y1) * Resolution / mbr.getHeight());
    return computeZOrder(ix, iy);
  }
  
  /**
   * Reverse the computation of the Z-value and returns x and y dimensions
   * @param mbr
   * @param x
   * @param y
   */
  public static void uncomputeZ(Rectangle mbr, long z, Point outPoint) {
    long ixy = unComputeZOrder(z);
    int ix = (int) (ixy >> 32);
    int iy = (int) (ixy & 0xffffffffL);
    outPoint.x = (double)(ix) * mbr.getWidth() / Resolution + mbr.x1;
    outPoint.y = (double)(iy) * mbr.getHeight() / Resolution + mbr.y1;
  }
  
  /**
   * Computes the Z-order (Morton order) of a two-dimensional point.
   * @param x - integer value of the x-axis (cannot exceed Integer.MAX_VALUE)
   * @param y - integer value of the y-axis (cannot exceed Integer.MAX_VALUE)
   * @return
   */
  public static long computeZOrder(long x, long y) {
    long morton = 0;
  
    for (long bitPosition = 0; bitPosition < 32; bitPosition++) {
      long mask = 1L << bitPosition;
      morton |= (x & mask) << (bitPosition + 1);
      morton |= (y & mask) << bitPosition;
    }
    return morton;
  }
  
  public static java.awt.Point unComputeZOrder(long morton, java.awt.Point point) {
    long ixy = unComputeZOrder(morton);
    point.x = (int) (ixy >>> 32);
    point.y = (int) (ixy & 0xffffffff);
    return point;
  }
  
  public static long unComputeZOrder(long morton) {
    long x = 0, y = 0;
    for (long bitPosition = 0; bitPosition < 32; bitPosition++) {
      long mask = 1L << (bitPosition << 1);
      y |= (morton & mask) >> bitPosition;
      x |= (morton & (mask << 1)) >> (bitPosition + 1);
    }
    return (x << 32) | y;
  }
  
  /**
   * Compute the minimal bounding rectangle MBR of a range on the Z-curve.
   * Notice that getting the MBR of the two end points does not always work.
   * 
   * @param zMin
   * @param zMax
   * @return
   */
  public static java.awt.Rectangle getMBRInteger(long zMin, long zMax) {
    long changedBits = zMin ^ zMax;
    // The mask contains 1's for all bits that are less or equal significant
    // to any changed bit
    long mask = changedBits;
    long oldMask;
    do {
      oldMask = mask;
      mask |= (mask >> 1);
    } while (mask != oldMask);
    // Both zMin and zMax can be used in the following equations because we
    // explicitly set all different bits
    java.awt.Point minXY = unComputeZOrder(zMin & (~mask), new java.awt.Point());
    java.awt.Point maxXY = unComputeZOrder(zMin | mask, new java.awt.Point());
    java.awt.Rectangle mbr = new java.awt.Rectangle(minXY.x, minXY.y, maxXY.x - minXY.x, maxXY.y - minXY.y);
    return mbr;
  }
  
  public static Rectangle getMBR(Rectangle mbr, long zMin, long zMax) {
    java.awt.Rectangle mbrInteger = getMBRInteger(zMin, zMax);
    Rectangle trueMBR = new Rectangle();
    trueMBR.x1 = (double)(mbrInteger.x) * mbr.getWidth() / Resolution + mbr.x1;
    trueMBR.y1 = (double)(mbrInteger.y) * mbr.getHeight() / Resolution + mbr.y1;
    trueMBR.x2 = (double)(mbrInteger.getMaxX()) * mbr.getWidth() / Resolution + mbr.x1;
    trueMBR.y2 = (double)(mbrInteger.getMaxY()) * mbr.getHeight() / Resolution + mbr.y1;
    return trueMBR;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    mbr.write(out);
    out.writeInt(zSplits.length);
    ByteBuffer bbuffer = ByteBuffer.allocate(zSplits.length * 8);
    for (long zSplit : zSplits)
      bbuffer.putLong(zSplit);
    if (bbuffer.hasRemaining())
      throw new RuntimeException("Did not calculate buffer size correctly");
    out.write(bbuffer.array(), bbuffer.arrayOffset(), bbuffer.position());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (mbr == null)
      mbr = new Rectangle();
    mbr.readFields(in);
    int partitionCount = in.readInt();
    zSplits = new long[partitionCount];
    int bufferLength = 8 * partitionCount;
    byte[] buffer = new byte[bufferLength];
    in.readFully(buffer);
    ByteBuffer bbuffer = ByteBuffer.wrap(buffer);
    for (int i = 0; i < partitionCount; i++) {
      zSplits[i] = bbuffer.getLong();
    }
    if (bbuffer.hasRemaining())
      throw new RuntimeException("Error reading STR partitioner");
  }
  
  @Override
  public int getPartitionCount() {
    return zSplits.length;
  }

  @Override
  public void overlapPartitions(Shape shape, ResultCollector<Integer> matcher) {
    if (shape == null)
      return;
    Rectangle shapeMBR = shape.getMBR();
    if (shapeMBR == null)
      return;
    // Assign to only one partition that contains the center point
    Point center = shapeMBR.getCenterPoint();
    long zValue = computeZ(mbr, center.x, center.y);
    int partition = Arrays.binarySearch(zSplits, zValue);
    if (partition < 0)
      partition = -partition - 1;
    matcher.collect(partition);
  }

  @Override
  public CellInfo getPartition(int id) {
    CellInfo cell = new CellInfo();
    cell.cellId = id;
    long zMax = zSplits[id];
    long zMin = id == 0? 0 : zSplits[id-1];
    
    Rectangle cellMBR = getMBR(mbr, zMin, zMax);
    cell.set(cellMBR);
    return cell;
  }
  
  public static void main(String[] args) throws IOException {
    OperationsParams params = new OperationsParams(new GenericOptionsParser(args));
    
    
    Path inPath = params.getInputPath();
    Path outPath = params.getOutputPath();

    OperationsParams.setShape(params, "mbr", FileMBR.fileMBR(inPath, params));
    
    ZCurvePartitioner p = createIndexingPartitioner(inPath, outPath, new JobConf(params));
    for (int i = 0; i < p.getPartitionCount(); i++) {
      System.out.println(p.getPartition(i).toWKT());
    }
  }
}