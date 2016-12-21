package edu.umn.cs.spatialHadoop.delaunay;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Stack;
import java.util.Vector;

import edu.umn.cs.spatialHadoop.core.SpatialAlgorithms;
import edu.umn.cs.spatialHadoop.util.MergeSorter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.*;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;

import edu.umn.cs.spatialHadoop.core.Point;
import edu.umn.cs.spatialHadoop.core.Rectangle;
import edu.umn.cs.spatialHadoop.util.BitArray;
import edu.umn.cs.spatialHadoop.util.IntArray;

/**
 * The divide and conquer Delaunay Triangulation (DT) algorithm as proposed in
 * L. J Guibas and J. Stolfi, "Primitives for the manipulation of general
 * subdivisions and the computation of Voronoi diagrams",
 * ACM Transactions on Graphics, 4(1985), 74-123,
 * and as further illustrated in
 * http://www.geom.uiuc.edu/~samuelp/del_project.html
 * @author Ahmed Eldawy
 *
 */
public class GSDTAlgorithm {
  
  static final Log LOG = LogFactory.getLog(GSDTAlgorithm.class);
  
  /** The original input set of points */
  Point[] points;

  /** A bitmask with a bit set for each site that has been previously reported
   * to the output. This is needed when merging intermediate triangulations to
   * avoid reporting the same output twice. If <code>null</code>, it indicates
   * that no sites have been previously reported.
   */
  BitArray reportedSites;

  /** Coordinates of all sites */
  double[] xs, ys;

  /**
   * All neighboring sites. Two neighbor sites have a common edge in the DT
   */
  IntArray[] neighbors;

  /**
   * Stores the final answer that contains the complete DT
   */
  private IntermediateTriangulation finalAnswer;

  /**
   * Use to report progress to avoid mapper or reducer timeout
   */
  protected Progressable progress;

  /**
   * A class that stores a set of triangles for part of the sites.
   * @author Ahmed Eldawy
   *
   */
  class IntermediateTriangulation {
    /**
     * The contiguous range of sites stored at this triangulation.
     * The range is inclusive of both site1 and site2
     */
    int site1, site2;
    /**Sites on the convex null of the triangulation*/
    int[] convexHull;

    /**
     * Constructs an empty triangulation to be used for merging
     */
    IntermediateTriangulation() {}
    
    /**
     * Initialize a triangulation with two sites only. The triangulation consists
     * of one line connecting the two sites and no triangles at all.
     * @param s1
     * @param s2
     */
    IntermediateTriangulation(int s1, int s2) {
      site1 = s1;
      site2 = s2;
      neighbors[s1].add(s2);
      neighbors[s2].add(s1);
      convexHull = new int[] {s1, s2};
    }
    
    /**
     * Initialize a triangulation with three sites. The trianglation consists
     * of a single triangles connecting the three points.
     * @param s1
     * @param s2
     * @param s3
     */
    IntermediateTriangulation(int s1, int s2, int s3) {
      site1 = s1;
      site2 = s3;
      neighbors[s1].add(s2); neighbors[s2].add(s1); // edge: s1 -- s2
      neighbors[s2].add(s3); neighbors[s3].add(s2); // edge: s3 -- s3
      if (calculateCircumCircleCenter(s1, s2, s3) == null) {
        // Degenerate case, three points are collinear
        convexHull = new int[] {s1, s3};
      } else {
        // Normal case
        neighbors[s1].add(s3); neighbors[s3].add(s1); // edge: s1 -- s3
        convexHull = new int[] {s1, s2, s3};
      }
    }

    /**
     * Create an intermediate triangulation out of a triangulation created
     * somewhere else (may be another machine). It stores all the edges in the
     * neighbors array and adjusts the node IDs in edges to match their new
     * position in the {@link GSDTAlgorithm#points} array
     * 
     * @param t
     *          The triangulation that needs to be added
     * @param pointShift
     *          How much shift should be added to each edge to adjust it to the
     *          new points array
     */
    public IntermediateTriangulation(Triangulation t, int pointShift) {
      // Assume that points have already been copied
      this.site1 = pointShift;
      this.site2 = pointShift + t.sites.length - 1;
      
      // Calculate the convex hull
      int[] thisPoints = new int[t.sites.length];
      for (int i = 0; i < thisPoints.length; i++)
        thisPoints[i] = i + pointShift;
      this.convexHull = convexHull(thisPoints);
      
      // Copy all edges to the neighbors list
      for (int i = 0; i < t.edgeStarts.length; i++) {
        int adjustedStart = t.edgeStarts[i] + pointShift;
        int adjustedEnd = t.edgeEnds[i] + pointShift;
        neighbors[adjustedStart].add(adjustedEnd);
      }
    }

    @Override
    public String toString() {
      return String.format("Triangulation: [%d, %d]", site1, site2);
    }

    /**
     * Perform some sanity checks to see if the current triangulation could
     * be incorrect. This can be used to find as early as possible when the
     * output becomes bad.
     * @return
     */
    public boolean isIncorrect() {
      // Test if there are any overlapping edges
      // First, compute the MBR of all edges
      class Edge extends Rectangle {
        int source, destination;

        public Edge(int s, int d) {
          this.source = s;
          this.destination = d;
          super.set(xs[s], ys[s], xs[d], ys[d]);
        }
      }

      List<Edge> edges = new ArrayList<Edge>();

      for (int s = site1; s <= site2; s++) {
        for (int d : neighbors[s]) {
          // Add each undirected edge only once
          if (s < d)
            edges.add(new Edge(s, d));
        }
      }

      Edge[] aredges = edges.toArray(new Edge[edges.size()]);
      final BooleanWritable correct = new BooleanWritable(true);

      try {
        SpatialAlgorithms.SelfJoin_rectangles(aredges, new OutputCollector<Edge, Edge>() {
          static final double Threshold = 1E-5;
          @Override
          public void collect(Edge e1, Edge e2) throws IOException {
            double x1 = xs[e1.source];
            double y1 = ys[e1.source];
            double x2 = xs[e1.destination];
            double y2 = ys[e1.destination];
            double x3 = xs[e2.source];
            double y3 = ys[e2.source];
            double x4 = xs[e2.destination];
            double y4 = ys[e2.destination];

            double den = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
            double ix = (x1 * y2 - y1 * x2) * (x3 - x4) / den - (x1 - x2) * (x3 * y4 - y3 * x4) / den;
            double iy = (x1 * y2 - y1 * x2) * (y3 - y4) / den - (y1 - y2) * (x3 * y4 - y3 * x4) / den;
            double minx1 = Math.min(x1, x2);
            double maxx1 = Math.max(x1, x2);
            double miny1 = Math.min(y1, y2);
            double maxy1 = Math.max(y1, y2);
            double minx2 = Math.min(x3, x4);
            double maxx2 = Math.max(x3, x4);
            double miny2 = Math.min(y3, y4);
            double maxy2 = Math.max(y3, y4);
            if ((ix - minx1 > Threshold && ix - maxx1 < -Threshold) && (iy - miny1 > Threshold && iy - maxy1 < -Threshold) &&
                (ix - minx2 > Threshold && ix - maxx2 < -Threshold) && (iy - miny2 > Threshold && iy - maxy2 < -Threshold)) {
              System.out.printf("line %f, %f, %f, %f\n", x1, y1, x2, y2);
              System.out.printf("line %f, %f, %f, %f\n", x3, y3, x4, y4);
              System.out.printf("circle %f, %f, 0.5\n", ix, iy);
              correct.set(false);
            }
          }
        }, null);

        // Test if all the lines of the convex hull are edges
        int[] sites = new int[this.site2 - this.site1 + 1];
        for (int i = site1; i <= site2; i++)
          sites[i - this.site1] = i;
        int[] ch = convexHull(sites);
        for (int i = 0; i < ch.length; i++) {
          int s = ch[i];
          int d = ch[(i+1)%ch.length];
          if (s > d) {
            // Swap to make it easier to search
            int t = s;
            s = d;
            d = t;
          }
          boolean found = false;
          for (int iEdge = 0; !found && iEdge < aredges.length; iEdge++) {
            found = s == aredges[iEdge].source && d == aredges[iEdge].destination;
          }
          if (!found) {
            correct.set(false);
            System.out.printf("Edge %d, %d on the convex hull and not found in the DT\n", s, d);
          }
        }

      } catch (IOException e) {
        e.printStackTrace();
        return false;
      }

      return !correct.get();
    }

    public void draw(PrintStream out) {
      // Draw to rasem
      out.println("group {");
      for (int i = site1; i <= site2; i++) {
        out.printf("text(%f, %f) { raw '%d' }\n", xs[i], ys[i], i);
      }
      out.println("}");
      out.println("group {");
      for (int i = site1; i <= site2; i++) {
        for (int j : neighbors[i]) {
          out.printf("line %f, %f, %f, %f\n", xs[i], ys[i], xs[j], ys[j]);
        }
      }
      out.println("}");
    }
  }

  /**
   * Constructs a triangulation that merges two existing triangulations.
   * @param inPoints
   * @param progress
   */
  public <P extends Point> GSDTAlgorithm(P[] inPoints, Progressable progress) {
    this.progress = progress;
    this.points = new Point[inPoints.length];
    this.xs = new double[points.length];
    this.ys = new double[points.length];
    this.neighbors = new IntArray[points.length];
    for (int i = 0; i < points.length; i++)
      neighbors[i] = new IntArray();
    this.reportedSites = new BitArray(points.length); // Initialized to zeros
    System.arraycopy(inPoints, 0, points, 0, points.length);

    // Compute the answer
    this.finalAnswer = computeTriangulation(0, points.length);
  }

  /**
   * Performs the actual computation for the given subset of the points array.
   */
  protected IntermediateTriangulation computeTriangulation(int start, int end) {
    // Sort all points by x-coordinates to prepare for processing
    // Sort points by their containing cell
    Arrays.sort(points, new Comparator<Point>() {
      @Override
      public int compare(Point p1, Point p2) {
        int dx = Double.compare(p1.x, p2.x);
        if (dx != 0)
          return dx;
        return Double.compare(p1.y, p2.y);
      }
    });

    // Store all coordinates in primitive arrays for efficiency
    for (int i = start; i < end; i++) {
      xs[i] = points[i].x;
      ys[i] = points[i].y;
    }


    int size = end - start;
    IntermediateTriangulation[] triangulations = new IntermediateTriangulation[size / 3 + (size % 3 == 0 ? 0 : 1)];
    // Compute the trivial Delaunay triangles of every three consecutive points
    int i, t = 0;
    for (i = start; i < end - 4; i += 3) {
      // Compute DT for three points
      triangulations[t++] =  new IntermediateTriangulation(i, i+1, i+2);
      if (progress != null && (t & 0xff) == 0)
        progress.progress();
    }
    if (end - i == 4) {
       // Compute DT for every two points
       triangulations[t++] = new IntermediateTriangulation(i, i+1);
       triangulations[t++] = new IntermediateTriangulation(i+2, i+3);
    } else if (end - i == 3) {
      // Compute for three points
      triangulations[t++] = new IntermediateTriangulation(i, i+1, i+2);
    } else if (end - i == 2) {
      // Two points, connect with a line
      triangulations[t++] = new IntermediateTriangulation(i, i+1);
    } else {
      throw new RuntimeException("Cannot happen");
    }

    if (progress != null)
      progress.progress();
    return mergeAllTriangulations(triangulations);
  }

  /**
   * Compute the DT by merging existing triangulations created at different
   * machines. The given triangulations should be sorted correctly such that
   * they can be merged in their current sort order.
   * @param ts
   * @param progress
   */
  public GSDTAlgorithm(Triangulation[] ts, Progressable progress) {
    this.progress = progress;
    // Copy all triangulations
    int totalPointCount = 0;
    for (Triangulation t : ts)
      totalPointCount += t.sites.length;
    
    this.points = new Point[totalPointCount];
    this.reportedSites = new BitArray(totalPointCount);
    // Initialize xs, ys and neighbors array
    this.xs = new double[totalPointCount];
    this.ys = new double[totalPointCount];
    this.neighbors = new IntArray[totalPointCount];
    for (int i = 0; i < this.neighbors.length; i++)
      this.neighbors[i] = new IntArray();
    
    IntermediateTriangulation[] triangulations = new IntermediateTriangulation[ts.length];
    int currentPointsCount = 0;
    for (int it = 0; it < ts.length; it++) {
      Triangulation t = ts[it];
      // Copy sites from that triangulation
      System.arraycopy(t.sites, 0, this.points, currentPointsCount, t.sites.length);
      
      // Set xs and ys for all points
      for (int i = currentPointsCount; i < currentPointsCount + t.sites.length; i++) {
        this.xs[i] = points[i].x;
        this.ys[i] = points[i].y;
        this.reportedSites.set(i, t.reportedSites.get(i - currentPointsCount));
      }
      
      // Create a corresponding partial answer
      triangulations[it] = new IntermediateTriangulation(t, currentPointsCount);
      
      currentPointsCount += t.sites.length;
    }

    if (progress != null)
      progress.progress();
    this.finalAnswer = mergeAllTriangulations(triangulations);
  }
  
  /**
   * Merges a set of triangulations in any sort order. First, the triangulations
   * are sorted in columns and each column is merged separately. After that,
   * columns are merged together to produce one final answer.
   * @param triangulations
   * @param progress
   * @return
   */
  static GSDTAlgorithm mergeTriangulations(
      List<Triangulation> triangulations, Progressable progress) {
    // Arrange triangulations column-by-column
    List<List<Triangulation>> columns = new ArrayList<List<Triangulation>>();
    int numTriangulations = 0;
    for (Triangulation t : triangulations) {
      double x1 = t.mbr.x1, x2 = t.mbr.x2;
      List<Triangulation> selectedColumn = null;
      int iColumn = 0;
      while (iColumn < columns.size() && selectedColumn == null) {
        Rectangle cmbr = columns.get(iColumn).get(0).mbr;
        double cx1 = cmbr.x1;
        double cx2 = cmbr.x2;
        if (x2 > cx1 && cx2 > x1) {
          selectedColumn = columns.get(iColumn);
        } else {
          iColumn++;
        }
      }
      if (selectedColumn == null) {
        // Create a new column
        selectedColumn = new ArrayList<Triangulation>();
        columns.add(selectedColumn);
      }
      selectedColumn.add(t);
      numTriangulations++;
    }
    
    LOG.info("Merging "+numTriangulations+" triangulations in "+columns.size()+" columns" );
    
    List<Triangulation> mergedColumns = new ArrayList<Triangulation>();
    // Merge all triangulations together column-by-column
    for (List<Triangulation> column : columns) {
      // Sort this column by y-axis
      Collections.sort(column, new Comparator<Triangulation>() {
        @Override
        public int compare(Triangulation t1, Triangulation t2) {
          double dy = t1.mbr.y1 - t2.mbr.y1;
          if (dy < 0)
            return -1;
          if (dy > 0)
            return 1;
          return 0;
        }
      });
  
      LOG.info("Merging "+column.size()+" triangulations vertically");
      GSDTAlgorithm algo =
          new GSDTAlgorithm(column.toArray(new Triangulation[column.size()]), progress);
      mergedColumns.add(algo.getFinalTriangulation());
    }
    
    // Merge the result horizontally
    Collections.sort(mergedColumns, new Comparator<Triangulation>() {
      @Override
      public int compare(Triangulation t1, Triangulation t2) {
        double dx = t1.mbr.x1 - t2.mbr.x1;
        if (dx < 0)
          return -1;
        if (dx > 0)
          return 1;
        return 0;
      }
    });
    LOG.info("Merging "+mergedColumns.size()+" triangulations horizontally");
    GSDTAlgorithm algo = new GSDTAlgorithm(
        mergedColumns.toArray(new Triangulation[mergedColumns.size()]),
        progress);
    return algo;
  }

  /**
   * Merge two adjacent triangulations into one
   * @param L
   * @param R
   * @return
   */
  protected IntermediateTriangulation merge(IntermediateTriangulation L, IntermediateTriangulation R) {
    IntermediateTriangulation merged = new IntermediateTriangulation();
    // Compute the convex hull of the result
    int[] bothHulls = new int[L.convexHull.length + R.convexHull.length];
    System.arraycopy(L.convexHull, 0, bothHulls, 0, L.convexHull.length);
    System.arraycopy(R.convexHull, 0, bothHulls, L.convexHull.length, R.convexHull.length);
    merged.convexHull = convexHull(bothHulls);

    // TODO avoid computing the circumcircle and test if a point is within the circle or not
    
    // Find the base LR-edge (lowest edge of the convex hull that crosses from L to R)
    int[] baseEdge = findBaseEdge(merged.convexHull, L, R);
    int baseL = baseEdge[0];
    int baseR = baseEdge[1];
  
    // Add the first base edge
    neighbors[baseL].add(baseR);
    neighbors[baseR].add(baseL);
    // Trace the base LR edge up to the top
    boolean finished = false;
    do { // Until the finished flag is raised
      // Search for the potential candidate on the right
      double anglePotential = -1, angleNextPotential = -1;
      int potentialCandidate = -1, nextPotentialCandidate = -1;
      for (int rNeighbor : neighbors[baseR]) {
        if (rNeighbor >= R.site1 && rNeighbor <= R.site2) {
          // Check this RR edge
          double cwAngle = calculateCWAngle(baseL, baseR, rNeighbor);
          if (potentialCandidate == -1 || cwAngle < anglePotential) {
            // Found a new potential candidate
            angleNextPotential = anglePotential;
            nextPotentialCandidate = potentialCandidate;
            anglePotential = cwAngle;
            potentialCandidate = rNeighbor;
          } else if (nextPotentialCandidate == -1 || cwAngle < angleNextPotential) {
            angleNextPotential = cwAngle;
            nextPotentialCandidate = rNeighbor;
          }
        }
      }
      int rCandidate = -1;
      if (anglePotential < Math.PI && potentialCandidate != -1) {
        // Compute the circum circle between the base edge and potential candidate
        Point circleCenter = calculateCircumCircleCenter(baseL, baseR, potentialCandidate);
        if (circleCenter != null) {
          if (nextPotentialCandidate == -1) {
            // The only potential candidate, accept it right away
            rCandidate = potentialCandidate;
          } else {
            // Check if the circumcircle of the base edge with the potential
            // candidate contains the next potential candidate
            double dx = circleCenter.x - xs[nextPotentialCandidate];
            double dy = circleCenter.y - ys[nextPotentialCandidate];
            double d1 = dx * dx + dy * dy;
            dx = circleCenter.x - xs[potentialCandidate];
            dy = circleCenter.y - ys[potentialCandidate];
            double d2 = dx * dx + dy * dy;
            if (d1 < d2) {
              // Delete the RR edge between baseR and rPotentialCandidate and restart
              neighbors[baseR].remove(potentialCandidate);
              neighbors[potentialCandidate].remove(baseR);
              continue;
            } else {
              rCandidate = potentialCandidate;
            }
          }
        }
      }
      
      // Search for the potential candidate on the left
      anglePotential = -1; angleNextPotential = -1;
      potentialCandidate = -1; nextPotentialCandidate = -1;
      for (int lNeighbor : neighbors[baseL]) {
        if (lNeighbor >= L.site1 && lNeighbor <= L.site2) {
          // Check this LL edge
          double ccwAngle = Math.PI * 2 - calculateCWAngle(baseR, baseL, lNeighbor);
          if (potentialCandidate == -1 || ccwAngle < anglePotential) {
            // Found a new potential candidate
            angleNextPotential = anglePotential;
            nextPotentialCandidate = potentialCandidate;
            anglePotential = ccwAngle;
            potentialCandidate = lNeighbor;
          } else if (nextPotentialCandidate == -1 || ccwAngle < angleNextPotential) {
            angleNextPotential = ccwAngle;
            nextPotentialCandidate = lNeighbor;
          }
        }
      }
      int lCandidate = -1;
      if (anglePotential - Math.PI < -1E-9 && potentialCandidate != -1) {
        // Compute the circum circle between the base edge and potential candidate
        Point circleCenter = calculateCircumCircleCenter(baseL, baseR, potentialCandidate);
        if (circleCenter != null) {
          if (nextPotentialCandidate == -1) {
            // The only potential candidate, accept it right away
            lCandidate = potentialCandidate;
          } else {
            // Check if the circumcircle of the base edge with the potential
            // candidate contains the next potential candidate
            double dx = circleCenter.x - xs[nextPotentialCandidate];
            double dy = circleCenter.y - ys[nextPotentialCandidate];
            double d1 = dx * dx + dy * dy;
            dx = circleCenter.x - xs[potentialCandidate];
            dy = circleCenter.y - ys[potentialCandidate];
            double d2 = dx * dx + dy * dy;
            if (d1 < d2) {
              // Delete the LL edge between baseL and potentialCandidate and restart
              neighbors[baseL].remove(potentialCandidate);
              neighbors[potentialCandidate].remove(baseL);
              continue;
            } else {
              lCandidate = potentialCandidate;
            }
          } // nextPotentialCandidate == -1
        } // circleCenter == null
      } // anglePotential < Math.PI      
      // Choose the right candidate
      if (lCandidate != -1 && rCandidate != -1) {
        // Two candidates, choose the correct one
        Point circumCircleL = calculateCircumCircleCenter(baseL, baseR, lCandidate);
        double dx = circumCircleL.x - xs[lCandidate];
        double dy = circumCircleL.y - ys[lCandidate];
        double lCandidateDistance = dx * dx + dy * dy;
        dx = circumCircleL.x - xs[rCandidate];
        dy = circumCircleL.y - ys[rCandidate];
        double rCandidateDistance = dx * dx + dy * dy;
        if (lCandidateDistance < rCandidateDistance) {
          // rCandidate is outside the circumcircle, lCandidate is correct
          rCandidate = -1;
        } else {
          // rCandidate is inside the circumcircle, lCandidate is incorrect
          lCandidate = -1;
        }
      }
      
      if (lCandidate != -1) {
        // Left candidate has been chosen
        // Make lPotentialCandidate and baseR the new base line
        baseL = lCandidate;
        // Add the new base edge
        neighbors[baseL].add(baseR);
        neighbors[baseR].add(baseL);
      } else if (rCandidate != -1) {
        // Right candidate has been chosen
        // Make baseL and rPotentialCandidate the new base line
        baseR = rCandidate;
        // Add the new base edge
        neighbors[baseL].add(baseR);
        neighbors[baseR].add(baseL);
      } else {
        // No candidates, merge finished
        finished = true;
      }
    } while (!finished);
    
    // Merge sites of both L and R
    merged.site1 = L.site1;
    merged.site2 = R.site2;
    return merged;
  }

  /**
   * Computes the base edge that is used to merge two triangulations.
   * There are at most two crossing edges from L to R, the base edge is the one
   * that makes CW angles with sites in the range [0, PI]. 
   * @param mergedConvexHull
   * @param L
   * @param R
   * @return
   */
  private int[] findBaseEdge(int[] mergedConvexHull, IntermediateTriangulation L,
      IntermediateTriangulation R) {
    int base1L = -1, base1R = -1; // First LR edge
    int base2L = -1, base2R = -1; // Second LR edge
    for (int i = 0; i < mergedConvexHull.length; i++) {
      int p1 = mergedConvexHull[i];
      int p2 = i == mergedConvexHull.length - 1 ? mergedConvexHull[0] : mergedConvexHull[i+1];
      if (inArray(L.convexHull, p1) && inArray(R.convexHull, p2)) {
        // Found an LR edge, store it
        if (base1L == -1) {
          // First LR edge
          base1L = p1;
          base1R = p2;
        } else {
          // Second LR edge
          base2L = p1;
          base2R = p2;
        }
      } else if (inArray(L.convexHull, p2) && inArray(R.convexHull, p1)) {
        if (base1L == -1) {
          // First LR edge
          base1L = p2;
          base1R = p1;
        } else {
          // Second LR edge
          base2L = p2;
          base2R = p1;
        }
      }
    }
    
    // Choose the right LR edge. The one which makes an angle [0, 180] with
    // each point in both partitions
    // Compute the angle of the first LR edge and test if it qualifies
    // Instead of testing with all the points, we test with one of the two points
    // on the second LR edge as they are probably two different points
    double cwAngleFirst;
    if (base1R == base2R) {
      // Both LR edges share the right point, can only test with the left
      // point of the second edge
      cwAngleFirst = calculateCWAngle(base1L, base1R, base2L);
    } else if (base1L == base2L) {
      // Both LR edges share the  left point, can only test with the right
      // point of the second edge
      cwAngleFirst = calculateCWAngle(base1L, base1R, base2R);
    } else {
      // The two LR edges do not share any points, we have the luxury of choosing
      // which point on the right edge to test. We will choose the point that
      // does not give an angle of PI which indicates three collinear points
      cwAngleFirst = calculateCWAngle(base1L, base1R, base2R);
      if (cwAngleFirst == Math.PI)
        cwAngleFirst = calculateCWAngle(base1L, base1R, base2L);
    }

    return cwAngleFirst < Math.PI ? new int[] {base1L, base1R} :
      new int[] {base2L, base2R};
  }

  /**
   * Merge all triangulations into one
   * @param triangulations
   * @return 
   */
  protected IntermediateTriangulation mergeAllTriangulations(IntermediateTriangulation[] triangulations) {
    long reportTime = 0;
    // Start the merge process
    while (triangulations.length > 1) {
      long currentTime = System.currentTimeMillis();
      if (currentTime - reportTime > 1000) {
        LOG.info("Merging "+triangulations.length+" triangulations");
        reportTime = currentTime;
      }
      // Merge every pair of DTs
      IntermediateTriangulation[] newTriangulations = new IntermediateTriangulation[triangulations.length / 2 + (triangulations.length & 1)];
      int t2 = 0;
      int t1;
      for (t1 = 0; t1 < triangulations.length - 1; t1 += 2) {
        IntermediateTriangulation dt1 = triangulations[t1];
        IntermediateTriangulation dt2 = triangulations[t1+1];
        newTriangulations[t2++] = merge(dt1, dt2);
        if (progress != null)
          progress.progress();
      }
      if (t1 < triangulations.length)
        newTriangulations[t2++] = triangulations[t1];
      triangulations = newTriangulations;
    }
    return triangulations[0];
  }

  /**
   * Returns the final answer as a triangulation.
   * @return
   */
  public Triangulation getFinalTriangulation() {
    Triangulation result = new Triangulation();

    result.sites = this.points.clone();
    result.reportedSites = this.reportedSites;
    result.sitesToReport = this.reportedSites.invert();
    int numEdges = 0;
    result.mbr = new Rectangle(Double.MAX_VALUE, Double.MAX_VALUE,
        -Double.MAX_VALUE, -Double.MAX_VALUE);
    for (int s1 = 0; s1 < this.neighbors.length; s1++) {
      //if (this.neighbors[s1].isEmpty())
      //  throw new RuntimeException("A site has no incident edges");
      numEdges += this.neighbors[s1].size();
      result.mbr.expand(result.sites[s1]);
    }
    // We store each undirected edge twice, once for each direction
    result.edgeStarts = new int[numEdges];
    result.edgeEnds = new int[numEdges];

    for (int s1 = 0; s1 < this.neighbors.length; s1++) {
      for (int s2 : this.neighbors[s1]) {
        numEdges--;
        result.edgeStarts[numEdges] = s1;
        result.edgeEnds[numEdges] = s2;
      }
    }
    result.sortEdges();
    if (numEdges != 0)
      throw new RuntimeException("Error in edges! Copied "+
    (result.edgeStarts.length - numEdges)+" instead of "+result.edgeStarts.length);
  
    return result;
  }
  
  /**
   * Computes the final answer as a set of Voronoi regions. Each region is
   * represented by a Geometry. The associated site for each Voronoi region is
   * stored as user data inside the geometry. The polygon that represents each
   * region is clipped to the given rectangle (MBR) to ensure the possibility
   * of representing infinite open regions.
   * The answer is split into final and non-final regions based on the given
   * MBR.
   */
  public void getFinalAnswerAsVoronoiRegions(Rectangle mbr,
      List<Geometry> finalRegions, List<Geometry> nonfinalRegions) {
    // Detect final and non-final sites
    BitArray unsafeSites = detectUnsafeSites(mbr);
    // We use the big MBR to clip open sides of Voronoi regions
    double bufferSize = Math.max(mbr.getWidth(), mbr.getHeight()) / 10;
    Rectangle bigMBR = mbr.buffer(bufferSize, bufferSize);
    Rectangle biggerMBR = mbr.buffer(bufferSize, bufferSize * 1.1);
    // A factory that creates polygons
    final GeometryFactory Factory = new GeometryFactory();
    
    for (int iSite = 0; iSite < points.length; iSite++) {
      // Compute the Voronoi region of this site as a polygon
      // Sort all neighbors in CCW order and compute the perependicular bisector
      // of each incident edge. The intersection of perpendicular bisectors form
      // the points of the polygon that represents the Voronoi region.
      // If the angle between two neighbors is larger than PI, this indicates
      // an open region
      final IntArray siteNeighbors = neighbors[iSite];

      // Compute angles between the points and its neighbors
      final double[] angles = new double[siteNeighbors.size()];
      for (int iNeighbor = 0; iNeighbor < siteNeighbors.size(); iNeighbor++) {
        double dx = points[siteNeighbors.get(iNeighbor)].x - points[iSite].x;
        double dy = points[siteNeighbors.get(iNeighbor)].y - points[iSite].y;
        double ccwAngle = Math.atan2(dy, dx);
        angles[iNeighbor] = ccwAngle < 0 ? ccwAngle += Math.PI * 2 : ccwAngle;
      }
      
      // Sort neighbors by CCW order
      IndexedSortable ccwSort = new IndexedSortable() {
        
        @Override
        public void swap(int i, int j) {
          siteNeighbors.swap(i, j);
          double t = angles[i];
          angles[i] = angles[j];
          angles[j] = t;
        }
        
        @Override
        public int compare(int i, int j) {
          double diff = angles[i] - angles[j];
          if (diff < 0) return -1;
          if (diff > 0) return 1;
          return 0;
        }
      };
      new QuickSort().sort(ccwSort, 0, siteNeighbors.size());
      
      // Traverse neighbors in CCW order and compute intersections of
      // perpendicular bisectors
      List<Point> voronoiRegionPoints = new ArrayList<Point>();

      int firstPoint = -1; // -1 indicates a closed polygon with no first point
      for (int iNeighbor1 = 0; iNeighbor1 < siteNeighbors.size(); iNeighbor1++) {
        int iNeighbor2 = (iNeighbor1 + 1) % siteNeighbors.size();
        double ccwAngle = angles[iNeighbor2] - angles[iNeighbor1];
        if (ccwAngle < 0)
          ccwAngle += Math.PI * 2;
        if (ccwAngle > Math.PI) {
          // An open side of the Voronoi region
          // Compute the intersection of each perpendicular bisector to the
          // boundary
          Point p1 = intersectPerpendicularBisector(iSite, siteNeighbors.get(iNeighbor1), biggerMBR);
          Point p2 = intersectPerpendicularBisector(siteNeighbors.get(iNeighbor2), iSite, biggerMBR);
          voronoiRegionPoints.add(p1);
          voronoiRegionPoints.add(p2);
          // Mark p2 as the first point in the open line string
          firstPoint = voronoiRegionPoints.size() - 1;
        } else {
          // A closed side of the Voronoi region. Calculate the next point as
          // the center of the empty circle
          Point emptyCircleCenter = calculateCircumCircleCenter(iSite,
              siteNeighbors.get(iNeighbor1), siteNeighbors.get(iNeighbor2));
          voronoiRegionPoints.add(emptyCircleCenter);
        }
      }
      
      
      // Create the Voronoi polygon
      Geometry geom;
      if (firstPoint == -1) {
        // Indicates a closed polygon with no starting point
        Coordinate[] coords = new Coordinate[voronoiRegionPoints.size() + 1];
        for (int i = 0; i < voronoiRegionPoints.size(); i++) {
          Point voronoiPoint = voronoiRegionPoints.get(i);
          coords[i] = new Coordinate(voronoiPoint.x, voronoiPoint.y);
        }
        coords[coords.length - 1] = coords[0];
        LinearRing ring = Factory.createLinearRing(coords);
        geom = Factory.createPolygon(ring, null);
      } else {
        // Indicates an open line string with 'firstPoint' as the starting point
        // Reorder points according to first point
        List<Point> orderedPoints = new ArrayList<Point>(voronoiRegionPoints.size());
        orderedPoints.addAll(voronoiRegionPoints.subList(firstPoint, voronoiRegionPoints.size()));
        orderedPoints.addAll(voronoiRegionPoints.subList(0, firstPoint));
        voronoiRegionPoints = orderedPoints;
        
        // Cut all the segments to the bigMBR
        for (int i2 = 1; i2 < voronoiRegionPoints.size(); i2++) {
          int i1 = i2 - 1;
          if (!bigMBR.contains(voronoiRegionPoints.get(i1)) &&
              !bigMBR.contains(voronoiRegionPoints.get(i2))) {
            // The whole line segment is outside, remove the whole segment
            voronoiRegionPoints.remove(i1);
          } else if (!bigMBR.contains(voronoiRegionPoints.get(i1))) {
            // Only the first point is outside the bigMBR
            Point cutPoint = bigMBR.intersectLineSegment(
                voronoiRegionPoints.get(i2), voronoiRegionPoints.get(i1));
            voronoiRegionPoints.set(i1, cutPoint);
          } else if (!bigMBR.contains(voronoiRegionPoints.get(i2))) {
            // Only i2 is outside bigMBR. We cut the whole linestring
            Point cutPoint = bigMBR.intersectLineSegment(
                voronoiRegionPoints.get(i1), voronoiRegionPoints.get(i2));
            voronoiRegionPoints.set(i2, cutPoint);
            // Remove all points after i2
            while (voronoiRegionPoints.size() > i2+1)
              voronoiRegionPoints.remove(voronoiRegionPoints.size() - 1);
          }
        }
        
        
        Coordinate[] coords = new Coordinate[voronoiRegionPoints.size()];
        for (int i = 0; i < voronoiRegionPoints.size(); i++) {
          Point voronoiPoint = voronoiRegionPoints.get(i);
          coords[i] = new Coordinate(voronoiPoint.x, voronoiPoint.y);
        }
        geom = Factory.createLineString(coords);
      }
      geom.setUserData(points[iSite]);
      (unsafeSites.get(iSite) ? nonfinalRegions : finalRegions).add(geom);
    }
  }
  
  /**
   * Splits the answer into safe and unsafe parts. Safe parts are final and can
   * be safely written to the output as they are not going to be affected by any
   * future merge steps. Unsafe parts cannot be written to the output yet as they
   * might be affected by a future merge step, e.g., some edges are pruned.
   *
   * Here is how this functions works.
   * 1. It classifies sites into safe sites and unsafe sites using the method
   *   detectUnsafeSites. An unsafe site participates to at least one unsafe
   *   triangle
   * 2. Two graphs are written, one that contains all safe sites along with all
   *   their incident edges and one for unsafe sites along with all their
   *   incident edges.
   *
   * Notice that edges are not completely separated as an edge connecting a safe
   * and an unsafe site is written twice. Keep in mind that an edge that is
   * incident to a safe site is also safe because, by definition, it
   * participates in at least one safe triangle.
   * This also means that the safe graph might contain some unsafe sites and
   * the unsafe graph might contain some safe sites. This cannot be avoided as
   * those edges connecting a safe and unsafe site is written in both graphs and
   * needs its two ends to be present for completeness.
   *
   * @param mbr The MBR used to check for safe and unsafe sites. It is assumed
   *          that no more points can be introduced in this MBR but more points
   *          can appear later outside that MBR.
   * @param safeGraph The graph of all safe sites along with their edges and neighbors.
   * @param unsafeGraph The set of unsafe sites along with their edges and neighbors.
   */
  public void splitIntoSafeAndUnsafeGraphs(Rectangle mbr,
                                           Triangulation safeGraph,
                                           Triangulation unsafeGraph) {
    BitArray unsafeSites = detectUnsafeSites(mbr);

    // A safe edge is incident to at least one safe site
    IntArray safeEdgeStarts = new IntArray();
    IntArray safeEdgeEnds = new IntArray();
    // An unsafe edge is incident to at least one unsafe site
    IntArray unsafeEdgeStarts = new IntArray();
    IntArray unsafeEdgeEnds = new IntArray();

    for (int i = 0; i < points.length; i++) {
      if (progress != null)
        progress.progress();
      for (int n : neighbors[i]) {
        if (unsafeSites.get(n) || unsafeSites.get(i)) {
          unsafeEdgeStarts.add(i);
          unsafeEdgeEnds.add(n);
        }
        // Do NOT add an else statement because an edge might be added to both
        if (!unsafeSites.get(n) || !unsafeSites.get(i)) {
          safeEdgeStarts.add(i);
          safeEdgeEnds.add(n);
        }
      }
    }
    
    safeGraph.sites = this.points;
    safeGraph.edgeStarts = safeEdgeStarts.toArray();
    safeGraph.edgeEnds = safeEdgeEnds.toArray();
    safeGraph.sitesToReport = unsafeSites.or(reportedSites).invert();
    safeGraph.reportedSites = reportedSites;
    safeGraph.sortEdges();

    unsafeGraph.sites = this.points;
    unsafeGraph.edgeStarts = unsafeEdgeStarts.toArray();
    unsafeGraph.edgeEnds = unsafeEdgeEnds.toArray();
    unsafeGraph.sitesToReport = new BitArray(this.points.length); // Report nothing
    unsafeGraph.reportedSites = safeGraph.sitesToReport.or(this.reportedSites);
    unsafeGraph.sortEdges();

    safeGraph.compact();
    unsafeGraph.compact();
  }

  /**
   * Detects which sites are unsafe based on the definition of Delaunay
   * triangulation. An unsafe site participates to at least one unsafe triangle.
   * An unsafe triangle has an empty circle that goes (partially) outside the
   * given MBR.
   * @param mbr
   * @return
   */
  private BitArray detectUnsafeSites(Rectangle mbr) {
    // Nodes that has its adjacency list sorted by node ID to speed up the merge
    BitArray sortedSites = new BitArray(points.length);
    // An unsafe site is a site that participates to at least one unsafe triangle 
    BitArray unsafeSites = new BitArray(points.length);
    // Sites that need to be checked whether they have unsafe triangles or not
    IntArray sitesToCheck = new IntArray();
    
    // Initially, add all sites on the convex hull to unsafe sites
    for (int convexHullPoint : finalAnswer.convexHull) {
      sitesToCheck.add(convexHullPoint);
      unsafeSites.set(convexHullPoint, true);
    }
    
    while (!sitesToCheck.isEmpty()) {
      if (progress != null)
        progress.progress();
      int siteToCheck = sitesToCheck.pop();
      IntArray neighbors1 = neighbors[siteToCheck];
      // Sort the array to speedup merging neighbors
      if (!sortedSites.get(siteToCheck)) {
        neighbors1.sort();
        sortedSites.set(siteToCheck, true);
      }
      for (int neighborID : neighbors1) {
        IntArray neighbors2 = neighbors[neighborID];
        // Sort neighbor nodes, if needed
        if (!sortedSites.get(neighborID)) {
          neighbors2.sort();
          sortedSites.set(neighborID, true);
        }
        // Find common nodes which form triangles
        int i1 = 0, i2 = 0;
        while (i1 < neighbors1.size() && i2 < neighbors2.size()) {
          if (neighbors1.get(i1) == neighbors2.get(i2)) {
            // Found a triangle. Check whether the triangle is safe or not
            // A safe triangle is a triangle with an empty circle that fits
            // completely inside partition boundaries. This means that this safe
            // triangle cannot be disrupted by any point in other partitions
            boolean safeTriangle = true;
            // Found a triangle between unsafeNode, neighborID and neighbors1[i1]
            Point emptyCircle = calculateCircumCircleCenter(siteToCheck, neighborID, neighbors1.get(i1));
            if (emptyCircle == null || !mbr.contains(emptyCircle)) {
              // The center is outside the MBR, unsafe
              safeTriangle = false;
            } else {
              // If the empty circle is not completely contained in the MBR,
              // the triangle is unsafe as the circle might become non-empty
              // when the merge process happens
              double dx = emptyCircle.x - xs[siteToCheck];
              double dy = emptyCircle.y - ys[siteToCheck];
              double r2 = dx * dx + dy * dy;
              double dist = Math.min(Math.min(emptyCircle.x - mbr.x1, mbr.x2 - emptyCircle.x),
                  Math.min(emptyCircle.y - mbr.y1, mbr.y2 - emptyCircle.y));
              if (dist * dist <= r2)
                safeTriangle = false;
            }
            if (!safeTriangle) {
              // The two additional sites are unsafe and need to be further checked
              if (!unsafeSites.get(neighborID)) {
                sitesToCheck.add(neighborID);
                unsafeSites.set(neighborID, true);
              }
              if (!unsafeSites.get(neighbors1.get(i1))) {
                sitesToCheck.add(neighbors1.get(i1));
                unsafeSites.set(neighbors1.get(i1), true);
              }
            }
            i1++;
            i2++;
          } else if (neighbors1.get(i1) < neighbors2.get(i2)) {
            i1++;
          } else {
            i2++;
          }
        }
      }
    }
    return unsafeSites;
  }

  public boolean test() {
    final double threshold = 1E-6;
    List<Point> starts = new Vector<Point>();
    List<Point> ends = new Vector<Point>();
    for (int s1 = 0; s1 < points.length; s1++) {
      for (int s2 : neighbors[s1]) {
        if (s1 < s2) {
          starts.add(new Point(xs[s1], ys[s1]));
          ends.add(new Point(xs[s2], ys[s2]));
        }
      }
    }
    
    for (int i = 0; i < starts.size(); i++) {
      double x1 = starts.get(i).x;
      double y1 = starts.get(i).y;
      double x2 = ends.get(i).x;
      double y2 = ends.get(i).y;
      for (int j = i + 1; j < starts.size(); j++) {
        double x3 = starts.get(j).x;
        double y3 = starts.get(j).y;
        double x4 = ends.get(j).x;
        double y4 = ends.get(j).y;
  
        double den = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
        double ix = (x1 * y2 - y1 * x2) * (x3 - x4) / den - (x1 - x2) * (x3 * y4 - y3 * x4) / den;
        double iy = (x1 * y2 - y1 * x2) * (y3 - y4) / den - (y1 - y2) * (x3 * y4 - y3 * x4) / den;
        double minx1 = Math.min(x1, x2);
        double maxx1 = Math.max(x1, x2); 
        double miny1 = Math.min(y1, y2);
        double maxy1 = Math.max(y1, y2); 
        double minx2 = Math.min(x3, x4);
        double maxx2 = Math.max(x3, x4); 
        double miny2 = Math.min(y3, y4);
        double maxy2 = Math.max(y3, y4); 
        if ((ix - minx1 > threshold && ix - maxx1 < -threshold) && (iy - miny1 > threshold && iy - maxy1 < -threshold) &&
            (ix - minx2 > threshold && ix - maxx2 < -threshold) && (iy - miny2 > threshold && iy - maxy2 < -threshold)) {
          System.out.printf("line %f, %f, %f, %f\n", x1, y1, x2, y2);
          System.out.printf("line %f, %f, %f, %f\n", x3, y3, x4, y4);
          System.out.printf("circle %f, %f, 0.5\n", ix, iy);
          throw new RuntimeException("error");
        }
      }
    }
    return true;
  }

  boolean inArray(int[] array, int objectToFind) {
    for (int objectToCompare : array)
      if (objectToFind == objectToCompare)
        return true;
    return false;
  }

  /**
   * Compute the clock-wise angle between (s2->s1) and (s2->s3)
   * @param s1
   * @param s2
   * @param s3
   * @return
   */
  double calculateCWAngle(int s1, int s2, int s3) {
    // TODO use cross product to avoid using atan2
    double angle1 = Math.atan2(ys[s1] - ys[s2], xs[s1] - xs[s2]);
    double angle2 = Math.atan2(ys[s3] - ys[s2], xs[s3] - xs[s2]);
    return angle1 > angle2 ? (angle1 - angle2) : (Math.PI * 2 + (angle1 - angle2));
  }
  
  /**
   * Calculate the intersection between the perpendicular bisector of the line
   * segment (p1, p2) towards the right (CCW) and the given rectangle.
   * It is assumed that the two pend points p1 and p2 lie inside the given
   * rectangle.
   * @param p1
   * @param p2
   * @param rect
   * @return
   */
  Point intersectPerpendicularBisector(int p1, int p2, Rectangle rect) {
    double px = (xs[p1] + xs[p2]) / 2;
    double py = (ys[p1] + ys[p2]) / 2;
    double vx = ys[p1] - ys[p2];
    double vy = xs[p2] - xs[p1];
    double a1 = ((vx >= 0 ? rect.x2 : rect.x1) - px) / vx;
    double a2 = ((vy >= 0 ? rect.y2 : rect.y1) - py) / vy;
    double a = Math.min(a1, a2);
    return new Point(px + a * vx, py + a * vy);
  }
  
  Point calculateCircumCircleCenter(int s1, int s2, int s3) {
    // Calculate the perpendicular bisector of the first two points
    double x1 = (xs[s1] + xs[s2]) / 2;
    double y1 = (ys[s1] + ys[s2]) /2;
    double x2 = x1 + ys[s2] - ys[s1];
    double y2 = y1 + xs[s1] - xs[s2];
    // Calculate the perpendicular bisector of the second two points 
    double x3 = (xs[s3] + xs[s2]) / 2;
    double y3 = (ys[s3] + ys[s2]) / 2;
    double x4 = x3 + ys[s2] - ys[s3];
    double y4 = y3 + xs[s3] - xs[s2];
    
    // Calculate the intersection of the two new lines
    // See https://en.wikipedia.org/wiki/Line%E2%80%93line_intersection
    double den = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);
    if (den < 1E-11 && den > -1E-11) {
      // The three points are collinear, circle exists at infinity
      // Although the calculations will return coordinates at infinity based
      // on the calculations, we prefer to return null to avoid the following
      // computations and allow the sender to easily check for this degenerate
      // case
      return null;
    }
    double ix = ((x1 * y2 - y1 * x2) * (x3 - x4) - (x1 - x2) * (x3 * y4 - y3 * x4)) / den;
    double iy = ((x1 * y2 - y1 * x2) * (y3 - y4) - (y1 - y2) * (x3 * y4 - y3 * x4)) / den;
    
    // We can also use the following equations from
    // http://mathforum.org/library/drmath/view/62814.html
//    double v12x = x2 - x1;
//    double v12y = y2 - y1;
//    double v34x = x4 - x3;
//    double v34y = y4 - y3;
//    
//    double a = ((x3 - x1) * v34y - (y3 - y1) * v34x) / (v12x * v34y - v12y * v34x);
//    double ix = x1 + a * v12x;
//    double iy = y1 + a * v12y;
    
    return new Point(ix, iy);
  }

  /**
   * Compute the convex hull of a list of points given by their indexes in the
   * array of points
   * @param points
   * @return
   */
  int[] convexHull(final int[] points) {
    Stack<Integer> lowerChain = new Stack<Integer>();
    Stack<Integer> upperChain = new Stack<Integer>();

    // Sort sites by increasing x-axis. We cannot rely of them being sorted as
    // different algorithms can partition the data points in different ways
    // For example, Dwyer's algorithm partition points by a grid
    IndexedSorter sort = new MergeSorter();
    IndexedSortable xSortable = new IndexedSortable() {
      @Override
      public int compare(int i, int j) {
        int dx = Double.compare(xs[points[i]], xs[points[j]]);
        if (dx != 0)
          return dx;
        return Double.compare(ys[points[i]], ys[points[j]]);
      }

      @Override
      public void swap(int i, int j) {
        int temp = points[i];
        points[i] = points[j];
        points[j] = temp;
      }
    };
    sort.sort(xSortable, 0, points.length);

    // Lower chain
    for (int i = 0; i < points.length; i++) {
      while(lowerChain.size() > 1) {
        int s1 = lowerChain.get(lowerChain.size() - 2);
        int s2 = lowerChain.get(lowerChain.size() - 1);
        int s3 = points[i];
        // Compute the cross product between (s1 -> s2) and (s1 -> s3)
        double crossProduct = (xs[s2] - xs[s1]) * (ys[s3] - ys[s1]) - (ys[s2] - ys[s1]) * (xs[s3] - xs[s1]);
        // Changing the following condition to "crossProduct <= 0" will remove
        // collinear points along the convex hull edge which might product incorrect
        // results with Delaunay triangulation as it might skip the points in
        // the middle.
        if (crossProduct < 0)
          lowerChain.pop();
        else break;
      }
      lowerChain.push(points[i]);
    }
    
    // Upper chain
    for (int i = points.length - 1; i >= 0; i--) {
      while(upperChain.size() > 1) {
        int s1 = upperChain.get(upperChain.size() - 2);
        int s2 = upperChain.get(upperChain.size() - 1);
        int s3 = points[i];
        double crossProduct = (xs[s2] - xs[s1]) * (ys[s3] - ys[s1]) - (ys[s2] - ys[s1]) * (xs[s3] - xs[s1]);
        // Changing the following condition to "crossProduct <= 0" will remove
        // collinear points along the convex hull edge which might product incorrect
        // results with Delaunay triangulation as it might skip the points in
        // the middle.
        if (crossProduct < 0)
          upperChain.pop();
        else break;
      }
      upperChain.push(points[i]);
    }
    
    lowerChain.pop();
    upperChain.pop();
    int[] result = new int[lowerChain.size() + upperChain.size()];
    for (int i = 0; i < lowerChain.size(); i++)
      result[i] = lowerChain.get(i);
    for (int i = 0; i < upperChain.size(); i++)
      result[i + lowerChain.size()] = upperChain.get(i);
    return result;    
  }
}
