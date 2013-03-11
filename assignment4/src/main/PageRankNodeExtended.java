/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.array.ArrayListOfFloatsWritable;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

/**
 * Representation of a graph node for PageRank. 
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class PageRankNodeExtended implements Writable {
  public static enum Type {
    Complete((byte) 0),  // PageRank mass and adjacency list.
    Mass((byte) 1),      // PageRank mass only.
    Structure((byte) 2); // Adjacency list only.

    public byte val;

    private Type(byte v) {
      this.val = v;
    }
  };

	private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

	private Type type;
	private int nodeid;
	private ArrayListOfFloatsWritable pageranks;
	private ArrayListOfIntsWritable adjacenyList;

	public PageRankNodeExtended() {}

	public ArrayListOfFloatsWritable getPageRankArray() {
		return pageranks;
	}

	public void setPageRankArray(ArrayListOfFloatsWritable list) {
		this.pageranks = list;
	}
	
	public void setPageRank(int position, float value){
		if(position >= 0 && position < pageranks.size()){
			pageranks.set(position, value);
		} else {
			throw new ArrayIndexOutOfBoundsException("Can't set pagerank at invalid position: " + position);
		}
	}
	
	public float getPageRank(int position){
		if(position >= 0 && position < pageranks.size()){
			return pageranks.get(position);
			
		} else{
			throw new ArrayIndexOutOfBoundsException("Can't get pagerank at invalid position:" + position);
		}
	}
	
	public int getNodeId() {
		return nodeid;
	}

	public void setNodeId(int n) {
		this.nodeid = n;
	}

	public ArrayListOfIntsWritable getAdjacenyList() {
		return adjacenyList;
	}

	public void setAdjacencyList(ArrayListOfIntsWritable list) {
		this.adjacenyList = list;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * Deserializes this object.
	 *
	 * @param in source for raw byte representation
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int b = in.readByte();
		type = mapping[b];
		nodeid = in.readInt();
		
		pageranks = new ArrayListOfFloatsWritable();
		pageranks.readFields(in);
		
		if (type.equals(Type.Mass)) {
			return;
		}
		
		adjacenyList = new ArrayListOfIntsWritable();
		adjacenyList.readFields(in);
	}

	/**
	 * Serializes this object.
	 *
	 * @param out where to write the raw byte representation
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(type.val);
		out.writeInt(nodeid);
		pageranks.write(out);
		
		if (type.equals(Type.Mass)) {
			return;
		}
		
		adjacenyList.write(out);
	}

	@Override
	public String toString() {
		return String.format("{%d %s %s}",
				nodeid, 
				(pageranks == null ? "[]" : pageranks.toString(10)), 
				(adjacenyList == null ? "[]" : adjacenyList.toString(10)) );
	}


  /**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNodeExtended create(DataInput in) throws IOException {
	  PageRankNodeExtended m = new PageRankNodeExtended();
    m.readFields(in);

    return m;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNodeExtended create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
