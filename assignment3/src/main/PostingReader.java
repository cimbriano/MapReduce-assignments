import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.io.pair.PairOfInts;


public class PostingReader {

  public static ArrayList<PairOfInts> readPostings(BytesWritable bytesWritable) throws IOException{

    byte[] bytes = bytesWritable.getBytes();

    ByteArrayInputStream postingsByteStream = new ByteArrayInputStream(bytes);
    DataInputStream inStream = new DataInputStream(postingsByteStream);

    ArrayList<PairOfInts> retVal = new ArrayList<PairOfInts>();

    int docno = 0;
    int nextDgap = 0;
    int termFreq = -1;
    int numPostings = WritableUtils.readVInt(inStream);
    

    for(int i = 0; i < numPostings; i++){

      nextDgap = WritableUtils.readVInt(inStream);
      termFreq = WritableUtils.readVInt(inStream);
      
      docno = docno + nextDgap;
      
      retVal.add(new PairOfInts(docno, termFreq));
    }


    return retVal;

  }

}
