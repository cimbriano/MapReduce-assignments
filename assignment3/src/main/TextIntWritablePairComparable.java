import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import edu.umd.cloud9.io.pair.PairOfWritables;


public class TextIntWritablePairComparable extends PairOfWritables<Text, IntWritable> 
  implements WritableComparable<PairOfWritables<Text, IntWritable>> {


  @Override
  public int compareTo(PairOfWritables<Text, IntWritable> arg0) {
    PairOfWritables<Text, IntWritable> pair1 = this;
    PairOfWritables<Text, IntWritable> pair2 = arg0;
    String leftText = pair1.getLeftElement().toString();
    String rightText = pair2.getLeftElement().toString();
    
    int leftElementCompare = leftText.compareTo(rightText);
    
    if( leftElementCompare == 0){
      
      return pair1.getRightElement().compareTo(pair2.getRightElement());
      
    } else{
      return leftElementCompare;
    }
    
  }

}
