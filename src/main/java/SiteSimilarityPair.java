import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ilan on 11/22/16.
 */
public class SiteSimilarityPair implements WritableComparable<SiteSimilarityPair> {

    // natural key
    String site ;

    // secondary key
    int similarity ;

    public void setSite(String site) {
        this.site = site;
    }

    public void setSimilarity(int similarity) {
        this.similarity = similarity;
    }

    @Override
    public int compareTo(SiteSimilarityPair that) {
        int compared = this.site.compareTo(that.site) ;
        if (compared != 0) {
            // Sort ascending
            return compared ;
        }
        // Sord descending
        return that.similarity - this.similarity ;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SiteSimilarityPair)) {
            return false ;
        }
        SiteSimilarityPair that = (SiteSimilarityPair)o ;
        return this.site.equals(that.site) && this.similarity == that.similarity ;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(site);
        out.writeInt(similarity);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        site = in.readUTF() ;
        similarity = in.readInt() ;
    }
}
