import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ilan on 11/22/16.
 */
public class SiteSimilarity implements WritableComparable<SiteSimilarity> {

    // natural key
    String siteX;

    // secondary key
    int similarity ;

    // not a key at all, but i need it as part of the composite key for the final output
    String siteY ;

    public void setSiteX(String siteX) {
        this.siteX = siteX;
    }

    public void setSimilarity(int similarity) {
        this.similarity = similarity;
    }

    public void setSiteY(String siteY) {
        this.siteY = siteY;
    }

    @Override
    public int compareTo(SiteSimilarity that) {
        int compared = this.siteX.compareTo(that.siteX) ;
        if (compared != 0) {
            // Sort ascending
            return compared ;
        }
        // Sord descending
        compared = that.similarity - this.similarity ;
        if (compared != 0) {
            return compared ;
        }
        return this.siteY.compareTo(that.siteY) ;
    }

    //I don't really need this "equals", but for completions sake...
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SiteSimilarity)) {
            return false ;
        }
        SiteSimilarity that = (SiteSimilarity)o ;
        return this.siteX.equals(that.siteX) &&
                this.siteY.equals(that.siteY) &&
                this.similarity == that.similarity ;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(siteX);
        out.writeInt(similarity);
        out.writeUTF(siteY);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        siteX = in.readUTF() ;
        similarity = in.readInt() ;
        siteY = in.readUTF() ;
    }
}
