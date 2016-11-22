import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 *
 * Create a map from the tag subsets to their corresponding sites
 *
 */
public class Phase2Mapper extends Mapper<Object, Text, Text, Text> {

    private Text siteText = new Text() ;
    private Text tagsText = new Text() ;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String tokens[] = value.toString().split("\t");
        String site = tokens[0] ;
        Set<String> tags = new TreeSet<>() ;
        for (int i = 1; i < tokens.length; i++) {
            tags.add(tokens[i]);
        }

        siteText.set(site);

        //Now calculate the powerset for the tag set and emit all subsets like so:
        //(tag1) -> siteX
        //(tag1 tag2) -> siteX
        //...
        for (Set<String> subset : powerSet(tags)) {
            StringBuilder sb = new StringBuilder();
            for (String tag : subset) {
                sb.append(tag).append("\t") ;
                tagsText.set(sb.toString());
                context.write(tagsText, siteText);
            }
        }

    }


    //A method to calculate the powerset of tags. Copied from stack overflow with slight variations
    //by me to account for the sorted nature needed by the tag sets.
    private Set<Set<String>> powerSet(Set<String> originalSet) {
        Set<Set<String>> sets = new HashSet<>();
        if (originalSet.isEmpty()) {
            sets.add(new HashSet<>());
            return sets;
        }
        List<String> list = new ArrayList<>(originalSet);
        String head = list.get(0);
        Set<String> rest = new TreeSet<>(list.subList(1, list.size()));
        for (Set<String> set : powerSet(rest)) {
            Set<String> newSet = new TreeSet<>();
            newSet.add(head);
            newSet.addAll(set);
            sets.add(newSet);
            sets.add(set);
        }
        return sets;
    }

}
