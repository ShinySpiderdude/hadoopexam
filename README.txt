The solution to the problem
---------------------------

We are going to leverage the fact that there is a small number of possible tags for a single site and that
we are only looking for top N similarities in order to keep calculations to a minimum:

Step 1 (Mapper):
Group tags by site, that is map the input (siteX tagY) to (siteX -> tag1, tag2, tag3....)

Step 2 (Mapper):
Sort the values (in some order, doesn't really matter which as long as the order is consistent across all maps.
 tags are strings so string natural ordering is fine). Then, for each possible ordered permutation emit:
 (permutation -> siteX)
 i.e. (tag1 -> siteX : (tag1, tag2) -> siteX : (tag1, tag3) -> siteX : (tag2, tag3 -> siteX) : (tag1, tag2, tag3 -> siteX)
Since there can be at most 10 tags per site, this operation is relatively fast per mapper

Step 3 (Reducer):
We now have a map from all tags being shared by sites to the sites that share them
i.e.
tag1 -> site1, site2, site3
tag1 tag2 -> site1, site3
etc..
(It is perhaps worth noting that some value sets may have as many as 10K values here)
We will sum the KEYS and emit the results
(1 -> site1, site2, site3) (2 -> site1, site3)...

Step 4 (Mapper):
We now map each entry to the form (siteX common-tags) -> (similar site) in order to create a secondary index
We don't need all sets of (siteX siteY) as this will both take too long (as long as 10K^2 operations per entry)
and is not needed since we only need top 10 similarities (we will "arbitrarily" choose the first 11 sites (for code
readability) to be the similar sites for every site in this entry).
(Technical note: We'll have to create a "WritableComparable" object for the key in order to sort by
(siteX ASC, common-tags DESC)

Step 5 (Reducer):
We are almost done. Since the input to the reducer is sorted by the [composite] key, we will have
sorted entries by both site and common-tag-number. We will iterate through the 10 highest entries and drop
the rest.

