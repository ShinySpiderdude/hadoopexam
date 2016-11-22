The solution to the problem
---------------------------
I must humbly admit that i did not find a solution that was both elegant and fast.
I have found an elegant solution, but it was very slow.
The solution i've found seems to run in a feasible time (relatively) although it is not exactly elegant so i ask you
to bear with me while i try my best to explain.

Basically, i am going to leverage the fact that there is a small number of possible tags for every single site X
and that i am only looking for top N similarities in order to keep calculations to a minimum:

Phase 1:

Step 1 (Mapper):
Group tags by siteX, that is map the input (siteX tagY) to (siteX -> tag1, tag2, tag3....)

Step 2 (Reducer):
Sort the values (there can be max 100) in some order, doesn't really matter which as long as the order is consistent across all entries.
 (tags are strings so string natural ordering is fine). And write to file

Phase 2:

Step 3 (Mapper):
For each possible ordered permutation emit:
 (permutation -> siteX)
 i.e. (tag1 -> siteX : (tag1, tag2) -> siteX : (tag1, tag3) -> siteX : (tag2, tag3 -> siteX) : (tag1, tag2, tag3 -> siteX)
Since there can be at most 100 tags per siteX, this operation is relatively fast per entry

Step 4 (Reducer):
We now have a map from all tags being shared by sites to the sites that share them
i.e.
tag1 -> site1, site2, site3
tag1 tag2 -> site1, site3
tag1 tag2 tag3 -> site1, site3
etc..
(It is perhaps worth noting that some value sets may have as many as 10K values here.
 Also if 2 similar sites appear at some tag combination, they will also appear at all the subsets of that combination)
We will sum the KEYS and emit the results (also dropping along all results that have just one siteX associated with them,
because that just means that a siteX is common with itself by virtue of that set of tags)
(1 -> site1, site2, site3) (2 -> site1, site3)...


Phase 3:

So this is where it gets a bit messy...
Ideally, We now want to eliminate duplicates we got above by the tag subsets. That is, if we have:
1 site1 site2 (from <tag1>)
1 site2 site1 (from <tag2>) (notice that they aren't ordered)
2 site1 site2 (from <tag1 tag2>)
We'll want to work with just the last, since this is the true similarity of site1 and site2.
In practice, that is not possible. Since some tags (or tag sets) may have 10K sites associated with them, creating each
pair could use up as much as (10K^2) time per entry, which is too much.
However, since we are only interested in top 10 we don't really need to pair ALL the sites with each other, it is enough
that we pair each siteX with just 10 random others for each entry.
This will drop the number of pairings to just 100K per entry.

Step 5 (Mapper):
For each entry, choose 10 random sites (since they aren't ordered, choosing the first 10 is just fine) and emit
 (siteX, siteY) -> count-of-common-tags for each pair (in an ordered manner, so that site1 < site2)
Last, pair the 10 chosen sites (ordered) between themselves and emit.

Step 6 (Reducer):
We now have entries of the form: (siteX, siteY) -> (number-of-common-tags-1, number-of-common-tags2)
emit the pair and the max of all common-tags

Phase 4:

Step 7 (Mapper):
In this step we do the secondary sorting and the top 10 in one go, doing just some small work in the reducer.
It is possible to use "secondary sort" the "traditional" way (at least, that's how it seems most people who blog
 about it do) of creating a support class for the key of type (siteX, similarity) (We still partition by siteX in order
 for all siteX mappings to go the same reducer).
 If i didn't need top 10 i would probably use that method, however, this might result in an output of:
 site1, 10 -> site2, site3, site4
 site1, 9 -> site5, site6
 site1, 8 -> site7
 etc..
which will result in having to do some funky coding to get the top 10.
Instead i opted to do following (i think it's simpler, i can see a different point of view here, though):
The key class is built out of (siteX, similarity, siteY) fields
Its comparator is made out of (siteX ASC, similarity DESC, siteY ASC)
The mapper emits no value
This results in a natural reducer input of:
site1, 10, site2
site1, 10, site3
site1, 10, site4
site1, 9, site5
etc...


Step 8 (Reducer):
We are almost done. Since the input to the reducer is sorted by the [composite] key, we will have
sorted entries by both siteX and common-tag-number.
Since the partitioner sends all "siteX" records to the same reducer, and they are in order we simply count records
until we reach 10 and we stop emitting. When siteX changes we simply reset the counter..



