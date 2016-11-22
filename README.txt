This README file has several sections. i'll jump straight to my solution, then the extras.

The solution to the problem (or at least A solution)
----------------------------------------------------
I will first humbly admit that i did not find a solution that was both elegant and fast.
I have found an elegant solution, but it was very slow.
The solution i did come up with seems to run in a feasible time (relatively) although it is not exactly elegant so i ask you
to bear with me while i try my best to explain.

Basically, i am going to leverage 2 facts:
1. That there is a small number of possible tags for every single site X
2. That i am only looking for top N similarities
These facts together will help me reduce the amount of needed operations significantly

The solution is made out of 4 phases, each phase consists of a mapper and a reducer.
For easy orientation there is a class per mapper/reducer and their name is PhaseXMapper/Reducer

Phase 1:

Step 1 (Mapper):
Group tags by site. that is map the input (siteX tagY) to (siteX -> tagY1, tagY2, tagY3....)

Step 2 (Reducer):
Sort the tags (there can be max 100) in some order, doesn't really matter which as long as the order is consistent across all entries.
 (tags are strings so string natural ordering is fine). Emit the results.
An example output could be:
site1 tag1 tag2 tag3
site2 tag2 tag3

Phase 2:

Step 3 (Mapper):
For each possible ordered subset of tags in the powerset (all possible subsets of a set) emit:
 (tag subset -> siteX)
 i.e. (tag1 -> siteX : (tag1, tag2) -> siteX : (tag1, tag3) -> siteX : (tag2, tag3 -> siteX) : (tag1, tag2, tag3 -> siteX)
Since there can be at most 100 tags per site, this operation is relatively fast per entry.
(We could have done the work Phase 1 reducer here instead. Perhaps if we had more mappers than reducers)

Step 4 (Reducer):
We now have a map from all tags being shared by sites to the sites that share them
i.e.
tag1 -> site1, site2, site3
tag1 tag2 -> site1, site3
tag1 tag2 tag3 -> site1, site3
etc..
It is worth noting that some value sets may have as many as 10K values here.
It is also worth noting that if 2 similar sites appear together at some tag combination, they will also appear
 together at all the subsets of that combination (example above)
We will sum the KEYS and emit the results (also dropping along all results that have just one site associated with them,
 because that just means that the site is common with itself by virtue of that set of tags)
example output:
1 site1 site2, site3
1 site2 site1
2 site1 site3

Phase 3:

So this is where it gets a bit messy...
Ideally, We now want to eliminate duplicates we got above by the tags' powerset. That is, if we have:
1 site1 site2 (from <tag1>)
1 site2 site1 (from <tag2>) (they may not be ordered)
2 site1 site2 (from <tag1 tag2>)
We'll want to work with just the last, since this is the true similarity of site1 and site2.
In practice, that is not possible. Since some tags (or tag sets) may have 10K sites associated with them, creating each
pair could use up as much as (10K^2) operations per entry, which is too much.
However (and this is the "trick"), since we are only interested in top 10 similar sites for each site,
 we don't really need to pair ALL the sites with each other,
 it is enough that we pair each site with just 10 random others for each entry.
This will drop the number of pairings to just 100K per entry.


I will try to explain this better, since this is the crucial point:
Suppose we need to find just TOP 2 similarities for each site. We pick any 2 sites from an entry (let's call them, the
"magical" sites) and match all other sites with them (by "matching" i mean emit (siteX, magical-siteY) -> similarity
and (magical-siteY, siteX) -> similarity)
And then them with themselves (the magical sites, that is)..
For example, if i had these entries (suppose that 5 is the max similarity possible):
5 site1 site2 site3 site4
4 site1 site2 site3 site4 site5
...
the output will be:
site1 site3 [5, 4]
site3 site1 [5, 4]
site1 site4 [5, 4]
site4 site1 [5, 4]
site1 site2 [5, 4]
site2 site1 [5, 4]
site1 site5 [4]
site5 site1 [4]
(the value lists might be longer)
...
Notice that once i pick the max value from each list i already have the TOP 2 required similarities for every site,
 and we did not need to pair site3 and site4 for that (or site5 with site3 or site4).



Step 5 (Mapper):
For each entry, choose 10 random sites (since they aren't ordered, choosing the first 10 is just fine) and emit
 (siteX, siteY) -> count-of-common-tags (i'll call this "similarity", for short) and (siteY, siteX) -> similarity.
Last, pair the 10 chosen sites between themselves and emit.

Step 6 (Reducer):
We now have entries of the form: (siteX, siteY) -> [similarity-1, similarity-2]
emit the pair and the maximum similarity

Phase 4:
At the start of this phase we have input of the form
site1 site2 40
site1 site3 39
site1 site2 42
site4 site7 88
etc..
The important thing here is that the sites aren't ordered in any way and that there may be more than the required
10 similarities for each pair.
As a reminder, it is worth noting that the list does not contain all possible pairs but it DOES contain at least
 10 similar-sites for each site and it is guaranteed that there is no similar-site for any site that has a better
 similarity for that site but does not appear in this list. That is, a similarity appearing on this list is the
 highest possible similarity for its given site-pair


Step 7 (Mapper):
In this step we do the secondary sorting and the top 10 in one go, doing just some small work in the reducer.
It is possible to use "secondary sort" the "traditional" way (at least, that's how it seems most people who blog
 about it do) of creating a support class for the key of type (siteX, similarity) but that's not the way that we are
 going to do it. (We still partition by siteX in order for all siteX mappings to go the same reducer, however)
If i didn't need top 10 i would probably use the "traditional" method, however, this might result in an output of:
 site1, 10 -> [site2, site3, site4]
 site1, 9 -> [site5, site6]
 site1, 8 -> [site7]
 etc..
which will result in having to do some funky coding to get the top 10.
Instead i opted to do following (i think it's simpler, i can see a different point of view here, though):
The key class is built out of (siteX, similarity, siteY) fields
Its comparator is made out of (siteX ASC, similarity DESC, siteY ASC)
The mapper emits the key with no value
This results in a natural reducer input of:
site1, 10, site2
site1, 10, site3
site1, 10, site4
site1, 9, site5
.
.
site2, 10, site1
site2, 8, site7
etc...


Step 8 (Reducer):
We are almost done. Since the input to the reducer is sorted by the [composite] key, we will have
sorted entries by both siteX and common-tag-number.
Since the partitioner sends all "siteX" records to the same reducer, and they are in order we simply count records
until we reach 10 and we stop emitting. When siteX changes we simply reset the counter..



That's it for the algorithm, now to some extras...

Assumptions about the input
---------------------------
I actually have only one assumption, that the input is valid.
That is that it is of the form "siteX   tagY" for each input.
The same line can even appear twice, i don't mind :)



How to run
----------
If you want to run this from the main class (or within an IDE) run the HadoopExam class
You can also run "mvn clean package" to create an executable jar named "HadoopExam-1.0.jar" in the "target" directory
In any case the program requires 3 command line arguments: [inputfile, temporary-folder, output-folder]
for example you could run:
java -jar HadoopExam-1.0.jar input.txt temp/ out/
I didn't try to run it on an actual hadoop cluster (simulated or not). I figured it didn't really matter.
If it is essential to make these adjustments tell me and i'll make them.



Time spent
----------
I didn't work on this project continuously but i think my time has been shared like so:
~10-20m: figuring out the "trivial" solution (the one that would work if "sites per tag" was a much smaller number, you know the one i'm talking about)
~30m: Trying to find workarounds for the 10K^2-operations barrier of the "trivial" solution, but failing
~30m-1h: Arriving at the current solution
Few hours: Trying to arrive at a more elegant solution (the kind that i won't have to use 35 lines to explain)
~15-30m: getting acquainted with MR techniques such as secondary sort and partitioning
Few hours: Getting acquainted with the MR java api, coding, testing, debugging, etc..
~30m-1h: writing this document