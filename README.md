ADAM playground
===============

Just a collection of implicit classes and helper functions that were side effect of my using ADAM.

Features Extensions
-------------------

Several new interesting methods to Feature and FeatureRDD classes.
Now it is possible to:
* filter features by regions
* extract genes and transcript features
* save features by contig
* use some extra methods on features
* coverage search

NucleotideContig Extensions
---------------------------

* search in the genome
* search by feature
* region extraction from the fragment
* extract multiple regions at once
* extract by string matching
* filter by overlapping features

String extensions
-----------------

* several helper method to deal with DNA
* several methods for text search

Adding to dependencies
======================

add the following to you build.sbt

```sbt
resolvers += sbt.Resolver.bintrayRepo("comp-bio-aging", "main")
libraryDependencies += "comp.bio.aging" %% "adam-playground" % "0.0.12"
```