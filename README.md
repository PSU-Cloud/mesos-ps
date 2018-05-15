# A working copy of Apache Mesos

# Apache Mesos

Apache Mesos is a cluster manager that provides efficient resource isolation
and sharing across distributed applications, or frameworks. It can run Hadoop,
Jenkins, Spark, Aurora, and other frameworks on a dynamically shared pool of
nodes.

Visit us at [mesos.apache.org](http://mesos.apache.org).

# Mailing Lists

 * [User Mailing List](mailto:user-subscribe@mesos.apache.org) ([Archive](https://mail-archives.apache.org/mod_mbox/mesos-user/))
 * [Development Mailing List](mailto:dev-subscribe@mesos.apache.org) ([Archive](https://mail-archives.apache.org/mod_mbox/mesos-dev/))

# Documentation

Documentation is available in the docs/ directory. Additionally, a rendered HTML
version can be found on the Mesos website's [Documentation](http://mesos.apache.org/documentation/) page.

# Installation

Instructions are included on the [Getting Started](http://mesos.apache.org/getting-started/) page.

# License

Apache Mesos is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

For additional information, see the LICENSE and NOTICE files.

# Added new features

Several different scheduling algorithms:
- coarse-grained DRF - "HierarchicalDRFAllocator";
- coarse-grained PSDSF - "HierarchicalPSDSFAllocator";
- coarse-grained rPSDSF - "HierarchicalRPSDSFAllocator";
- fine-grained DRF - "FineHierarchicalDRFAllocator";
- fine-grained PSDSF - "FineHierarchicalPSDSFAllocator";
- fine-grained RPSDSF - "FineHierarchicalRPSDSFAllocator";
- fine-grained TSF - "FineHierarchicalTSFAllocator";
- fine-grained Best-fit DRF - "FineBFHierarchicalDRFAllocator".

The default algorithm is coarse-grained DRF. To activate other algorithms, for example fine-grained DRF, you can run

```
./bin/mesos-master.sh --ip=172.31.16.219 --work_dir=/var/lib/mesos-master --roles="*,wc,pi" --weights="*=1,wc=3,pi=3" --allocator=FineHierarchicalDRF # focus on --allocator=*, ignore other configs
```

The [slide](http://www.cse.psu.edu/~yxs182/doc/adding_more_scheduling_algorithms_in_mesos.pptx) covers the implementation details of those alternative Mesos schedulers.

For more details regarding the performance of those different scheduling fashions see our [report](https://arxiv.org/abs/1803.00922).
