# PipeFilter

## Introduction

Approximate membership query data structures ( i.e., filters) have ubiquitous applications in networking and computing. Cuckoo filters are emerging as the replacement for Bloom filters because they support deletions and usually have higher operation throughput and space efficiency. However, their designs are confined to a single-threaded execution paradigm, consequently unable to exploit the parallel processing capabilities of modern hardware such as multi-core CPUs, FPGAs, and P4 chips. This paper presents PipeFilter, a faster and more space-efficient filter that harnesses pipeline parallelism for superior performance. PipeFilter re-architects Cuckoo filter by partitioning its data structure into several sub-filters, each providing a candidate position for every item. During insertion, items traverse these sub-filters in sequence to secure an unoccupied position for storage. This allows the filter operations, including insertion, lookup, and deletion, to be naturally divided across several pipeline stages, each overseeing one of the sub-filters, which can further be implemented through multi-threaded execution or pipeline stages of programmable hardware to achieve significantly higher throughput. Moreover, PipeFilter excels in space utilization as it permits each item to explore more candidate positions. We implement and optimize PipeFilter on four platforms (single-core CPU, multi-core CPU, FPGA, and P4). Experimental results demonstrate that PipeFilter surpasses all baseline methods on four platforms. When running with a single core, it showcases a notable 25%$\sim$66% improvement in operation throughput and a high load factor exceeding 99%. When parallel processing on other platforms, PipeFilter achieves 7$\sim$800 times higher throughput than single-threaded execution.

## Descriptions

### Implementation:

The hardware and software versions of PipeFilter are implemented on FPGA, P4, single core CPU and multi-core CPU platforms respectively.





