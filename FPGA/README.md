# FPGA Implementation

## Platform
We implement the hardware version of PipeFilter on a NetFPGA-embedded prototype, where a NetFPGA-1G-CML development board connects with a workstation (with Ryzen7 1700 @3.0GHz CPU and 64GB RAM) through PCIe Gen2 X4 lanes.

## Description
We provide C++ codes for fpga implementation here, which can be directly synthesized into Verilog codes using Vivado HLS. These codes include different versions of $k=2\ \ b=4, k=2\ \ b=8, k=4\ \ b=4, k=4\ \ b=8$.
