# CPU classes [Azure]
Overall, Azure provides the following CPUs.
| Model | Freq. [GHz] | Turbo Freq. [GHz] | L3 Size [MB] |
| ----: | :---------: | :---------------: | :----------: |
| Coffee Lake E-2288G | 3.7 | 5.0 | 16 | 
| Haswell E5-2673 v3 | 2.4 | 3.1 | 30 |
| Skylake 8168 | 2.7 | 3.7 | 33 |
| Cascade Lake 6246R | 3.4 | 4.1 | 36 |
| Cascade Lake 8272CL | 2.6 | 3.5 | 36 |
| Skylake 8171M | 2.1 | -- | 36 |
| Cascade Lake 8280M | 2.7 | 4.0 | 39 |
| Intel Xeon E7-8890 v3 | 2.5 | 3.3 | 45 |
| Ice Lake 8370C | 2.8 | 3.5 | 48 |
| Broadwell E5-2673 v4 | 2.3 | 3.5 | 50 |
| Sapphire Rapids 8473C | 2.1 | 3.8 | 105 |

## CPU instance sizes
For RAM memory of around 32 GB, these CPUs are available in the following VM sizes.

### Coffee Lake E-2288G [16 MB]
#### DCsv2-series, `DC8_v2` [ unavailable ]
These VMs help protect the confidentiality and integrity of data and code while it’s processed in the public cloud.

Example confidential use cases include: databases, blockchain, multiparty data analytics, fraud detection, anti-money laundering, usage analytics, intelligence analysis and machine learning.

*__Price__ :* 624.88 $/month <!-- currently unavailable due to insufficient vCPU quota. -->

### Sapphire Rapids 8473C [150 MB]
#### Dv4-series, `D8_v4` [ unavailable ]
These sizes offer a combination of vCPU, memory and remote storage options for most production workloads.

*__Price__ :* 312.44 $/month <!-- currently unavailable due to insufficient vCPU quota. -->

### Broadwell E5-2673 v4 [50 MB]
#### Av2-series, `A4m_v2`
These VMs have CPU performance and memory configurations best suited for entry level workloads like development and test. The size is throttled to offer consistent processor performance for the running instance, regardless of the hardware it is deployed on (which means the the actual L3 sizes could be scaled down internally).

Some example use cases include development and test servers, low traffic web servers, small to medium databases, proof-of-concepts, and code repositories.

*__Price__ :* 212.43 $/month

#### Dv2-series, `D4_v2`
A follow-on to the original D-series, this series features a more powerful CPU and optimal CPU-to-memory configuration,making it suitable for most production workloads.

*__Price__ :* 384.71 $/month (for 28 GB RAM) <!-- currently unavailable due to insufficient vCPU quota. -->

#### Dv3-series, `D32_v3`
This series run on many different processors in a hyper-threaded configuration, providing a better value proposition for most general purpose workloads. Memory has been expanded, while disk and network limits have been adjusted on a per core basis to align with the move to hyperthreading.

*__Price__ :* 1,249.76 $/month <!-- currently unavailable due to insufficient vCPU quota. -->

#### Ev3-series, `E32_v3`
The memory-optimized version of the Dv3 series.

*__Price__ :* 1,646.88 $/month <!-- currently unavailable due to insufficient vCPU quota. -->

### 
eral
