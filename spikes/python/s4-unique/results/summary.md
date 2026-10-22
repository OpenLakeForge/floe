| History | Layout | Key | memory_limit | Spill cap | Wall s | VmHWM MiB | RssAnon MiB | VmHWM / limit | Spill MiB | Result |
|---|---|---|---|---|---|---|---|---|---|---|
| 100M | unpartitioned | id | 4GB | 15GB | 4.8 | 1,445 | 1,353 | 0.38 | 0 | exact |
| 100M | unpartitioned | id | 2GB | 15GB | 4.4 | 1,443 | 1,352 | 0.76 | 0 | exact |
| 100M | unpartitioned | id | 1GB | 15GB | 6.4 | 1,256 | 1,164 | 1.32 | 363 | exact |
| 100M | unpartitioned | id | 512MB | 15GB | 8.3 | 807 | 716 | 1.65 | 986 | exact |
| 100M | unpartitioned | id | 256MB | 15GB | 10.2 | 483 | 387 | 1.98 | 1,423 | exact |
| 100M | unpartitioned | id | 128MB | 15GB | 11.9 | 345 | 253 | 2.83 | 1,591 | exact |
| 100M | unpartitioned | id | 64MB | 15GB | 3.0 | 240 | 150 | 3.93 | 516 | Out of Memory Error: failed to allocate data of size 16.0 MiB (51.9 MiB/61.0 MiB used) |
| 100M | unpartitioned | account, seq | 4GB | 15GB | 6.5 | 1,978 | 1,887 | 0.52 | 0 | exact |
| 100M | unpartitioned | account, seq | 2GB | 15GB | 7.5 | 1,749 | 1,658 | 0.92 | 0 | exact |
| 100M | unpartitioned | account, seq | 1GB | 15GB | 12.4 | 1,338 | 1,247 | 1.40 | 1,470 | exact |
| 100M | unpartitioned | account, seq | 512MB | 15GB | 14.3 | 841 | 750 | 1.72 | 2,217 | exact |
| 100M | unpartitioned | account, seq | 256MB | 15GB | 16.5 | 556 | 466 | 2.28 | 2,358 | exact |
| 100M | unpartitioned | account, seq | 128MB | 15GB | 3.8 | 322 | 228 | 2.64 | 845 | Out of Memory Error: failed to allocate data of size 32.0 MiB (119.3 MiB/122.0 MiB used) |
| 100M | bucket(16,id), ~3.1M rows/file | id | 4GB | 15GB | 5.0 | 1,460 | 1,370 | 0.38 | 0 | exact |
| 100M | bucket(16,id), ~3.1M rows/file | id | 2GB | 15GB | 4.5 | 1,456 | 1,364 | 0.76 | 0 | exact |
| 100M | bucket(16,id), ~3.1M rows/file | id | 1GB | 15GB | 5.8 | 1,181 | 1,091 | 1.24 | 349 | exact |
| 100M | bucket(16,id), ~3.1M rows/file | id | 512MB | 15GB | 8.2 | 819 | 729 | 1.68 | 971 | exact |
| 100M | bucket(16,id), ~3.1M rows/file | id | 256MB | 15GB | 10.0 | 474 | 382 | 1.94 | 1,478 | exact |
| 100M | bucket(16,id), ~3.1M rows/file | id | 128MB | 15GB | 11.9 | 370 | 280 | 3.03 | 1,620 | exact |
| 100M | bucket(16,id), ~3.1M rows/file | id | 64MB | 15GB | 3.0 | 236 | 145 | 3.87 | 486 | Out of Memory Error: failed to allocate data of size 16.0 MiB (51.9 MiB/61.0 MiB used) |
| 100M | bucket(16,id), ~3.1M rows/file | account, seq | 4GB | 15GB | 7.0 | 2,110 | 2,019 | 0.55 | 0 | exact |
| 100M | bucket(16,id), ~3.1M rows/file | account, seq | 2GB | 15GB | 8.2 | 1,851 | 1,761 | 0.97 | 0 | exact |
| 100M | bucket(16,id), ~3.1M rows/file | account, seq | 1GB | 15GB | 13.2 | 1,309 | 1,218 | 1.37 | 1,566 | exact |
| 100M | bucket(16,id), ~3.1M rows/file | account, seq | 512MB | 15GB | 17.0 | 805 | 698 | 1.65 | 2,125 | exact |
| 100M | bucket(16,id), ~3.1M rows/file | account, seq | 256MB | 15GB | 20.4 | 530 | 440 | 2.17 | 2,441 | exact |
| 100M | bucket(16,id), ~3.1M rows/file | account, seq | 128MB | 15GB | 4.2 | 321 | 229 | 2.63 | 756 | Out of Memory Error: failed to allocate data of size 32.0 MiB (119.3 MiB/122.0 MiB used) |
| 100M | bucket(16,id), ~0.78M rows/file | id | 4GB | 15GB | 5.8 | 1,463 | 1,371 | 0.38 | 0 | exact |
| 100M | bucket(16,id), ~0.78M rows/file | id | 2GB | 15GB | 5.4 | 1,459 | 1,366 | 0.76 | 0 | exact |
| 100M | bucket(16,id), ~0.78M rows/file | id | 1GB | 15GB | 6.8 | 1,240 | 1,149 | 1.30 | 345 | exact |
| 100M | bucket(16,id), ~0.78M rows/file | id | 512MB | 15GB | 9.3 | 806 | 707 | 1.65 | 983 | exact |
| 100M | bucket(16,id), ~0.78M rows/file | id | 256MB | 15GB | 11.8 | 470 | 379 | 1.93 | 1,433 | exact |
| 100M | bucket(16,id), ~0.78M rows/file | id | 128MB | 15GB | 13.1 | 356 | 262 | 2.92 | 1,606 | exact |
| 100M | bucket(16,id), ~0.78M rows/file | id | 64MB | 15GB | 3.0 | 235 | 144 | 3.85 | 466 | Out of Memory Error: failed to allocate data of size 16.0 MiB (51.6 MiB/61.0 MiB used) |
| 100M | bucket(16,id), ~0.78M rows/file | account, seq | 4GB | 15GB | 8.6 | 2,111 | 2,020 | 0.55 | 0 | exact |
| 100M | bucket(16,id), ~0.78M rows/file | account, seq | 2GB | 15GB | 10.3 | 1,860 | 1,769 | 0.98 | 0 | exact |
| 100M | bucket(16,id), ~0.78M rows/file | account, seq | 1GB | 15GB | 15.4 | 1,383 | 1,291 | 1.45 | 1,524 | exact |
| 100M | bucket(16,id), ~0.78M rows/file | account, seq | 512MB | 15GB | 18.6 | 804 | 713 | 1.65 | 2,132 | exact |
| 100M | bucket(16,id), ~0.78M rows/file | account, seq | 256MB | 15GB | 20.8 | 555 | 465 | 2.27 | 2,467 | exact |
| 100M | bucket(16,id), ~0.78M rows/file | account, seq | 128MB | 15GB | 3.8 | 326 | 232 | 2.67 | 901 | Out of Memory Error: failed to allocate data of size 32.0 MiB (119.5 MiB/122.0 MiB used) |
| 500M | unpartitioned | id | 4GB | 8GB | 18.6 | 1,876 | 1,784 | 0.49 | 0 | exact |
| 500M | unpartitioned | id | 2GB | 8GB | 15.9 | 1,878 | 1,788 | 0.98 | 0 | exact |
| 500M | unpartitioned | id | 1GB | 8GB | 24.4 | 1,200 | 1,109 | 1.26 | 1,967 | exact |
| 500M | unpartitioned | id | 512MB | 8GB | 35.1 | 829 | 737 | 1.70 | 4,181 | exact |
| 500M | unpartitioned | id | 256MB | 8GB | 41.7 | 513 | 422 | 2.10 | 5,934 | exact |
| 500M | unpartitioned | id | 128MB | 8GB | 44.9 | 388 | 297 | 3.18 | 6,382 | exact |
| 500M | unpartitioned | id | 64MB | 8GB | 2.7 | 242 | 152 | 3.96 | 488 | Out of Memory Error: failed to allocate data of size 16.0 MiB (51.9 MiB/61.0 MiB used) |
| 500M | unpartitioned | account, seq | 4GB | 8GB | 24.4 | 1,980 | 1,918 | 0.52 | 0 | exact |
| 500M | unpartitioned | account, seq | 2GB | 8GB | 28.1 | 1,721 | 1,659 | 0.90 | 0 | exact |
| 500M | unpartitioned | account, seq | 1GB | 8GB | 49.1 | 1,381 | 1,318 | 1.45 | 6,260 | exact |
| 500M | unpartitioned | account, seq | 512MB | 8GB | 39.5 | 750 | 685 | 1.54 | 7,629 | Out of Memory Error: failed to offload data block of size 256.0 KiB (7.4 GiB/7.4 GiB used). |
| 500M | unpartitioned | account, seq | 512MB | 12GB | 71.3 | 870 | 807 | 1.78 | 8,607 | exact |
| 500M | bucket(16,id), ~3.1M rows/file | id | 4GB | 8GB | 18.3 | 1,946 | 1,883 | 0.51 | 0 | exact |
| 500M | bucket(16,id), ~3.1M rows/file | id | 2GB | 8GB | 15.9 | 1,944 | 1,882 | 1.02 | 0 | exact |
| 500M | bucket(16,id), ~3.1M rows/file | id | 1GB | 8GB | 24.6 | 1,208 | 1,138 | 1.27 | 2,009 | exact |
| 500M | bucket(16,id), ~3.1M rows/file | id | 512MB | 8GB | 34.1 | 804 | 741 | 1.65 | 4,364 | exact |
| 500M | bucket(16,id), ~3.1M rows/file | id | 256MB | 8GB | 41.8 | 497 | 434 | 2.04 | 6,162 | exact |
| 500M | bucket(16,id), ~3.1M rows/file | id | 128MB | 8GB | 43.2 | 355 | 293 | 2.91 | 6,718 | exact |
| 500M | bucket(16,id), ~3.1M rows/file | id | 64MB | 8GB | 2.7 | 207 | 143 | 3.39 | 504 | Out of Memory Error: failed to allocate data of size 16.0 MiB (51.6 MiB/61.0 MiB used) |
| 500M | bucket(16,id), ~3.1M rows/file | account, seq | 4GB | 8GB | 25.0 | 2,698 | 2,637 | 0.71 | 0 | exact |
| 500M | bucket(16,id), ~3.1M rows/file | account, seq | 2GB | 8GB | 31.0 | 2,077 | 2,015 | 1.09 | 0 | exact |
| 500M | bucket(16,id), ~3.1M rows/file | account, seq | 1GB | 8GB | 57.9 | 1,314 | 1,251 | 1.38 | 6,757 | exact |
| 500M | bucket(16,id), ~3.1M rows/file | account, seq | 512MB | 8GB | 44.4 | 724 | 661 | 1.48 | 7,629 | Out of Memory Error: failed to offload data block of size 96.0 KiB (7.4 GiB/7.4 GiB used). |
| 500M | bucket(16,id), ~3.1M rows/file | account, seq | 512MB | 12GB | 47.3 | 823 | 761 | 1.69 | 11,445 | Out of Memory Error: failed to offload data block of size 256.0 KiB (11.1 GiB/11.1 GiB used). |
| 500M | bucket(16,id), ~0.78M rows/file | id | 4GB | 6GB | 23.6 | 1,957 | 1,895 | 0.51 | 0 | exact |
| 500M | bucket(16,id), ~0.78M rows/file | id | 2GB | 6GB | 20.8 | 1,956 | 1,894 | 1.03 | 0 | exact |
| 500M | bucket(16,id), ~0.78M rows/file | id | 1GB | 6GB | 30.1 | 1,159 | 1,097 | 1.22 | 1,915 | exact |
| 500M | bucket(16,id), ~0.78M rows/file | account, seq | 4GB | 6GB | 34.1 | 2,704 | 2,643 | 0.71 | 0 | exact |
| 500M | bucket(16,id), ~0.78M rows/file | account, seq | 2GB | 6GB | 39.2 | 2,087 | 2,025 | 1.09 | 0 | exact |
| 500M | bucket(16,id), ~0.78M rows/file | account, seq | 1GB | 6GB | 44.4 | 1,223 | 1,156 | 1.28 | 5,722 | Out of Memory Error: failed to offload data block of size 96.0 KiB (5.5 GiB/5.5 GiB used). |
