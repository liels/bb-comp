#!/bin/bash

# Script to keep /var/TmpChia full of C5 compressed Chia plots using bladebit cudaplot.
# Calculates the maximum batch size (--count) based on available space.
# Assumes sequential plotting: temp space for one plot at a time.
# Plot size (P): ~82 GiB (C5 compressed k32 plot, rounded up for safety).
# Temp space (T): ~200 GiB (estimated for --disk-128 hybrid mode, rounded up).
# Buffer: 20 GiB (extra safety margin to avoid filling completely).

PLOT_SIZE=82  # GiB
TEMP_SPACE=200  # GiB
BUFFER=20  # GiB
DIR="/var/TmpChia"
SLEEP_TIME=10  # Seconds to sleep if not enough space (10 seconds)

while true; do
    # Get available space in GiB (using df with block size 1GiB)
    AVAILABLE=$(df --block-size=1073741824 "$DIR" | awk 'NR==2 {print $4}')
    
    # Effective available space with buffer
    EFFECTIVE_AVAILABLE=$((AVAILABLE - BUFFER))
    
    if [ "$EFFECTIVE_AVAILABLE" -lt "$TEMP_SPACE" ]; then
        echo "Not enough space to plot. Available: ${AVAILABLE} GiB. Sleeping for ${SLEEP_TIME} seconds..."
        sleep "$SLEEP_TIME"
        continue
    fi
    
    # Calculate max N: 1 + floor( (effective_available - TEMP_SPACE) / PLOT_SIZE )
    MAX_ADDITIONAL=$(( (EFFECTIVE_AVAILABLE - TEMP_SPACE) / PLOT_SIZE ))
    N=$((1 + MAX_ADDITIONAL))
    
    # Ensure N at least 1 if possible
    if [ "$N" -lt 1 ]; then
        N=1
    fi
    
    echo "Starting to create ${N} plots. Available space: ${AVAILABLE} GiB."
    
    # Run the chia plotter command
    chia plotters bladebit cudaplot \
        --threads 32 \
        --count "$N" \
        --farmerkey a495c8129d58d6a74e79bf0ed46c88717fa5eb75466be8ebd0745650ca46903cc01cc63699a0ef6b7ac955791058b83e \
        --contract xch1pkzac5sx0wk9xe7hm8a2g42fr7ep8x7tm7nmf0k06jsjlz98c4rqt6c8df \
        --tmp_dir "$DIR" \
        --tmp_dir2 "$DIR" \
        --verbose \
        --final_dir "$DIR" \
        --compress 5 \
        --disk-128
    
    echo "Finished creating ${N} plots."
done