#!/bin/bash


# if [ "$#" -ne 1 ]; then
#   echo "Usage: ./db_bench_zns.sh [zns_device, e.g. nvme7n2]"
#   exit 1 
# fi

# configuration
DEVICE=nvme0n1
OUTPUT=build
BIN_DIR=../bin
DB_BIN=$BIN_DIR/db_bench
ZENFS_BIN=$BIN_DIR/zenfs


# benchmark configuration
KEY_SIZE=36
VALUE_SIZE=$((8 * 1024))
BLOB_FILE_SIZE=$((32 * 1024 * 1024))
MEMTABLE_SIZE=$((128 * 1024 * 1024))
BYTES_PER_GiB=$((1024 * 1024 * 1024))
BENCH_BASE_SIZE=20
KEY_NUM=$(($BENCH_BASE_SIZE * $BYTES_PER_GiB / $VALUE_SIZE))

# ./cp.sh

sudo nvme zns reset-zone /dev/$DEVICE -a

sudo rm -rf /tmp/zenfs_$DEVICE*
sudo ${ZENFS_BIN} mkfs --zbd=$DEVICE --aux_path=/tmp/zenfs_$DEVICE --force=true # --finish_threshold=4

   sudo gdb --args ${DB_BIN} \
    --zbd_path=$DEVICE \
    --disable_wal=false \
    --benchmarks=fillrandom \
    --use_existing_db=0 \
    --statistics=1 \
    --stats_per_interval=1 \
    --stats_interval_seconds=2 \
    --max_background_flushes=1 \
    --max_background_compactions=1 \
    --enable_lazy_compaction=0 \
    --level0_file_num_compaction_trigger=4 \
    --sync=0 \
    --allow_concurrent_memtable_write=1 \
    --delayed_write_rate=419430400 \
    --enable_write_thread_adaptive_yield=1 \
    --threads=1 \
    --num_levels=7 \
    --key_size=36 \
    --value_size=$VALUE_SIZE \
    --level_compaction_dynamic_level_bytes=false \
    --mmap_read=false \
    --compression_type=none \
    --memtablerep=skip_list \
    --use_terark_table=false \
    --blob_size=1024 \
    --blob_gc_ratio=0.0625 \
    --write_buffer_size=$MEMTABLE_SIZE \
    --target_file_size_base=$BLOB_FILE_SIZE \
    --target_blob_file_size=$BLOB_FILE_SIZE \
    --blob_file_defragment_size=33554432 \
    --optimize_filters_for_hits=true \
    --num=$KEY_NUM \
    --db=testdb \
    --use_direct_io_for_flush_and_compaction=true \
    --use_direct_reads=false \
    --writable_file_max_buffer_size=$((4 * 1024 * 1024)) \
    --max_dependence_blob_overlap=100000000000 \
    --block_size=$((4 * 1024)) \
    --zenfs_low_gc_ratio=0.3 \
    --zenfs_high_gc_ratio=0.6 \
    --zenfs_force_gc_ratio=0.9
