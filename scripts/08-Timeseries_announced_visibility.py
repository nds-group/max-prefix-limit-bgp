#!/usr/bin/env python3
"""
Time Series Analysis of Prefix Visibility (Parallelized Version)

This script loads the per-datetime processed visibility files and computes time series metrics
using multiprocessing for faster processing.

Metrics computed per ASN and IP version:
- Total number of prefixes (all visibility levels)
- Total number of prefixes (all visibility levels)
- Total prefixes with visibility >= 90%
- Total prefixes with visibility >= 95%
- Total prefixes with visibility == 100%
"""

import os
import json
import pickle
import datetime
import glob
from collections import defaultdict
from pathlib import Path
from multiprocessing import Pool, cpu_count


def process_single_file(filepath):
    """
    Process a single visibility file and extract metrics.
    
    Args:
        filepath: Path to the visibility pickle file
    
    Returns:
        Tuple of (success, result) where result is either the processed data or error message
    """
    try:
        with open(filepath, "rb") as f:
            data = pickle.load(f)
        
        timestamp = data["timestamp"]
        asn_data_dict = data["asn_data"]
        
        # Local storage for this file's data
        file_metrics = {}
        
        # Process each ASN in this timestamp
        for asn, asn_info in asn_data_dict.items():
            # Skip bogus ASNs
            if asn_info.get("bogus", False):
                continue
            
            if asn not in file_metrics:
                file_metrics[asn] = {
                    4: {'total': {}, 'high_visibility_90': {}, 'high_visibility': {}, 'perfect_visibility': {}},
                    6: {'total': {}, 'high_visibility_90': {}, 'high_visibility': {}, 'perfect_visibility': {}}
                }
            
            # Process IPv4 and IPv6 separately
            for ip_version in [4, 6]:
                if ip_version not in asn_info:
                    continue
                
                ipv_data = asn_info[ip_version]
                prefixes = ipv_data.get('prefixes', {})
                
                # Count prefixes by visibility
                total_prefixes = len(prefixes)
                high_visibility_90 = 0  # >= 90%
                high_visibility = 0  # >= 95%
                perfect_visibility = 0  # == 100%
                
                for prefix, prefix_info in prefixes.items():
                    visibility = prefix_info.get('visibility', 0)
                    
                    if visibility >= 90:
                        high_visibility_90 += 1

                    if visibility >= 95:
                        high_visibility += 1
                    
                    if visibility == 100:
                        perfect_visibility += 1
                
                # Store in this file's metrics
                file_metrics[asn][ip_version]['total'][timestamp] = total_prefixes
                file_metrics[asn][ip_version]['high_visibility_90'][timestamp] = high_visibility_90
                file_metrics[asn][ip_version]['high_visibility'][timestamp] = high_visibility
                file_metrics[asn][ip_version]['perfect_visibility'][timestamp] = perfect_visibility
        
        return (True, file_metrics)
    
    except Exception as e:
        return (False, f"Error in {filepath}: {str(e)}")


def merge_file_results(all_results):
    """
    Merge results from all processed files into a single data structure.
    
    Args:
        all_results: List of file_metrics dictionaries from process_single_file
    
    Returns:
        Combined timeseries_data dictionary
    """
    timeseries_data = defaultdict(
        lambda: {
            4: {'total': {}, 'high_visibility_90': {}, 'high_visibility': {}, 'perfect_visibility': {}},
            6: {'total': {}, 'high_visibility_90': {}, 'high_visibility': {}, 'perfect_visibility': {}}
        }
    )
    
    for file_metrics in all_results:
        for asn, asn_data in file_metrics.items():
            for ip_version in [4, 6]:
                for metric in ['total', 'high_visibility_90', 'high_visibility', 'perfect_visibility']:
                    timeseries_data[asn][ip_version][metric].update(
                        asn_data[ip_version][metric]
                    )
    
    return dict(timeseries_data)


def main():
    # Load configuration
    print("Loading configuration...")
    REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(REPO_ROOT, "settings.json")) as fd:
        parameters = json.load(fd)
        for _k in ("DATA_DIR", "DATA_RAW_DIR", "IMAGE_DIR", "WORKING_DIR", "VISIBILITY_OUTPUT_DIR", "VISIBILITY_ANNOUNCED_OUTPUT_DIR"):
            if isinstance(parameters.get(_k), str) and not os.path.isabs(parameters[_k]):
                parameters[_k] = os.path.normpath(os.path.join(REPO_ROOT, parameters[_k]))
    
    try:
        data_dir = parameters["DATA_DIR"]
        start_date = parameters["START_DATE"]
        end_date = parameters["END_DATE"]
        collectors = parameters["COLLECTORS"]

        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        total_rrcs = len(collectors)

        # Input directory with per-datetime files
        input_dir = parameters["VISIBILITY_ANNOUNCED_OUTPUT_DIR"]
    except Exception as e:
        raise ValueError(f"Invalid parameter file: {e}")

    print(f"Configuration loaded:")
    print(f"  Date range: {start_date.date()} to {end_date.date()}")
    print(f"  Total RRCs: {total_rrcs}")
    print(f"  Input dir: {input_dir}")
    
    # Discover files
    print("\nDiscovering processed files...")
    pattern = f"{input_dir}/visibility_*.pkl"
    visibility_files = sorted(glob.glob(pattern))
    
    print(f"Found {len(visibility_files)} processed visibility files")
    
    # Calculate expected number of files
    delta = datetime.timedelta(hours=8)
    expected_count = 0
    current = start_date
    while current < end_date:
        expected_count += 1
        current += delta

    print(f"Expected: {expected_count} files")

    if len(visibility_files) < expected_count:
        missing = expected_count - len(visibility_files)
        print(f"⚠️  Warning: {missing} files are missing ({missing/expected_count*100:.1f}%)")
    elif len(visibility_files) == expected_count:
        print(f"✓ All expected files are present")
    else:
        print(f"⚠️  Warning: More files than expected ({len(visibility_files)} > {expected_count})")
    
    # Process files in parallel
    num_processes = min(cpu_count(), len(visibility_files))
    num_processes = 15
    print(f"\nProcessing {len(visibility_files)} files using {num_processes} processes...")
    
    start_time = datetime.datetime.now()
    
    with Pool(processes=num_processes) as pool:
        # Process files in parallel with progress updates
        results = []
        for i, result in enumerate(pool.imap_unordered(process_single_file, visibility_files)):
            results.append(result)
            
            # Progress updates
            if (i + 1) % 20 == 0 or (i + 1) == len(visibility_files):
                print(f"  Progress: {i+1}/{len(visibility_files)} ({(i+1)/len(visibility_files)*100:.1f}%)")
    
    end_time = datetime.datetime.now()
    processing_time = (end_time - start_time).total_seconds()
    
    print(f"\n✓ Completed in {processing_time:.2f} seconds ({processing_time/60:.2f} minutes)")
    print(f"  Average: {processing_time/len(visibility_files):.3f} seconds per file")
    
    # Separate successful results from failures
    successful_results = [r[1] for r in results if r[0]]
    failed_results = [r[1] for r in results if not r[0]]
    
    print(f"\n✓ Successfully processed {len(successful_results)} files")
    
    if failed_results:
        print(f"\n⚠️  Failed to load {len(failed_results)} files:")
        for error in failed_results[:10]:
            print(f"  - {error}")
        if len(failed_results) > 10:
            print(f"  ... and {len(failed_results) - 10} more")
    
    # Merge all results
    print("\nMerging results from all files...")
    timeseries_data = merge_file_results(successful_results)
    
    print(f"  Total ASNs with data: {len(timeseries_data):,}")
    
    # Summary statistics
    print("\n" + "=" * 60)
    print("TIME SERIES SUMMARY")
    print("=" * 60)

    # Count ASNs with IPv4 vs IPv6 data
    asns_with_ipv4 = 0
    asns_with_ipv6 = 0

    for asn, asn_data in timeseries_data.items():
        if asn_data[4]['total']:
            asns_with_ipv4 += 1
        if asn_data[6]['total']:
            asns_with_ipv6 += 1

    print(f"\nASN Statistics:")
    print(f"  Total ASNs: {len(timeseries_data):,}")
    print(f"  ASNs with IPv4 data: {asns_with_ipv4:,}")
    print(f"  ASNs with IPv6 data: {asns_with_ipv6:,}")

    # Collect all timestamps
    all_timestamps = set()
    for asn, asn_data in timeseries_data.items():
        for ip_version in [4, 6]:
            all_timestamps.update(asn_data[ip_version]['total'].keys())

    all_timestamps = sorted(all_timestamps)

    print(f"\nTemporal Coverage:")
    print(f"  Total unique timestamps: {len(all_timestamps)}")
    if all_timestamps:
        print(f"  Date range: {min(all_timestamps)} to {max(all_timestamps)}")
        duration = max(all_timestamps) - min(all_timestamps)
        print(f"  Duration: {duration.days} days")

    print(f"\nMetrics Available per ASN and IP version:")
    print(f"  - Total prefixes (all visibility levels)")
    print(f"  - High visibility prefixes (>= 90%)")
    print(f"  - High visibility prefixes (>= 95%)")
    print(f"  - Perfect visibility prefixes (== 100%)")
    
    # Save results
    print("\nSaving time series data...")
    output_dir = f"{data_dir}/processed"
    os.makedirs(output_dir, exist_ok=True)

    output_file = f"{output_dir}/timeseries_prefix_announced_visibility.pkl"

    with open(output_file, "wb") as fd:
        pickle.dump(timeseries_data, fd)

    file_size_mb = os.path.getsize(output_file) / (1024**2)
    print(f"✓ Saved time series data to: {output_file}")
    print(f"  File size: {file_size_mb:.2f} MB")

    # Also save metadata about the time series
    metadata = {
        'total_asns': len(timeseries_data),
        'asns_with_ipv4': asns_with_ipv4,
        'asns_with_ipv6': asns_with_ipv6,
        'total_timestamps': len(all_timestamps),
        'date_range': {
            'start': str(min(all_timestamps)) if all_timestamps else None,
            'end': str(max(all_timestamps)) if all_timestamps else None
        },
        'metrics': ['total', 'high_visibility_90', 'high_visibility', 'perfect_visibility'],
        'processing_time_seconds': processing_time,
        'num_processes': num_processes,
        'created': datetime.datetime.now().isoformat()
    }

    metadata_file = f"{output_dir}/timeseries_announced_metadata.json"
    with open(metadata_file, "w") as fd:
        json.dump(metadata, fd, indent=2, default=str)

    print(f"✓ Saved metadata to: {metadata_file}")
    
    print("\n" + "=" * 60)
    print("✓ Time series data is ready!")
    print("=" * 60)
    
    return timeseries_data


if __name__ == "__main__":
    main()
