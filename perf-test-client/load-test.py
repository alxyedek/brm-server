#!/usr/bin/env python3
"""
Load Testing Tool for HTTP APIs

A simple, efficient load testing tool that performs concurrent HTTP requests
with warmup, detailed statistics, and graceful interruption handling.
"""

import asyncio
import argparse
import signal
import sys
import time
from typing import List, Dict, Any
import urllib.request
import urllib.error
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics


class LoadTester:
    def __init__(self, url: str, concurrent: int, total: int, timeout: int):
        self.url = url
        self.concurrent = concurrent
        self.total = total
        self.timeout = timeout
        self.results: List[Dict[str, Any]] = []
        self.interrupted = False
        self.start_time = None
        self.end_time = None
        self.results_lock = threading.Lock()
        
    def signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        print("\n\n⚠️  Interrupt received. Finishing current requests and showing results...")
        self.interrupted = True
        
    def make_request(self, request_id: int) -> Dict[str, Any]:
        """Make a single HTTP request and record timing"""
        start_time = time.time()
        result = {
            'request_id': request_id,
            'start_time': start_time,
            'end_time': None,
            'response_time': None,
            'status_code': None,
            'success': False,
            'error': None
        }
        
        try:
            req = urllib.request.Request(self.url)
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                end_time = time.time()
                response_time = (end_time - start_time) * 1000  # Convert to milliseconds
                
                result.update({
                    'end_time': end_time,
                    'response_time': response_time,
                    'status_code': response.getcode(),
                    'success': response.getcode() == 200
                })
                
        except urllib.error.HTTPError as e:
            end_time = time.time()
            result.update({
                'end_time': end_time,
                'response_time': (end_time - start_time) * 1000,
                'status_code': e.code,
                'error': f'HTTP {e.code}'
            })
        except urllib.error.URLError as e:
            end_time = time.time()
            result.update({
                'end_time': end_time,
                'response_time': (end_time - start_time) * 1000,
                'error': str(e.reason) if hasattr(e, 'reason') else str(e)
            })
        except Exception as e:
            end_time = time.time()
            result.update({
                'end_time': end_time,
                'response_time': (end_time - start_time) * 1000,
                'error': str(e)
            })
            
        return result
        
    def warmup(self):
        """Perform warmup requests"""
        print(f"Warmup... ({self.concurrent} requests)")
        
        with ThreadPoolExecutor(max_workers=self.concurrent) as executor:
            futures = [executor.submit(self.make_request, i) for i in range(self.concurrent)]
            for future in as_completed(futures):
                try:
                    future.result()  # Just wait for completion, don't store results
                except Exception:
                    pass  # Ignore warmup errors
                    
        print("Warmup complete. Starting test in 1 second...")
        time.sleep(1)
        
    def run_test(self):
        """Run the main load test"""
        print(f"\nRunning load test: ", end="", flush=True)
        
        request_id = 0
        self.start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.concurrent) as executor:
            while request_id < self.total and not self.interrupted:
                # Calculate how many requests to make in this batch
                remaining = self.total - request_id
                batch_size = min(self.concurrent, remaining)
                
                # Submit batch of requests
                futures = [
                    executor.submit(self.make_request, request_id + i) 
                    for i in range(batch_size)
                ]
                
                # Collect results as they complete
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        with self.results_lock:
                            self.results.append(result)
                    except Exception as e:
                        with self.results_lock:
                            self.results.append({
                                'request_id': request_id,
                                'start_time': time.time(),
                                'end_time': time.time(),
                                'response_time': 0,
                                'status_code': None,
                                'success': False,
                                'error': str(e)
                            })
                
                request_id += batch_size
                
                # Update progress
                progress = request_id / self.total
                bar_length = 20
                filled_length = int(bar_length * progress)
                bar = '=' * filled_length + '-' * (bar_length - filled_length)
                print(f"\rRunning load test: [{bar}] {request_id}/{self.total} ({progress:.1%})", end="", flush=True)
                
        self.end_time = time.time()
        print()  # New line after progress bar
        
    def print_results(self):
        """Print detailed test results"""
        if not self.results:
            print("No results to display.")
            return
            
        # Calculate statistics
        successful_requests = [r for r in self.results if r['success']]
        failed_requests = [r for r in self.results if not r['success']]
        
        total_requests = len(self.results)
        successful_count = len(successful_requests)
        failed_count = len(failed_requests)
        success_rate = (successful_count / total_requests) * 100 if total_requests > 0 else 0
        
        # Response time statistics
        response_times = [r['response_time'] for r in self.results if r['response_time'] is not None]
        
        if response_times:
            avg_time = statistics.mean(response_times)
            min_time = min(response_times)
            max_time = max(response_times)
            median_time = statistics.median(response_times)
            
            # Calculate percentiles manually
            sorted_times = sorted(response_times)
            n = len(sorted_times)
            p95_time = sorted_times[int(0.95 * n)] if n > 0 else 0
            p99_time = sorted_times[int(0.99 * n)] if n > 0 else 0
        else:
            avg_time = min_time = max_time = median_time = p95_time = p99_time = 0
            
        # Total elapsed time
        elapsed_time = self.end_time - self.start_time if self.start_time and self.end_time else 0
        
        # Print results
        print("\n" + "=" * 50)
        print("=== Load Test Results ===")
        print(f"Total requests: {total_requests}")
        print(f"Successful (200 OK): {successful_count} ({success_rate:.1f}%)")
        print(f"Failed/Timeout: {failed_count} ({100 - success_rate:.1f}%)")
        print(f"Total elapsed time: {elapsed_time:.2f}s")
        
        if response_times:
            print(f"\nResponse Time Statistics (ms):")
            print(f"  Average: {avg_time:.1f}")
            print(f"  Median (50th): {median_time:.1f}")
            print(f"  Min: {min_time:.1f}")
            print(f"  Max: {max_time:.1f}")
            print(f"  95th percentile: {p95_time:.1f}")
            print(f"  99th percentile: {p99_time:.1f}")
            
        # Error breakdown
        if failed_requests:
            error_types = {}
            for req in failed_requests:
                error = req.get('error', f"HTTP {req.get('status_code', 'unknown')}")
                error_types[error] = error_types.get(error, 0) + 1
                
            print(f"\nError Breakdown:")
            for error, count in error_types.items():
                print(f"  {error}: {count}")
                
        print("=" * 50)
        
        if self.interrupted:
            print("⚠️  Test was interrupted. Results show completed requests only.")


def main():
    parser = argparse.ArgumentParser(
        description="Load testing tool for HTTP APIs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python load-test.py --url http://localhost:8080/rest/blocking --concurrent 10 --total 100
  python load-test.py --url http://localhost:8080/rest/simple --concurrent 50 --total 1000 --timeout 5
  python load-test.py --url "http://localhost:8080/rest/blocking?operation-type=sleep" --concurrent 20 --total 500
        """
    )
    
    parser.add_argument('--url', required=True, help='Target URL to test')
    parser.add_argument('--concurrent', type=int, default=10, help='Number of concurrent requests (default: 10)')
    parser.add_argument('--total', type=int, default=100, help='Total number of requests to make (default: 100)')
    parser.add_argument('--timeout', type=int, default=30, help='Request timeout in seconds (default: 30)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.concurrent <= 0:
        print("Error: concurrent must be greater than 0")
        sys.exit(1)
    if args.total <= 0:
        print("Error: total must be greater than 0")
        sys.exit(1)
    if args.timeout <= 0:
        print("Error: timeout must be greater than 0")
        sys.exit(1)
    if args.concurrent > args.total:
        print("Error: concurrent cannot be greater than total")
        sys.exit(1)
        
    # Print configuration
    print("=== Load Test Configuration ===")
    print(f"URL: {args.url}")
    print(f"Concurrent requests: {args.concurrent}")
    print(f"Total requests: {args.total}")
    print(f"Timeout: {args.timeout}s")
    print("=" * 32)
    
    # Create and run load tester
    tester = LoadTester(args.url, args.concurrent, args.total, args.timeout)
    
    # Set up signal handler for graceful interruption
    signal.signal(signal.SIGINT, tester.signal_handler)
    
    # Run the test
    try:
        tester.warmup()
        tester.run_test()
    except Exception as e:
        print(f"\nError during test execution: {e}")
        sys.exit(1)
    finally:
        tester.print_results()


if __name__ == "__main__":
    main()
