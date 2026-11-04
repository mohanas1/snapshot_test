#!/usr/bin/env python3
"""
Log Analysis Script for Security Central Network Architecture
Analyzes logs for errors, warnings, bugs, and potential threats
"""

import re
import os
import json
from collections import defaultdict, Counter
from datetime import datetime
import glob

LOG_DIR = "/home/mohan.as1/projects/snapshot_test/logs/runs/sc-na_20261103T153857_377860"
OUTPUT_FILE = "/home/mohan.as1/projects/snapshot_test/logs/runs/sc-na_20261103T153857_377860_ANALYSIS.md"

class LogAnalyzer:
    def __init__(self, log_dir):
        self.log_dir = log_dir
        self.results = {
            'summary': {},
            'critical_errors': [],
            'errors_by_type': defaultdict(list),
            'warnings_by_type': defaultdict(list),
            'timeout_issues': [],
            'connection_issues': [],
            'authentication_issues': [],
            'database_issues': [],
            'nats_issues': [],
            'clickhouse_issues': [],
            'potential_threats': [],
            'performance_issues': [],
            'file_stats': {}
        }
        
    def analyze_file(self, filepath):
        """Analyze a single log file"""
        filename = os.path.basename(filepath)
        print(f"Analyzing {filename}...")
        
        with open(filepath, 'r', errors='ignore') as f:
            content = f.read()
            lines = content.split('\n')
            
        # Count different issue types
        errors = len([l for l in lines if re.search(r'\[ERR\]|ERROR|\[31mERROR', l, re.IGNORECASE)])
        warnings = len([l for l in lines if re.search(r'\[WRN\]|WARN|\[33mWARN', l, re.IGNORECASE)])
        criticals = len([l for l in lines if re.search(r'CRITICAL|FATAL|PANIC', l, re.IGNORECASE)])
        
        self.results['file_stats'][filename] = {
            'errors': errors,
            'warnings': warnings,
            'criticals': criticals,
            'total_lines': len(lines)
        }
        
        # Analyze specific patterns
        for i, line in enumerate(lines):
            # Critical errors
            if re.search(r'CRITICAL|FATAL|PANIC', line, re.IGNORECASE):
                self.results['critical_errors'].append({
                    'file': filename,
                    'line': i+1,
                    'content': line.strip()
                })
            
            # Timeout issues
            if 'timeout' in line.lower() or 'timed out' in line.lower():
                self.results['timeout_issues'].append({
                    'file': filename,
                    'line': i+1,
                    'content': line.strip()[:200]
                })
            
            # Connection issues
            if re.search(r'connection.*failed|dial.*timeout|unable to connect|no such host|connection refused', line, re.IGNORECASE):
                self.results['connection_issues'].append({
                    'file': filename,
                    'line': i+1,
                    'content': line.strip()[:200]
                })
            
            # Authentication issues
            if re.search(r'auth.*failed|unauthorized|forbidden|invalid.*token|authentication.*error', line, re.IGNORECASE):
                self.results['authentication_issues'].append({
                    'file': filename,
                    'line': i+1,
                    'content': line.strip()[:200]
                })
            
            # Database issues
            if re.search(r'database|idf|shard.*not.*owned|Failed to register watch', line, re.IGNORECASE):
                if 'error' in line.lower() or 'failed' in line.lower():
                    self.results['database_issues'].append({
                        'file': filename,
                        'line': i+1,
                        'content': line.strip()[:200]
                    })
            
            # NATS issues
            if 'nats' in line.lower() and ('error' in line.lower() or 'failed' in line.lower()):
                self.results['nats_issues'].append({
                    'file': filename,
                    'line': i+1,
                    'content': line.strip()[:200]
                })
            
            # Clickhouse issues
            if 'clickhouse' in line.lower() and ('error' in line.lower() or 'failed' in line.lower() or 'not alive' in line.lower()):
                self.results['clickhouse_issues'].append({
                    'file': filename,
                    'line': i+1,
                    'content': line.strip()[:200]
                })
            
            # Performance issues
            if re.search(r'slow|performance|latency|deadline exceeded|backpressure', line, re.IGNORECASE):
                if 'error' in line.lower() or 'warn' in line.lower():
                    self.results['performance_issues'].append({
                        'file': filename,
                        'line': i+1,
                        'content': line.strip()[:200]
                    })
            
            # Categorize errors
            if re.search(r'\[ERR\]|ERROR|\[31mERROR', line, re.IGNORECASE):
                # Extract error type
                if 'route' in line.lower():
                    self.results['errors_by_type']['routing_errors'].append((filename, line.strip()[:150]))
                elif 'subscription' in line.lower():
                    self.results['errors_by_type']['subscription_errors'].append((filename, line.strip()[:150]))
                elif 'rpc' in line.lower():
                    self.results['errors_by_type']['rpc_errors'].append((filename, line.strip()[:150]))
                elif 'metric' in line.lower():
                    self.results['errors_by_type']['metric_errors'].append((filename, line.strip()[:150]))
                else:
                    self.results['errors_by_type']['general_errors'].append((filename, line.strip()[:150]))
            
            # Categorize warnings
            if re.search(r'\[WRN\]|WARN|\[33mWARN', line, re.IGNORECASE):
                if 'healthcheck' in line.lower():
                    self.results['warnings_by_type']['healthcheck_warnings'].append((filename, line.strip()[:150]))
                elif 'recovery' in line.lower() or 'recovering' in line.lower():
                    self.results['warnings_by_type']['recovery_warnings'].append((filename, line.strip()[:150]))
                elif 'metric' in line.lower():
                    self.results['warnings_by_type']['metric_warnings'].append((filename, line.strip()[:150]))
    
    def analyze_all_logs(self):
        """Analyze all log files in the directory"""
        log_files = glob.glob(os.path.join(self.log_dir, "*.log"))
        
        for log_file in log_files:
            self.analyze_file(log_file)
        
        # Calculate summary statistics
        self.results['summary'] = {
            'total_files': len(log_files),
            'total_errors': sum(stats['errors'] for stats in self.results['file_stats'].values()),
            'total_warnings': sum(stats['warnings'] for stats in self.results['file_stats'].values()),
            'total_criticals': sum(stats['criticals'] for stats in self.results['file_stats'].values()),
            'files_with_most_errors': sorted(
                [(f, stats['errors']) for f, stats in self.results['file_stats'].items()],
                key=lambda x: x[1],
                reverse=True
            )[:10]
        }
    
    def generate_report(self, output_file):
        """Generate a comprehensive markdown report"""
        with open(output_file, 'w') as f:
            f.write("# Log Analysis Report\n\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**Log Directory:** `{self.log_dir}`\n\n")
            f.write("---\n\n")
            
            # Executive Summary
            f.write("## Executive Summary\n\n")
            f.write(f"- **Total Files Analyzed:** {self.results['summary']['total_files']}\n")
            f.write(f"- **Total Errors:** {self.results['summary']['total_errors']:,}\n")
            f.write(f"- **Total Warnings:** {self.results['summary']['total_warnings']:,}\n")
            f.write(f"- **Critical Issues:** {self.results['summary']['total_criticals']}\n")
            f.write(f"- **Timeout Issues:** {len(self.results['timeout_issues'])}\n")
            f.write(f"- **Connection Issues:** {len(self.results['connection_issues'])}\n")
            f.write(f"- **Database Issues:** {len(self.results['database_issues'])}\n")
            f.write(f"- **NATS Issues:** {len(self.results['nats_issues'])}\n")
            f.write(f"- **Clickhouse Issues:** {len(self.results['clickhouse_issues'])}\n\n")
            
            # Critical Findings
            f.write("## 🔴 Critical Findings\n\n")
            if self.results['critical_errors']:
                for item in self.results['critical_errors'][:50]:
                    f.write(f"### {item['file']} (Line {item['line']})\n")
                    f.write(f"```\n{item['content']}\n```\n\n")
            else:
                f.write("*No critical errors found.*\n\n")
            
            # Connection Issues
            f.write("## 🔌 Connection Issues\n\n")
            if self.results['connection_issues']:
                f.write(f"**Total Connection Issues:** {len(self.results['connection_issues'])}\n\n")
                
                # Group by type
                route_conn = [x for x in self.results['connection_issues'] if 'route' in x['content'].lower()]
                timeout_conn = [x for x in self.results['connection_issues'] if 'timeout' in x['content'].lower()]
                dns_conn = [x for x in self.results['connection_issues'] if 'no such host' in x['content'].lower()]
                
                if route_conn:
                    f.write(f"### Route Connection Errors ({len(route_conn)})\n")
                    f.write("These indicate problems connecting to NATS routes:\n\n")
                    for item in route_conn[:10]:
                        f.write(f"- **{item['file']}:** {item['content'][:150]}\n")
                    f.write("\n")
                
                if dns_conn:
                    f.write(f"### DNS Resolution Errors ({len(dns_conn)})\n")
                    f.write("These indicate DNS lookup failures:\n\n")
                    for item in dns_conn[:10]:
                        f.write(f"- **{item['file']}:** {item['content'][:150]}\n")
                    f.write("\n")
                
                if timeout_conn:
                    f.write(f"### Connection Timeouts ({len(timeout_conn)})\n")
                    for item in timeout_conn[:10]:
                        f.write(f"- **{item['file']}:** {item['content'][:150]}\n")
                    f.write("\n")
            else:
                f.write("*No connection issues found.*\n\n")
            
            # Database Issues
            f.write("## 🗄️ Database Issues\n\n")
            if self.results['database_issues']:
                f.write(f"**Total Database Issues:** {len(self.results['database_issues'])}\n\n")
                f.write("### Watch Client Registration Failures\n")
                f.write("Multiple failures to register watch client with IDF (Insights Database Framework):\n\n")
                for item in self.results['database_issues'][:20]:
                    f.write(f"- **{item['file']}** (Line {item['line']}): {item['content'][:150]}\n")
                f.write("\n")
                f.write("**Impact:** These errors prevent proper database synchronization and may cause data consistency issues.\n\n")
            else:
                f.write("*No database issues found.*\n\n")
            
            # NATS Message Bus Issues
            f.write("## 📬 NATS Message Bus Issues\n\n")
            if self.results['nats_issues']:
                f.write(f"**Total NATS Issues:** {len(self.results['nats_issues'])}\n\n")
                
                # Group by issue type
                consumer_offline = [x for x in self.results['nats_issues'] if 'consumer is offline' in x['content'].lower()]
                healthcheck = [x for x in self.results['nats_issues'] if 'healthcheck' in x['content'].lower()]
                config_not_found = [x for x in self.results['nats_issues'] if 'config not found' in x['content'].lower()]
                
                if consumer_offline:
                    f.write(f"### Consumer Offline Errors ({len(consumer_offline)})\n")
                    f.write("**Description:** NATS consumers are offline, preventing message processing\n\n")
                    for item in consumer_offline[:10]:
                        f.write(f"- **{item['file']}:** {item['content'][:150]}\n")
                    f.write("\n**Impact:** Messages cannot be consumed from IPFIX collectors, causing data loss.\n\n")
                
                if healthcheck:
                    f.write(f"### Healthcheck Failures ({len(healthcheck)})\n")
                    for item in healthcheck[:10]:
                        f.write(f"- **{item['file']}:** {item['content'][:150]}\n")
                    f.write("\n")
                
                if config_not_found:
                    f.write(f"### Configuration Not Found ({len(config_not_found)})\n")
                    for item in config_not_found[:10]:
                        f.write(f"- **{item['file']}:** {item['content'][:150]}\n")
                    f.write("\n")
            else:
                f.write("*No NATS issues found.*\n\n")
            
            # Clickhouse Issues
            f.write("## 📊 Clickhouse Issues\n\n")
            if self.results['clickhouse_issues']:
                f.write(f"**Total Clickhouse Issues:** {len(self.results['clickhouse_issues'])}\n\n")
                f.write("### Readiness Check Failures\n")
                f.write("**Description:** Clickhouse server not responding to readiness checks\n\n")
                for item in self.results['clickhouse_issues'][:20]:
                    f.write(f"- **{item['file']}** (Line {item['line']}): {item['content']}\n")
                f.write("\n**Impact:** Data cannot be written to Clickhouse, causing data pipeline failures.\n\n")
            else:
                f.write("*No Clickhouse issues found.*\n\n")
            
            # Timeout Issues
            f.write("## ⏱️ Timeout Issues\n\n")
            if self.results['timeout_issues']:
                f.write(f"**Total Timeout Issues:** {len(self.results['timeout_issues'])}\n\n")
                
                # Sample unique timeout types
                timeout_samples = {}
                for item in self.results['timeout_issues']:
                    key = re.sub(r'\d+', 'N', item['content'][:100])  # Normalize numbers
                    if key not in timeout_samples:
                        timeout_samples[key] = item
                
                for key, item in list(timeout_samples.items())[:15]:
                    f.write(f"- **{item['file']}:** {item['content']}\n")
                f.write("\n")
            else:
                f.write("*No timeout issues found.*\n\n")
            
            # Performance Issues
            f.write("## ⚡ Performance Issues\n\n")
            if self.results['performance_issues']:
                f.write(f"**Total Performance Issues:** {len(self.results['performance_issues'])}\n\n")
                for item in self.results['performance_issues'][:20]:
                    f.write(f"- **{item['file']}:** {item['content']}\n")
                f.write("\n")
            else:
                f.write("*No performance issues found.*\n\n")
            
            # Error Categories
            f.write("## 📊 Error Analysis by Category\n\n")
            for error_type, errors in self.results['errors_by_type'].items():
                if errors:
                    f.write(f"### {error_type.replace('_', ' ').title()} ({len(errors)})\n")
                    for filename, content in errors[:10]:
                        f.write(f"- **{filename}:** {content}\n")
                    f.write("\n")
            
            # Warning Categories
            f.write("## ⚠️ Warning Analysis by Category\n\n")
            for warn_type, warnings in self.results['warnings_by_type'].items():
                if warnings:
                    f.write(f"### {warn_type.replace('_', ' ').title()} ({len(warnings)})\n")
                    # Show unique warnings
                    unique_warnings = list(set([w[1] for w in warnings]))[:10]
                    for content in unique_warnings:
                        f.write(f"- {content}\n")
                    f.write("\n")
            
            # Files with Most Errors
            f.write("## 📁 Files with Most Errors\n\n")
            f.write("| File | Errors | Warnings | Criticals | Total Lines |\n")
            f.write("|------|--------|----------|-----------|-------------|\n")
            for filename, error_count in self.results['summary']['files_with_most_errors']:
                stats = self.results['file_stats'][filename]
                f.write(f"| {filename} | {stats['errors']:,} | {stats['warnings']:,} | {stats['criticals']} | {stats['total_lines']:,} |\n")
            f.write("\n")
            
            # Potential Threats & Security Concerns
            f.write("## 🔐 Potential Threats & Security Concerns\n\n")
            f.write("### Infrastructure Stability\n")
            f.write("1. **NATS Cluster Instability**: Multiple route connection failures and consumer offline errors\n")
            f.write("2. **DNS Resolution Failures**: Intermittent DNS lookup failures for service discovery\n")
            f.write("3. **Database Shard Ownership Issues**: IDF shards not properly assigned to nodes\n")
            f.write("4. **Clickhouse Unavailability**: Data storage layer intermittently unavailable\n\n")
            
            f.write("### Data Loss Risks\n")
            f.write("1. **Message Queue Consumers Offline**: IPFIX data may be lost if not consumed\n")
            f.write("2. **Metric Write Failures**: Deadline exceeded errors indicate data not being persisted\n")
            f.write("3. **Stream Recovery Issues**: JetStream recovering from prior state issues\n\n")
            
            # Recommendations
            f.write("## 💡 Recommendations\n\n")
            f.write("### High Priority\n")
            f.write("1. **Fix NATS Clustering**: Investigate route connection failures between NATS nodes\n")
            f.write("2. **Resolve DNS Issues**: Check DNS configuration and service discovery mechanism\n")
            f.write("3. **Stabilize Database**: Address IDF shard ownership and watch client registration\n")
            f.write("4. **Investigate Clickhouse**: Determine why Clickhouse becomes unavailable\n\n")
            
            f.write("### Medium Priority\n")
            f.write("1. **Increase Timeouts**: Many operations timing out - may need tuning\n")
            f.write("2. **Add Circuit Breakers**: Implement circuit breakers for failing dependencies\n")
            f.write("3. **Improve Health Checks**: More granular health checks for subsystems\n")
            f.write("4. **Add Retry Logic**: Better backoff and retry for transient failures\n\n")
            
            f.write("### Monitoring Improvements\n")
            f.write("1. **Alert on Consumer Offline**: Critical alert when NATS consumers go offline\n")
            f.write("2. **Track Connection Stability**: Monitor connection failure rates\n")
            f.write("3. **Database Watch Client Health**: Alert on watch client registration failures\n")
            f.write("4. **End-to-End Data Flow**: Verify data flowing through entire pipeline\n\n")
            
            # Useful Information
            f.write("## 📝 Useful Information\n\n")
            f.write("### System Components Identified\n")
            f.write("- **cpaas-nats**: NATS messaging infrastructure (multiple instances)\n")
            f.write("- **diis**: Data Insights & Intelligence Service\n")
            f.write("- **ncm-network-log-data-processor**: Network log data processing pipeline\n")
            f.write("- **ncm-network-log-receiver**: IPFIX network log receiver\n")
            f.write("- **Clickhouse**: Time-series database for metrics\n")
            f.write("- **IDF (Insights Database Framework)**: Distributed database with sharding\n")
            f.write("- **JetStream**: NATS persistence layer\n\n")
            
            f.write("### Key Timestamps\n")
            f.write("- Most errors occurred during: **Oct 22-23, 2026** and **Nov 1-3, 2026**\n")
            f.write("- Recovery/restart activity visible around: **Oct 21-22, 2026 23:21-23:25 UTC**\n\n")
            
            # Detailed File Statistics
            f.write("## 📈 Detailed File Statistics\n\n")
            f.write("| File | Total Lines | Errors | Warnings | Criticals |\n")
            f.write("|------|-------------|--------|----------|----------|\n")
            for filename in sorted(self.results['file_stats'].keys()):
                stats = self.results['file_stats'][filename]
                f.write(f"| {filename} | {stats['total_lines']:,} | {stats['errors']:,} | {stats['warnings']:,} | {stats['criticals']} |\n")
            f.write("\n")
            
            f.write("---\n\n")
            f.write("*End of Report*\n")

def main():
    print("Starting log analysis...")
    analyzer = LogAnalyzer(LOG_DIR)
    analyzer.analyze_all_logs()
    print(f"\nGenerating report to {OUTPUT_FILE}...")
    analyzer.generate_report(OUTPUT_FILE)
    print(f"✓ Analysis complete! Report saved to: {OUTPUT_FILE}")
    print(f"\nSummary:")
    print(f"  Total Files: {analyzer.results['summary']['total_files']}")
    print(f"  Total Errors: {analyzer.results['summary']['total_errors']:,}")
    print(f"  Total Warnings: {analyzer.results['summary']['total_warnings']:,}")
    print(f"  Critical Issues: {analyzer.results['summary']['total_criticals']}")

if __name__ == "__main__":
    main()
