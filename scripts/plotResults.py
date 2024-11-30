import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
import os

def plot_timing_cost(inc_enabled, traditional):
    plt.figure(figsize=(14, 7))
    plt.plot(inc_enabled['CompNodes'], inc_enabled['TimingCost'], label='INC-enabled', marker='o')
    plt.plot(traditional['CompNodes'], traditional['TimingCost'], label='Traditional', marker='D')
    plt.title('Timing Cost', fontweight='bold', fontsize=18)
    plt.xlabel('Computing Nodes', fontsize=14, fontweight='bold')
    plt.ylabel('Timing Cost (Ticks)', fontsize=14, fontweight='bold')
    plt.legend(loc='upper left', fontsize=12)
    plt.grid(True)
    plt.xscale('log')
    plt.yscale('linear')
    max_comp_nodes = max(inc_enabled['CompNodes'].max(), traditional['CompNodes'].max())
    min_comp_nodes = min(inc_enabled['CompNodes'].min(), traditional['CompNodes'].min())
    ticks = np.logspace(np.log10(min_comp_nodes), np.log10(max_comp_nodes), num=10)
    plt.xticks(ticks, [f'{int(tick):,}' for tick in ticks])
    plt.tight_layout(pad=0.1)
    plt.savefig('results/TimingCost.png', bbox_inches='tight')
    plt.close()

def plot_bandwidth_usage_msg(inc_enabled, traditional):
    plt.figure(figsize=(14, 7))
    plt.plot(inc_enabled['CompNodes'], inc_enabled['BandwidthUsage_Msg'], label='INC-enabled', marker='o')
    plt.plot(traditional['CompNodes'], traditional['BandwidthUsage_Msg'], label='Traditional', marker='D')
    plt.title('Bandwidth Usage (Message Based)', fontweight='bold', fontsize=18)
    plt.xlabel('Computing Nodes', fontsize=14, fontweight='bold')
    plt.ylabel('Bandwidth Usage (Messages)', fontsize=14, fontweight='bold')
    plt.legend(loc='upper left', fontsize=12)
    plt.grid(True)
    plt.xscale('log')
    plt.yscale('linear')
    max_comp_nodes = max(inc_enabled['CompNodes'].max(), traditional['CompNodes'].max())
    min_comp_nodes = min(inc_enabled['CompNodes'].min(), traditional['CompNodes'].min())
    ticks = np.logspace(np.log10(min_comp_nodes), np.log10(max_comp_nodes), num=10)
    plt.xticks(ticks, [f'{int(tick):,}' for tick in ticks])
    plt.tight_layout(pad=0.1)
    plt.savefig('results/BandwidthUsage_Msg.png', bbox_inches='tight')
    plt.close()

def plot_bandwidth_usage_byte(inc_enabled, traditional):
    plt.figure(figsize=(14, 7))
    plt.plot(inc_enabled['CompNodes'], inc_enabled['BandwidthUsage_Byte'], label='INC-enabled', marker='o')
    plt.plot(traditional['CompNodes'], traditional['BandwidthUsage_Byte'], label='Traditional', marker='D')
    plt.title('Bandwidth Usage', fontweight='bold', fontsize=18)
    plt.xlabel('Computing Nodes', fontsize=14, fontweight='bold')
    plt.ylabel('Bandwidth Usage (Bytes)', fontsize=14, fontweight='bold')
    plt.legend(loc='upper left', fontsize=12)
    plt.grid(True)
    plt.xscale('log')
    plt.yscale('linear')
    max_comp_nodes = max(inc_enabled['CompNodes'].max(), traditional['CompNodes'].max())
    min_comp_nodes = min(inc_enabled['CompNodes'].min(), traditional['CompNodes'].min())
    ticks = np.logspace(np.log10(min_comp_nodes), np.log10(max_comp_nodes), num=10)
    plt.xticks(ticks, [f'{int(tick):,}' for tick in ticks])
    plt.tight_layout(pad=0.1)
    plt.savefig('results/BandwidthUsage_Byte.png', bbox_inches='tight')
    plt.close()

def plot_compl_time_diff(inc_enabled, traditional):
    plt.figure(figsize=(14, 7))
    plt.plot(inc_enabled['CompNodes'], inc_enabled['ComplTimeDiff'], label='INC-enabled', marker='o')
    plt.plot(traditional['CompNodes'], traditional['ComplTimeDiff'], label='Traditional', marker='D')
    plt.title('Completion Time Variation', fontweight='bold', fontsize=18)
    plt.xlabel('Computing Nodes', fontsize=14, fontweight='bold')
    plt.ylabel('Max. Completion Time Difference (Ticks)', fontsize=14, fontweight='bold')
    plt.legend(loc='upper left', fontsize=12)
    plt.grid(True)
    plt.xscale('log')
    plt.yscale('linear')
    max_comp_nodes = max(inc_enabled['CompNodes'].max(), traditional['CompNodes'].max())
    min_comp_nodes = min(inc_enabled['CompNodes'].min(), traditional['CompNodes'].min())
    ticks = np.logspace(np.log10(min_comp_nodes), np.log10(max_comp_nodes), num=10)
    plt.xticks(ticks, [f'{int(tick):,}' for tick in ticks])
    plt.tight_layout(pad=0.1)
    plt.savefig('results/ComplTimeDiff.png', bbox_inches='tight')
    plt.close()

def visualize_data(csv_file_path='result.csv'):
    if not os.path.exists(csv_file_path):
        print(f"File not found: {csv_file_path}")
        sys.exit(1)

    data = pd.read_csv(csv_file_path)

    inc_enabled = data[data['INC'] == 1]
    traditional = data[data['INC'] == 0]

    # Create results directory if it doesn't exist
    if not os.path.exists('results'):
        os.makedirs('results')

    plot_timing_cost(inc_enabled, traditional)
    plot_bandwidth_usage_msg(inc_enabled, traditional)
    plot_bandwidth_usage_byte(inc_enabled, traditional)
    plot_compl_time_diff(inc_enabled, traditional)

if __name__ == "__main__":
    if len(sys.argv) > 2:
        print("Usage: python plotResults.py [<csv_file_path>]")
        sys.exit(1)

    csv_file_path = sys.argv[1] if len(sys.argv) == 2 else 'result.csv'
    visualize_data(csv_file_path)