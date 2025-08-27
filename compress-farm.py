import os
import subprocess
import yaml
import logging
import threading
import concurrent.futures
from typing import List, Dict, Tuple

# Set up logging
logging.basicConfig(
    filename='/home/phil/plot_replace.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

# List of machines
machines = ['omni', 'neo', 'archon']

def get_plot_dirs(machine: str) -> List[str]:
    config_path = '/home/phil/.chia/mainnet/config/config.yaml'
    try:
        config_output = subprocess.check_output(['ssh', machine, 'cat', config_path]).decode()
        config = yaml.safe_load(config_output)
        return config.get('harvester', {}).get('plot_directories', [])
    except Exception as e:
        logging.error(f'Error getting config from {machine}: {e}')
        return []

def scan_plot_dir(machine: str, plot_dir: str, min_size: int) -> List[Dict]:
    logging.info(f"Scanning plots in {machine}:{plot_dir}")
    try:
        cmd = f"find {plot_dir} -maxdepth 1 -name 'plot-k32-*.plot' -exec stat -c '%s %n' {{}} + 2>/dev/null"
        output = subprocess.check_output(['ssh', machine, cmd]).decode().splitlines()
        plots = []
        for line in output:
            if line.strip():
                parts = line.split(maxsplit=1)
                if len(parts) == 2:
                    size_str, full_path = parts
                    try:
                        file_size = int(size_str)
                    except ValueError:
                        continue
                    if file_size < min_size:
                        logging.info(f'Skipping small plot {full_path} ({file_size / (1 << 30):.2f} GiB)')
                        continue
                    basename = os.path.basename(full_path)
                    plots.append({
                        'machine': machine,
                        'dir': plot_dir,
                        'file': basename,
                        'fullpath': full_path
                    })
        return plots
    except Exception as e:
        logging.error(f'Error scanning {machine}:{plot_dir}: {e}')
        return []

# Collect old plots
old_plots: List[Dict] = []
min_size = 90 * (1 << 30)  # 90 GiB in bytes

# First, get all plot directories sequentially (quick operation)
dir_pairs = []
for machine in machines:
    plot_dirs = get_plot_dirs(machine)
    logging.info(f"Plot directories on {machine}: {', '.join(plot_dirs)}")
    for plot_dir in plot_dirs:
        dir_pairs.append((machine, plot_dir))

# Scan directories in parallel
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    future_to_dir = {executor.submit(scan_plot_dir, m, d, min_size): (m, d) for m, d in dir_pairs}
    for future in concurrent.futures.as_completed(future_to_dir):
        old_plots.extend(future.result())

logging.info(f'Found {len(old_plots)} old plots to replace.')

# Plotting parameters
final_dir = '/var/TmpChia'
buffer_space = 1 << 30  # 1 GiB buffer
plot_command = [
    'chia', 'plotters', 'bladebit', 'cudaplot',
    '--threads', '32',
    '--count', '1',
    '--farmerkey', 'a495c8129d58d6a74e79bf0ed46c88717fa5eb75466be8ebd0745650ca46903cc01cc63699a0ef6b7ac955791058b83e',
    '--contract', 'xch1pkzac5sx0wk9xe7hm8a2g42fr7ep8x7tm7nmf0k06jsjlz98c4rqt6c8df',
    '--tmp_dir', '/var/TmpChia',
    '--tmp_dir2', '/var/TmpChia',
    '--verbose',
    '--final_dir', final_dir,
    '--compress', '5',
    '--disk-128'
]

generation_lock = threading.Lock()
lock = threading.Lock()
pending: Dict[Tuple[str, str], int] = {}

# Pre-generate up to 16 compressed plots
with generation_lock:
    existing_plots = [
        f for f in os.listdir(final_dir)
        if f.startswith('plot-k32-') and f.endswith('.plot')
        and os.path.getsize(os.path.join(final_dir, f)) < min_size
    ]
    num_compressed = len(existing_plots)
    if num_compressed < 16:
        count = 16 - num_compressed
        temp_command = plot_command.copy()
        count_idx = temp_command.index('--count')
        temp_command[count_idx + 1] = str(count)
        logging.info(f'Generating {count} additional compressed plots to reach 16.')
        try:
            subprocess.check_call(temp_command)
        except Exception as e:
            logging.error(f'Error pre-generating plots: {e}')

def transfer_func(old: Dict, tmp_file: str, new_plot_file: str, deleted: bool, mkey: Tuple[str, str], full_tmp: str, new_size: int):
    try:
        logging.info(f'Copying {tmp_file} to {old["machine"]}:{old["dir"]}')
        subprocess.check_call(['scp', full_tmp, f'{old["machine"]}:{old["dir"]}/'])

        logging.info(f'Renaming {tmp_file} to {new_plot_file} on {old["machine"]}')
        subprocess.check_call(
            ['ssh', old['machine'], f'mv "{old["dir"]}/{tmp_file}" "{old["dir"]}/{new_plot_file}"']
        )

        with lock:
            pending[mkey] -= new_size

        logging.info(f'Replacement complete for {old["fullpath"]} with {new_plot_file}')

        # Delete the renamed plot on coroline after transfer complete
        os.remove(full_tmp)

    except Exception as e:
        logging.error(f'Error during transfer for {old["fullpath"]}: {e}')
        subprocess.call(['ssh', old['machine'], f'rm "{old["dir"]}/{tmp_file}" 2>/dev/null'])
        # Do not delete local file on failure

# Group old plots by machine
old_by_machine: Dict[str, List[Dict]] = {}
for old in old_plots:
    mach = old['machine']
    old_by_machine.setdefault(mach, []).append(old)

def process_machine(mach: str, olds: List[Dict]):
    for old in olds:
        logging.info(f'Starting replacement for {old["fullpath"]} on {old["machine"]}')

        with generation_lock:
            existing_plots = sorted([
                f for f in os.listdir(final_dir)
                if f.startswith('plot-k32-') and f.endswith('.plot')
                and os.path.getsize(os.path.join(final_dir, f)) < min_size
            ])
            if existing_plots:
                new_plot_file = existing_plots[0]
                logging.info(f'Using existing plot {new_plot_file}')
            else:
                # List existing plots before generating new one
                before_plots = set(f for f in os.listdir(final_dir) if f.endswith('.plot'))

                # Generate new plot
                try:
                    subprocess.check_call(plot_command)
                except Exception as e:
                    logging.error(f'Error generating plot: {e}')
                    continue

                # Find the new plot
                after_plots = set(f for f in os.listdir(final_dir) if f.endswith('.plot'))
                new_files = list(after_plots - before_plots)
                if len(new_files) != 1:
                    logging.error(f'Unexpected number of new plots: {new_files}')
                    continue
                new_plot_file = new_files[0]

            full_new = os.path.join(final_dir, new_plot_file)
            new_size = os.path.getsize(full_new)

            # Rename new plot to .tmp locally
            tmp_file = new_plot_file + '.tmp'
            full_tmp = os.path.join(final_dir, tmp_file)
            os.rename(full_new, full_tmp)

        # Get free space on target directory
        try:
            free_output = subprocess.check_output(
                ['ssh', old['machine'], f'df -B1 --output=avail "{old["dir"]}" | tail -1']
            ).decode().strip()
            free_space = int(free_output)
        except Exception as e:
            logging.error(f'Error getting free space on {old["machine"]}:{old["dir"]}: {e}')
            os.remove(full_tmp)  # Clean up
            continue

        mkey = (old['machine'], old['dir'])
        with lock:
            pend = pending.get(mkey, 0)
            effective_free = free_space - pend

        deleted = False
        if effective_free < new_size + buffer_space:
            logging.info(
                f'Insufficient space (effective {effective_free / (1 << 30):.2f} GiB free, '
                f'need {(new_size + buffer_space) / (1 << 30):.2f} GiB), '
                f'deleting old plot {old["fullpath"]}'
            )
            try:
                subprocess.check_call(['ssh', old['machine'], f'rm "{old["fullpath"]}"'])
                deleted = True
            except Exception as e:
                logging.error(f'Error deleting old plot {old["fullpath"]}: {e}')
                os.remove(full_tmp)  # Clean up
                continue

        # Add to pending
        with lock:
            pending[mkey] = pending.get(mkey, 0) + new_size

        # Perform transfer
        transfer_func(old, tmp_file, new_plot_file, deleted, mkey, full_tmp, new_size)

with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = []
    for mach, olds in old_by_machine.items():
        futures.append(executor.submit(process_machine, mach, olds))

    concurrent.futures.wait(futures)