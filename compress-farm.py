import os
import subprocess
import yaml
import logging
import threading
import concurrent.futures
import time
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
                    # Skip compressed plots
                    if file_size < min_size:
#                        logging.info(f'Skipping small plot {full_path} ({file_size / (1 << 30):.2f} GiB)')
# Throwing a comment in because I'm not sure what VS Code is doing with git
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
dirs_by_machine: Dict[str, List[str]] = {}
for machine in machines:
    plot_dirs = get_plot_dirs(machine)
    dirs_by_machine[machine] = plot_dirs
    logging.info(f"Plot directories on {machine}: {', '.join(plot_dirs)}")

dir_pairs = [(m, d) for m in machines for d in dirs_by_machine[m]]

# Scan directories in parallel
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    future_to_dir = {executor.submit(scan_plot_dir, m, d, min_size): (m, d) for m, d in dir_pairs}
    for future in concurrent.futures.as_completed(future_to_dir):
        old_plots.extend(future.result())

logging.info(f'Found {len(old_plots)} old plots to process.')

# Plotting parameters
final_dir = '/var/TmpChia'
buffer_space = 1 << 30  # 1 GiB buffer
stop_threshold = 85 * (1 << 30)  # 85 GiB
compressed_estimate = 85 * (1 << 30)  # ~85 GiB for C5 plot
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
plot_stop = threading.Event()

def plotter_thread():
    while not plot_stop.is_set():
        generated = False
        with generation_lock:
            existing_plots = [
                f for f in os.listdir(final_dir)
                if f.startswith('plot-k32-') and f.endswith('.plot')
                and os.path.getsize(os.path.join(final_dir, f)) < min_size
            ]
            num = len(existing_plots)
            logging.info(f'Local buffer has {num} plots.')
            # Check local free space
            try:
                free_output = subprocess.check_output(
                    ['df', '-B1', '--output=avail', final_dir]
                ).decode().splitlines()[-1].strip()
                local_free = int(free_output)
            except Exception as e:
                logging.error(f'Error getting local free space: {e}')
                local_free = 0
            logging.info(f'Local free space: {local_free / (1 << 30):.2f} GiB')

            if local_free > compressed_estimate + buffer_space:
                logging.info(f'Generating new plot, current local buffer {num}, free {local_free / (1 << 30):.2f} GiB')
                try:
                    subprocess.check_call(plot_command)
                    generated = True
                except Exception as e:
                    logging.error(f'Error generating plot in background: {e}')
            else:
                logging.info(f'Insufficient local space for new plot: {local_free / (1 << 30):.2f} GiB')

        if not generated:
            time.sleep(10)  # Wait 10s if not generated
        else:
            time.sleep(1)  # Short sleep if generated

# Start background plotter
plotter = threading.Thread(target=plotter_thread)
plotter.start()

def transfer_func(machine: str, plot_dir: str, tmp_file: str, new_plot_file: str, full_tmp: str, new_size: int, mkey: Tuple[str, str], old_fullpath: str = None):
    retries = 3
    for attempt in range(retries):
        try:
            logging.info(f'Copying {tmp_file} to {machine}:{plot_dir} (attempt {attempt+1}/{retries})')
            subprocess.check_call(['scp', full_tmp, f'{machine}:{plot_dir}/'])

            logging.info(f'Renaming {tmp_file} to {new_plot_file} on {machine}')
            subprocess.check_call(
                ['ssh', machine, f'mv "{plot_dir}/{tmp_file}" "{plot_dir}/{new_plot_file}"']
            )

            with lock:
                pending[mkey] -= new_size

            if old_fullpath:
                logging.info(f'Deleted {old_fullpath} to make space and added {new_plot_file}')
            else:
                logging.info(f'Added {new_plot_file}')

            # Delete the renamed plot on coroline after transfer complete
            os.remove(full_tmp)
            return

        except Exception as e:
            logging.error(f'Error during transfer to {machine}:{plot_dir} (attempt {attempt+1}/{retries}): {e}')
            subprocess.call(['ssh', machine, f'rm "{plot_dir}/{tmp_file}" 2>/dev/null'])
            if attempt < retries - 1:
                time.sleep(10)  # Wait before retry

    # Final failure - rename back to .plot and put back in pool
    original_file = os.path.join(final_dir, new_plot_file)
    os.rename(full_tmp, original_file)
    logging.error(f'Failed to transfer {tmp_file} after {retries} attempts. Renamed back to {new_plot_file} and returned to pool.')

# Group old plots by filesystem
old_by_fs: Dict[Tuple[str, str], List[Dict]] = {}
for old in old_plots:
    key = (old['machine'], old['dir'])
    old_by_fs.setdefault(key, []).append(old)

def process_machine(mach: str):
    for plot_dir in dirs_by_machine[mach]:
        mkey = (mach, plot_dir)
        remaining_olds = old_by_fs.get(mkey, []).copy()
        logging.info(f'Processing filesystem {mach}:{plot_dir} with {len(remaining_olds)} uncompressed plots')

        while True:
            # Get free space
            try:
                free_output = subprocess.check_output(
                    ['ssh', mach, f'df -B1 --output=avail "{plot_dir}" | tail -1']
                ).decode().strip()
                free_space = int(free_output)
            except Exception as e:
                logging.error(f'Error getting free space on {mach}:{plot_dir}: {e}')
                break

            with lock:
                pend = pending.get(mkey, 0)
                effective_free = free_space - pend

            if effective_free < stop_threshold and len(remaining_olds) == 0:
                logging.info(f'Effective free space {effective_free / (1 << 30):.2f} GiB < 85 GiB and no more uncompressed plots, stopping for {mach}:{plot_dir}')
                break

            # Get a new compressed plot
            with generation_lock:
                existing_plots = sorted([
                    f for f in os.listdir(final_dir)
                    if f.startswith('plot-k32-') and f.endswith('.plot')
                    and os.path.getsize(os.path.join(final_dir, f)) < min_size
                ])
                if not existing_plots:
                    logging.info('No plot available, waiting 10s')
                    time.sleep(10)
                    continue

                new_plot_file = existing_plots[0]
                logging.info(f'Using existing plot {new_plot_file}')

                full_new = os.path.join(final_dir, new_plot_file)
                new_size = os.path.getsize(full_new)

                # Rename new plot to .tmp locally inside lock
                tmp_file = new_plot_file + '.tmp'
                full_tmp = os.path.join(final_dir, tmp_file)
                os.rename(full_new, full_tmp)

            # Recheck effective free (in case pend changed)
            with lock:
                pend = pending.get(mkey, 0)
                effective_free = free_space - pend

            old_fullpath = None
            if effective_free < new_size + buffer_space:
                if not remaining_olds:
                    logging.info(f'Insufficient space and no more uncompressed plots to delete for {mach}:{plot_dir}')
                    # Rename back since not using
                    os.rename(full_tmp, full_new)
                    continue
                old = remaining_olds.pop(0)
                logging.info(
                    f'Insufficient space ({effective_free / (1 << 30):.2f} GiB free, '
                    f'need {(new_size + buffer_space) / (1 << 30):.2f} GiB), '
                    f'deleting uncompressed plot {old["fullpath"]}'
                )
                try:
                    subprocess.check_call(['ssh', mach, f'rm "{old["fullpath"]}"'])
                    old_fullpath = old['fullpath']
                except Exception as e:
                    logging.error(f'Error deleting uncompressed plot {old["fullpath"]}: {e}')
                    # Rename back
                    os.rename(full_tmp, full_new)
                    continue

                # Re-get free space after delete
                try:
                    free_output = subprocess.check_output(
                        ['ssh', mach, f'df -B1 --output=avail "{plot_dir}" | tail -1']
                    ).decode().strip()
                    free_space = int(free_output)
                except Exception as e:
                    logging.error(f'Error getting free space after delete: {e}')
                    # Rename back
                    os.rename(full_tmp, full_new)
                    continue

                with lock:
                    pend = pending.get(mkey, 0)
                    effective_free = free_space - pend

                if effective_free < new_size + buffer_space:
                    logging.error(f'Still insufficient space after delete for {mach}:{plot_dir}')
                    # Rename back
                    os.rename(full_tmp, full_new)
                    continue

            # Add to pending
            with lock:
                pending[mkey] = pending.get(mkey, 0) + new_size

            # Perform transfer
            transfer_func(mach, plot_dir, tmp_file, new_plot_file, full_tmp, new_size, mkey, old_fullpath)

with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = []
    for mach in machines:
        futures.append(executor.submit(process_machine, mach))

    concurrent.futures.wait(futures)

# Stop background plotter
plot_stop.set()
plotter.join()