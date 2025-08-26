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

# Collect old plots
old_plots: List[Dict] = []
min_size = 90 * (1 << 30)  # 90 GiB in bytes
for machine in machines:
    plot_dirs = get_plot_dirs(machine)
    for plot_dir in plot_dirs:
        try:
            files_output = subprocess.check_output(
                ['ssh', machine, f'ls {plot_dir}/plot-k32-*.plot 2>/dev/null']
            ).decode().splitlines()
            for full_path in files_output:
                # Get file size
                try:
                    size_output = subprocess.check_output(
                        ['ssh', machine, f'stat -c %s "{full_path}"']
                    ).decode().strip()
                    file_size = int(size_output)
                except Exception as e:
                    logging.error(f'Error getting size of {full_path} on {machine}: {e}')
                    continue

                if file_size < min_size:
                    logging.info(f'Skipping small plot {full_path} ({file_size / (1 << 30):.2f} GiB)')
                    continue

                basename = os.path.basename(full_path)
                old_plots.append({
                    'machine': machine,
                    'dir': plot_dir,
                    'file': basename,
                    'fullpath': full_path
                })
        except subprocess.CalledProcessError:
            # No files matching, continue
            pass

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

lock = threading.Lock()
pending: Dict[Tuple[str, str], int] = {}

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

        if not deleted:
            logging.info(f'Deleting old plot {old["fullpath"]}')
            subprocess.check_call(['ssh', old['machine'], f'rm "{old["fullpath"]}"'])

        logging.info(f'Replacement complete for {old["fullpath"]} with {new_plot_file}')

        # Delete the renamed plot on coroline after transfer complete
        os.remove(full_tmp)

    except Exception as e:
        logging.error(f'Error during transfer for {old["fullpath"]}: {e}')
        subprocess.call(['ssh', old['machine'], f'rm "{old["dir"]}/{tmp_file}" 2>/dev/null'])
        # Do not delete local file on failure

with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = []
    for old in old_plots:
        logging.info(f'Starting replacement for {old["fullpath"]} on {old["machine"]}')

        # Check for existing new plots
        existing_plots = [
            f for f in os.listdir(final_dir)
            if f.startswith('plot-k32-') and f.endswith('.plot')
            and os.path.getsize(os.path.join(final_dir, f)) < min_size
        ]
        if existing_plots:
            new_plot_file = sorted(existing_plots)[0]
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

        # Get free space on target directory
        try:
            free_output = subprocess.check_output(
                ['ssh', old['machine'], f'df -B1 --output=avail "{old["dir"]}" | tail -1']
            ).decode().strip()
            free_space = int(free_output)
        except Exception as e:
            logging.error(f'Error getting free space on {old["machine"]}:{old["dir"]}: {e}')
            os.remove(full_new)  # Clean up unused new plot
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
                os.remove(full_new)  # Clean up
                continue

        # Rename new plot to .tmp locally
        tmp_file = new_plot_file + '.tmp'
        full_tmp = os.path.join(final_dir, tmp_file)
        print("The program is paused to rename file. Press Enter to continue.")
        input()  # Pauses here until Enter is pressed
        print("Execution resumed!")
        os.rename(full_new, full_tmp)

        # Add to pending
        with lock:
            pending[mkey] = pending.get(mkey, 0) + new_size

        # Submit to executor
        futures.append(executor.submit(transfer_func, old, tmp_file, new_plot_file, deleted, mkey, full_tmp, new_size))

    concurrent.futures.wait(futures)