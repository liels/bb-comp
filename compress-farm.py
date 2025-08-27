To view keyboard shortcuts, press question mark
View keyboard shortcuts

Home 

Explore 

Notifications 

Messages 

SuperGrok 

Premium 

Lists 

Bookmarks 

Jobs 

Communities 

Articles 

Verified Orgs 

Profile 

More 

Post

Philip Imants Long 

@Phil_I_Long

Expert

See new posts

This command creates compressed chia plots on plotter coroline:chia plotters bladebit cudaplot --threads 32 --count 12 --farmerkey a495c8129d58d6a74e79bf0ed46c88717fa5eb75466be8ebd0745650ca46903cc01cc63699a0ef6b7ac955791058b83e \
--contract xch1pkzac5sx0wk9xe7hm8a2g42fr7ep8x7tm7nmf0k06jsjlz98c4rqt6c8df --tmp_dir /var/TmpChia --tmp_dir2 /var/TmpChia --verbose --final_dir /var/TmpChia --compress 5 --disk-128omni is the farmer, neo and archon are harvesters.  All chia processes are run by phil, configuration in /home/phil/.chia/mainnet/config/config.yaml  who has public key ssh access to those machines.  The directories where plots exist can be found in the config files.Write a script for me that will generate the compressed plots, rename them name.tmp and distribute them to the other machines chia directories and once the transfer is complete rename the file back to name.  Give me a log file to track progress.

Thoughts

This command creates compressed chia plots on plotter coroline:chia plotters bladebit cudaplot --threads 32 --count 12 --farmerkey a495c8129d58d6a74e79bf0ed46c88717fa5eb75466be8ebd0745650ca46903cc01cc63699a0ef6b7ac955791058b83e \
--contract xch1pkzac5sx0wk9xe7hm8a2g42fr7ep8x7tm7nmf0k06jsjlz98c4rqt6c8df --tmp_dir /var/TmpChia --tmp_dir2 /var/TmpChia --verbose --final_dir /var/TmpChia --compress 5 --disk-128omni is the farmer and has local plots, neo and archon are remote harvesters.  All chia processes are run by phil, configuration in /home/phil/.chia/mainnet/config/config.yaml  who has public key ssh access to those machines.  The directories where plots exist can be found in the config files.Write a script for me in python that will generate the compressed plots, rename them name.tmp and distribute them to the other machines chia directories and once the transfer is complete rename the file back to name.  Give me a log file to track progress.

Thoughts

5 web pages

This command creates compressed chia plots on plotter coroline:chia plotters bladebit cudaplot --threads 32 --count 12 --farmerkey a495c8129d58d6a74e79bf0ed46c88717fa5eb75466be8ebd0745650ca46903cc01cc63699a0ef6b7ac955791058b83e  --contract xch1pkzac5sx0wk9xe7hm8a2g42fr7ep8x7tm7nmf0k06jsjlz98c4rqt6c8df --tmp_dir /var/TmpChia --tmp_dir2 /var/TmpChia --verbose --final_dir /var/TmpChia --compress 5 --disk-128omni is the farmer and has local plots, neo and archon are remote harvesters.  All chia processes are run by phil, configuration in /home/phil/.chia/mainnet/config/config.yaml  who has public key ssh access to those machines.  The directories where plots exist can be found in the config files.I want to replace the existing plots with 2022 date stamps/names with compressed plots. Write a script for me in python that will Generate the compressed plots, rename them name.tmp
Check for sufficient space in the filesystem for the new plot, if there isn't enough delete an existing plot to make room. 
Copy the renamed plot to the remote filesystem. Once the transfer is complete rename the file back to name.

Give me a log file to track progress.

Thoughts

python

import os
import subprocess
import yaml
import logging
from typing import List, Dict

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
for machine in machines:
    plot_dirs = get_plot_dirs(machine)
    for plot_dir in plot_dirs:
        try:
            files_output = subprocess.check_output(
                ['ssh', machine, f'ls {plot_dir}/plot-k32-2022*.plot 2>/dev/null']
            ).decode().splitlines()
            for full_path in files_output:
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

for old in old_plots:
    logging.info(f'Starting replacement for {old["fullpath"]} on {old["machine"]}')

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
        continue

    deleted = False
    if free_space < new_size + buffer_space:
        logging.info(
            f'Insufficient space ({free_space / (1 << 30):.2f} GiB free, '
            f'need {(new_size + buffer_space) / (1 << 30):.2f} GiB), '
            f'deleting old plot {old["fullpath"]}'
        )
        try:
            subprocess.check_call(['ssh', old['machine'], f'rm "{old["fullpath"]}"'])
            deleted = True
        except Exception as e:
            logging.error(f'Error deleting old plot {old["fullpath"]}: {e}')
            continue

    # Rename new plot to .tmp locally
    tmp_file = new_plot_file + '.tmp'
    full_tmp = os.path.join(final_dir, tmp_file)
    os.rename(full_new, full_tmp)

    # Copy .tmp to target
    logging.info(f'Copying {tmp_file} to {old["machine"]}:{old["dir"]}')
    try:
        subprocess.check_call(['scp', full_tmp, f'{old["machine"]}:{old["dir"]}/'])
    except Exception as e:
        logging.error(f'Error copying {tmp_file} to {old["machine"]}:{old["dir"]}: {e}')
        # Attempt to clean up local tmp
        os.remove(full_tmp)
        continue

    # Rename on remote to original name
    logging.info(f'Renaming {tmp_file} to {new_plot_file} on {old["machine"]}')
    try:
        subprocess.check_call(
            ['ssh', old['machine'], f'mv "{old["dir"]}/{tmp_file}" "{old["dir"]}/{new_plot_file}"']
        )
    except Exception as e:
        logging.error(f'Error renaming on remote: {e}')
        # Clean up local and attempt remote cleanup
        os.remove(full_tmp)
        subprocess.call(['ssh', old['machine'], f'rm "{old["dir"]}/{tmp_file}"'])
        continue

    # Delete old plot if not already deleted
    if not deleted:
        logging.info(f'Deleting old plot {old["fullpath"]}')
        try:
            subprocess.check_call(['ssh', old['machine'], f'rm "{old["fullpath"]}"'])
        except Exception as e:
            logging.error(f'Error deleting old plot {old["fullpath"]}: {e}')

    # Clean up local .tmp
    os.remove(full_tmp)

    logging.info(f'Replacement complete for {old["fullpath"]} with {new_plot_file}')

5 web pages

Modify the script to check for existing plots in /var/TmpChia.  If enough exist transfer and delete them before generating new plots.

Thoughts

python

import os
import subprocess
import yaml
import logging
from typing import List, Dict

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
for machine in machines:
    plot_dirs = get_plot_dirs(machine)
    for plot_dir in plot_dirs:
        try:
            files_output = subprocess.check_output(
                ['ssh', machine, f'ls {plot_dir}/plot-k32-2022*.plot 2>/dev/null']
            ).decode().splitlines()
            for full_path in files_output:
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

for old in old_plots:
    logging.info(f'Starting replacement for {old["fullpath"]} on {old["machine"]}')

    # Check for existing new plots
    existing_plots = [f for f in os.listdir(final_dir) if f.startswith('plot-k32-') and f.endswith('.plot') and '2022' not in f]
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
        continue

    deleted = False
    if free_space < new_size + buffer_space:
        logging.info(
            f'Insufficient space ({free_space / (1 << 30):.2f} GiB free, '
            f'need {(new_size + buffer_space) / (1 << 30):.2f} GiB), '
            f'deleting old plot {old["fullpath"]}'
        )
        try:
            subprocess.check_call(['ssh', old['machine'], f'rm "{old["fullpath"]}"'])
            deleted = True
        except Exception as e:
            logging.error(f'Error deleting old plot {old["fullpath"]}: {e}')
            continue

    # Rename new plot to .tmp locally
    tmp_file = new_plot_file + '.tmp'
    full_tmp = os.path.join(final_dir, tmp_file)
    os.rename(full_new, full_tmp)

    # Copy .tmp to target
    logging.info(f'Copying {tmp_file} to {old["machine"]}:{old["dir"]}')
    try:
        subprocess.check_call(['scp', full_tmp, f'{old["machine"]}:{old["dir"]}/'])
    except Exception as e:
        logging.error(f'Error copying {tmp_file} to {old["machine"]}:{old["dir"]}: {e}')
        # Attempt to clean up local tmp
        os.remove(full_tmp)
        continue

    # Rename on remote to original name
    logging.info(f'Renaming {tmp_file} to {new_plot_file} on {old["machine"]}')
    try:
        subprocess.check_call(
            ['ssh', old['machine'], f'mv "{old["dir"]}/{tmp_file}" "{old["dir"]}/{new_plot_file}"']
        )
    except Exception as e:
        logging.error(f'Error renaming on remote: {e}')
        # Clean up local and attempt remote cleanup
        os.remove(full_tmp)
        subprocess.call(['ssh', old['machine'], f'rm "{old["dir"]}/{tmp_file}"'])
        continue

    # Delete old plot if not already deleted
    if not deleted:
        logging.info(f'Deleting old plot {old["fullpath"]}')
        try:
            subprocess.check_call(['ssh', old['machine'], f'rm "{old["fullpath"]}"'])
        except Exception as e:
            logging.error(f'Error deleting old plot {old["fullpath"]}: {e}')

    # Clean up local .tmp
    os.remove(full_tmp)

    logging.info(f'Replacement complete for {old["fullpath"]} with {new_plot_file}')

Modify the script to leave alone any plots that are smaller than 90GB.  They are new compressed ones already on the target system.

Thoughts

python

import os
import subprocess
import yaml
import logging
from typing import List, Dict

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
                ['ssh', machine, f'ls {plot_dir}/plot-k32-2022*.plot 2>/dev/null']
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

for old in old_plots:
    logging.info(f'Starting replacement for {old["fullpath"]} on {old["machine"]}')

    # Check for existing new plots
    existing_plots = [f for f in os.listdir(final_dir) if f.startswith('plot-k32-') and f.endswith('.plot') and '2022' not in f]
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
        continue

    deleted = False
    if free_space < new_size + buffer_space:
        logging.info(
            f'Insufficient space ({free_space / (1 << 30):.2f} GiB free, '
            f'need {(new_size + buffer_space) / (1 << 30):.2f} GiB), '
            f'deleting old plot {old["fullpath"]}'
        )
        try:
            subprocess.check_call(['ssh', old['machine'], f'rm "{old["fullpath"]}"'])
            deleted = True
        except Exception as e:
            logging.error(f'Error deleting old plot {old["fullpath"]}: {e}')
            continue

    # Rename new plot to .tmp locally
    tmp_file = new_plot_file + '.tmp'
    full_tmp = os.path.join(final_dir, tmp_file)
    os.rename(full_new, full_tmp)

    # Copy .tmp to target
    logging.info(f'Copying {tmp_file} to {old["machine"]}:{old["dir"]}')
    try:
        subprocess.check_call(['scp', full_tmp, f'{old["machine"]}:{old["dir"]}/'])
    except Exception as e:
        logging.error(f'Error copying {tmp_file} to {old["machine"]}:{old["dir"]}: {e}')
        # Attempt to clean up local tmp
        os.remove(full_tmp)
        continue

    # Rename on remote to original name
    logging.info(f'Renaming {tmp_file} to {new_plot_file} on {old["machine"]}')
    try:
        subprocess.check_call(
            ['ssh', old['machine'], f'mv "{old["dir"]}/{tmp_file}" "{old["dir"]}/{new_plot_file}"']
        )
    except Exception as e:
        logging.error(f'Error renaming on remote: {e}')
        # Clean up local and attempt remote cleanup
        os.remove(full_tmp)
        subprocess.call(['ssh', old['machine'], f'rm "{old["dir"]}/{tmp_file}"'])
        continue

    # Delete old plot if not already deleted
    if not deleted:
        logging.info(f'Deleting old plot {old["fullpath"]}')
        try:
            subprocess.check_call(['ssh', old['machine'], f'rm "{old["fullpath"]}"'])
        except Exception as e:
            logging.error(f'Error deleting old plot {old["fullpath"]}: {e}')

    # Clean up local .tmp
    os.remove(full_tmp)

    logging.info(f'Replacement complete for {old["fullpath"]} with {new_plot_file}')

remove the filename test for old plots.  Some are 2021.  Rely on size alone.

Thoughts

python

import os
import subprocess
import yaml
import logging
from typing import List, Dict

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
    '--farmerkey', 'a495c8129d58d6a74e79bf0ed46cimport88717fa5eb75466be8ebd0745650ca46903cc01cc63699a0ef6b7ac955791058b83e',
    '--contract', 'xch1pkzac5sx0wk9xe7hm8a2g42fr7ep8x7tm7nmf0k06jsjlz98c4rqt6c8df',
    '--tmp_dir', '/var/TmpChia',
    '--tmp_dir2', '/var/TmpChia',
    '--verbose',
    '--final_dir', final_dir,
    '--compress', '5',
    '--disk-128'
]

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
 Dict        try:
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
            ['ssh', old['machine'], f'df -B1 --output=(['avail "{old["dir"]}" | tail -1']
        ).decode().strip()
        free_space = int(free_output)
    except Exception as e:
:
        logging logging.error(f'Error getting free space on {["machine"]}:{old["dir"]}: {e}')
        continue

    deleted = False
    if free_space < new_size + buffer_space:
        logging.info(
            f'Insufficient space ({free_space / (1 << 30):.2f} GiB free, '
            f'need {(new_size + buffer_space) / (1 << 30):.2f} GiB), '
            f'deleting old plot {old["fullpath"]}'
        )
        try:
            subprocess.check_call(['ssh', old['machine'], f'rm "{old["fullpath"]}" deleted = True
        except Exception as e:
            logging.error(f'Error deleting old plot {old["fullpath"]}: {e}')
            continue

    # Rename new plot to .tmp locally
    tmp_file = new_plot_file + '.tmp'
    full_tmp = os.path.join(final_dir, tmp_file)
    os.rename(full_new, full_tmp)

    # Copy .tmp to target
    logging.info(f'Copying {tmp_file} to {old["machine"]}:{old["dir"]}')
    try:
        subprocess.check_call(['scp', full_tmp, f'{old["machine"]}:{old["dir"]}/'])
    except Exception as e:
        logging.error(f'Error copying {tmp_file} to {old["machine"]}:{old["dir"]}: {e}')
        # Attempt to clean up local tmp
        os.remove(full_tmp)
        continue

    # Rename on remote to original name
    logging.info(f'Renaming {tmp_file} to {new_plot_file} on {old["machine"]}')
    try:
        subprocess.check_call(
            ['ssh', old['machine'], f'm955v "{old["dir"]}/{tmp_file}" "{old["dir"]}/{new_plot_file}"']
        )
    except Exception as e:
        logging.error(f'Error renaming on remote: {e}')
        # Clean up local and attempt remote cleanup
        os.remove(full_tmp)
        subprocess.call(['ssh', old['machine'], f'rm "{Chold["dir"]}/{tmp_file}"'])
        continue

    # Delete old plot if not already deleted
    if not deleted:
        logging.info(f'Deleting old plot {old["fullpath"]}')
        try:
            subprocess.check_call(['ssh', old['machine'], f'rm "{old["fullpath"]}"'])
        except Exception as e:
            logging.error(f'Error deleting old plot {old["fullpath"]}: {e}')

    # Clean up local .tmp
    os.remove(full_tmp)

    logging.info(f'Replacement complete for {old["fullpath"]} with {new_plot_file}')

Modify the code to do each target machine (archon, omni, neo) in paralell so that it is likely that 3 scps are going on at once.

Thoughts

python

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

    except Exception as e:
        logging.error(f'Error during transfer for {old["fullpath"]}: {e}')
        subprocess.call(['ssh', old['machine'], f'rm "{old["dir"]}/{tmp_file}" 2>/dev/null'])

    finally:
        os.remove(full_tmp)

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
        os.rename(full_new, full_tmp)

        # Add to pending
        with lock:
            pending[mkey] = pending.get(mkey, 0) + new_size

        # Submit to executor
        futures.append(executor.submit(transfer_func, old, tmp_file, new_plot_file, deleted, mkey, full_tmp, new_size))

    concurrent.futures.wait(futures)

Modify the code to delete the renamed plot on coroline in /var/TmpChia after the transfer to omni, neo, or archon is complete.  Coroline is the plotter,the other three are remote to that machine.

Thoughts

python

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
        os.rename(full_new, full_tmp)

        # Add to pending
        with lock:
            pending[mkey] = pending.get(mkey, 0) + new_size

        # Submit to executor
        futures.append(executor.submit(transfer_func, old, tmp_file, new_plot_file, deleted, mkey, full_tmp, new_size))

    concurrent.futures.wait(futures)

Modify the code to log which remote filesystems are being scanned for plots.

Thoughts

python

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
    logging.info(f"Plot directories on {machine}: {', '.join(plot_dirs)}")
    for plot_dir in plot_dirs:
        logging.info(f"Scanning plots in {machine}:{plot_dir}")
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
        os.rename(full_new, full_tmp)

        # Add to pending
        with lock:
            pending[mkey] = pending.get(mkey, 0) + new_size

        # Submit to executor
        futures.append(executor.submit(transfer_func, old, tmp_file, new_plot_file, deleted, mkey, full_tmp, new_size))

    concurrent.futures.wait(futures)

Modify the code to scan all the remote filesystems in parallel.

Thoughts

python

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
        os.rename(full_new, full_tmp)

        # Add to pending
        with lock:
            pending[mkey] = pending.get(mkey, 0) + new_size

        # Submit to executor
        futures.append(executor.submit(transfer_func, old, tmp_file, new_plot_file, deleted, mkey, full_tmp, new_size))

    concurrent.futures.wait(futures)

Modify the code so that there is no more than one scp to each target machine at a time.  The state we want is one compressed plot being copied to one filesystem on each remote system simultaneously.   Also, if there are fewer than 16 compressed plots in /var/TmpChia on coroline, create new plots up to 16.  Use the count parameter.

Thoughts

python

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

If there is enough space for the copied plot plus the buffer there is no need to delete a plot to make space.  The way the script works now a new copy only starts after an old one is done and a space check is complete.  None of the directories should have more than 200GB free.  [phil@omni comp-plots]$ df -h
Filesystem                  Size  Used Avail Use% Mounted on
devtmpfs                     63G     0   63G   0% /dev
tmpfs                        63G   20K   63G   1% /dev/shm
tmpfs                        63G   26M   63G   1% /run
tmpfs                        63G     0   63G   0% /sys/fs/cgroup
/dev/mapper/almalinux-root   70G   30G   41G  43% /
/dev/mapper/almalinux-home  829G  260G  570G  32% /home
/dev/nvme0n1p3             1014M  454M  561M  45% /boot
/dev/sdj                     13T   13T   75G 100% /IntChia17
/dev/sdm                     13T   13T   75G 100% /IntChia13
/dev/sdl                     13T   13T   74G 100% /IntChia10
/dev/sde                     13T   13T   75G 100% /var/IntChia8
/dev/sdk                     13T   13T  457G  97% /IntChia15
/dev/sdg                     13T   13T   96G 100% /var/IntChia9
/dev/sdf                    9.1T  9.1T   24G 100% /var/IntChia6
/dev/sdc                     13T   13T  166G  99% /var/IntChia14
/dev/sdh                     13T   13T   74G 100% /IntChia16
/dev/sdn                     17T   17T   40K 100% /var/IntChiaX3
/dev/sdi                    9.1T  9.1T   25G 100% /var/IntChia11
/dev/sdb                    9.1T  9.1T   44G 100% /var/IntChiaX2
neo:/storage                 90T   85T  4.2T  96% /storage
tmpfs                        13G   12K   13G   1% /run/user/42
/dev/sdd                     13T   13T  561G  96% /IntChia12
/dev/sda                     13T   13T   75G 100% /IntChia18
tmpfs                        13G     0   13G   0% /run/user/1000
[phil@omni comp-plots]$ Update the script so that old plots are only deleted when space is needed for new plots.

Thought for 58s

python

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

Minor updates to the script as a new baseline:import os
import subprocess
import yaml
import logging
import threading
import concurrent.futures
from typing import List, Dict, Tuple# Set up logging
logging.basicConfig(
    filename='/home/phil/plot_replace.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)# List of machines
machines = ['omni', 'neo', 'archon']def get_plot_dirs(machine: str) -> List[str]:
    config_path = '/home/phil/.chia/mainnet/config/config.yaml'
    try:
        config_output = subprocess.check_output(['ssh', machine, 'cat', config_path]).decode()
        config = yaml.safe_load(config_output)
        return config.get('harvester', {}).get('plot_directories', [])
    except Exception as e:
        logging.error(f'Error getting config from {machine}: {e}')
        return []def scan_plot_dir(machine: str, plot_dir: str, min_size: int) -> List[Dict]:
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
                    if file_size < min_size:#                        logging.info(f'Skipping small plot {full_path} ({file_size / (1 << 30):.2f} GiB)')
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
    return []# Collect old plots
old_plots: List[Dict] = []
min_size = 90 * (1 << 30)  # 90 GiB in bytes# First, get all plot directories sequentially (quick operation)
dir_pairs = []
for machine in machines:
    plot_dirs = get_plot_dirs(machine)
    logging.info(f"Plot directories on {machine}: {', '.join(plot_dirs)}")
    for plot_dir in plot_dirs:
        dir_pairs.append((machine, plot_dir))# Scan directories in parallel
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    future_to_dir = {executor.submit(scan_plot_dir, m, d, min_size): (m, d) for m, d in dir_pairs}
    for future in concurrent.futures.as_completed(future_to_dir):
        old_plots.extend(future.result())logging.info(f'Found {len(old_plots)} old plots to replace.')# Plotting parameters
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
]generation_lock = threading.Lock()
lock = threading.Lock()
pending: Dict[Tuple[str, str], int] = {}# Pre-generate up to 32 compressed plots
with generation_lock:
    existing_plots = [
        f for f in os.listdir(final_dir)
        if f.startswith('plot-k32-') and f.endswith('.plot')
        and os.path.getsize(os.path.join(final_dir, f)) < min_size
    ]
    num_compressed = len(existing_plots)
    if num_compressed < 32:
        count = 32 - num_compressed
        temp_command = plot_command.copy()
        count_idx = temp_command.index('--count')
        temp_command[count_idx + 1] = str(count)
        logging.info(f'Generating {count} additional compressed plots to reach 32.')
        try:
            subprocess.check_call(temp_command)
        except Exception as e:
            logging.error(f'Error pre-generating plots: {e}')def transfer_func(old: Dict, tmp_file: str, new_plot_file: str, deleted: bool, mkey: Tuple[str, str], full_tmp: str, new_size: int):
    try:
        logging.info(f'Copying {tmp_file} to {old["machine"]}:{old["dir"]}')
        subprocess.check_call(['scp', full_tmp, f'{old["machine"]}:{old["dir"]}/'])    logging.info(f'Renaming {tmp_file} to {new_plot_file} on {old["machine"]}')
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
    # Do not delete local file on failure# Group old plots by machine
old_by_machine: Dict[str, List[Dict]] = {}
for old in old_plots:
    mach = old['machine']
    old_by_machine.setdefault(mach, []).append(old)def process_machine(mach: str, olds: List[Dict]):
    for old in olds:
        logging.info(f'Starting replacement for {old["fullpath"]} on {old["machine"]}')    with generation_lock:
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
    transfer_func(old, tmp_file, new_plot_file, deleted, mkey, full_tmp, new_size)with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = []
    for mach, olds in old_by_machine.items():
        futures.append(executor.submit(process_machine, mach, olds))concurrent.futures.wait(futures)

Thought for 56s

python

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
                    # Skip compressed plots
                    if file_size < min_size:
#                        logging.info(f'Skipping small plot {full_path} ({file_size / (1 << 30):.2f} GiB)')
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

# Pre-generate up to 32 compressed plots
with generation_lock:
    existing_plots = [
        f for f in os.listdir(final_dir)
        if f.startswith('plot-k32-') and f.endswith('.plot')
        and os.path.getsize(os.path.join(final_dir, f)) < min_size
    ]
    num_compressed = len(existing_plots)
    if num_compressed < 32:
        count = 32 - num_compressed
        temp_command = plot_command.copy()
        count_idx = temp_command.index('--count')
        temp_command[count_idx + 1] = str(count)
        logging.info(f'Generating {count} additional compressed plots to reach 32.')
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

9 web pages

This is my farm before compressing the plots:Farming status: Farming
Total chia farmed: 67.878699253013
User transaction fees: 0.003699253013
Block rewards: 67.875
Last height farmed: 7510144
Local Harvester
   1705 plots of size: 168.751 TiB on-disk, 168.835 TiBe (effective)
Remote Harvester for IP: 192.168.1.192
   1378 plots of size: 136.390 TiB on-disk, 136.454 TiBe (effective)
Remote Harvester for IP: 192.168.1.188
   1439 plots of size: 142.424 TiB on-disk, 142.495 TiBe (effective)
Plot count for all harvesters: 4522
Total size of plots: 447.566 TiB, 447.784 TiBe (effective)
Estimated network space: 13.162 EiB
Expected time to win: 1 weekAfter compression what will the statistics be?

Thought for 1m 49s

Farming status: Farming
Total chia farmed: 67.878699253013
User transaction fees: 0.003699253013
Block rewards: 67.875
Last height farmed: 7510144
Local Harvester
   1705 plots of size: 135.31 TiB on-disk, 168.835 TiBe (effective)
Remote Harvester for IP: 192.168.1.192
   1378 plots of size: 109.37 TiB on-disk, 136.454 TiBe (effective)
Remote Harvester for IP: 192.168.1.188
   1439 plots of size: 114.21 TiB on-disk, 142.495 TiBe (effective)
Plot count for all harvesters: 4522
Total size of plots: 358.89 TiB, 447.784 TiBe (effective)
Estimated network space: 13.162 EiB
Expected time to win: 1 week

17 web pages

Update the script to fill the filesystems to capacity with compressed plots so that the end state is only C5 compressed plots and filesystems with less than 85GB free.  Do not think of compressed plots as 1-1 replacements for uncompressed plots.  Only delete uncompressed plots if space is needed for the compressed plot.

Thought for 1m 41s

python

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
                    # Skip compressed plots
                    if file_size < min_size:
#                        logging.info(f'Skipping small plot {full_path} ({file_size / (1 << 30):.2f} GiB)')
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

# Pre-generate up to 32 compressed plots
with generation_lock:
    existing_plots = [
        f for f in os.listdir(final_dir)
        if f.startswith('plot-k32-') and f.endswith('.plot')
        and os.path.getsize(os.path.join(final_dir, f)) < min_size
    ]
    num_compressed = len(existing_plots)
    if num_compressed < 32:
        count = 32 - num_compressed
        temp_command = plot_command.copy()
        count_idx = temp_command.index('--count')
        temp_command[count_idx + 1] = str(count)
        logging.info(f'Generating {count} additional compressed plots to reach 32.')
        try:
            subprocess.check_call(temp_command)
        except Exception as e:
            logging.error(f'Error pre-generating plots: {e}')

def transfer_func(machine: str, plot_dir: str, tmp_file: str, new_plot_file: str, full_tmp: str, new_size: int, mkey: Tuple[str, str], old_fullpath: str = None):
    try:
        logging.info(f'Copying {tmp_file} to {machine}:{plot_dir}')
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

    except Exception as e:
        logging.error(f'Error during transfer to {machine}:{plot_dir}: {e}')
        subprocess.call(['ssh', machine, f'rm "{plot_dir}/{tmp_file}" 2>/dev/null'])
        # Do not delete local file on failure

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

            if effective_free < stop_threshold:
                logging.info(f'Effective free space {effective_free / (1 << 30):.2f} GiB < 85 GiB, stopping for {mach}:{plot_dir}')
                break

            # Get a new compressed plot
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

            # Recheck effective free (in case pend changed)
            with lock:
                pend = pending.get(mkey, 0)
                effective_free = free_space - pend

            old_fullpath = None
            if effective_free < new_size + buffer_space:
                if not remaining_olds:
                    logging.info(f'Insufficient space and no more uncompressed plots to delete for {mach}:{plot_dir}')
                    # Put back the plot
                    continue  # or break, but since we got it, but to put back, but since renamed not yet
                    # No rename yet, so just continue
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
                    continue

                # Re-get free space after delete
                try:
                    free_output = subprocess.check_output(
                        ['ssh', mach, f'df -B1 --output=avail "{plot_dir}" | tail -1']
                    ).decode().strip()
                    free_space = int(free_output)
                except Exception as e:
                    logging.error(f'Error getting free space after delete: {e}')
                    continue

                with lock:
                    pend = pending.get(mkey, 0)
                    effective_free = free_space - pend

                if effective_free < new_size + buffer_space:
                    logging.error(f'Still insufficient space after delete for {mach}:{plot_dir}')
                    continue

            # Rename new plot to .tmp locally
            tmp_file = new_plot_file + '.tmp'
            full_tmp = os.path.join(final_dir, tmp_file)
            os.rename(full_new, full_tmp)

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

Quick Answer

