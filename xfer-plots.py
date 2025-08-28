import os
import time
import subprocess
import threading
import yaml
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

TMP_DIR = '/var/TmpChia'
HOSTS = ['neo', 'omni', 'archon']
GB = 1024 ** 3
UNCOMPRESSED_THRESHOLD = 90 * GB
COMPRESSED_MAX = 85 * GB

def get_plot_dirs(host):
    logging.debug(f"Fetching plot directories for host: {host}")
    try:
        config_text = subprocess.check_output(['ssh', host, 'cat /home/phil/.chia/mainnet/config/config.yaml']).decode()
        conf = yaml.safe_load(config_text)
        dirs = conf['harvester']['plot_directories']
        logging.info(f"Plot directories for {host}: {dirs}")
        return dirs
    except Exception as e:
        logging.error(f"Error fetching plot directories for {host}: {e}")
        return []

def get_free_space(host, directory):
    logging.debug(f"Checking free space on {host}:{directory}")
    cmd = ['ssh', host, f'df -B1 "{directory}" | tail -1 | awk \'{{print $4}}\'']
    try:
        out = subprocess.check_output(cmd).decode().strip()
        free = int(out)
        logging.debug(f"Free space on {host}:{directory}: {free} bytes")
        return free
    except Exception as e:
        logging.error(f"Error getting free space on {host}:{directory}: {e}")
        return 0

def find_suitable_dir(host, needed_space, active_dirs):
    logging.debug(f"Finding suitable directory on {host} needing {needed_space} bytes")
    dirs_to_remove = []
    for d in active_dirs[host]:
        free = get_free_space(host, d)
        if free >= needed_space:
            logging.info(f"Suitable directory found on {host}: {d} with {free} bytes free")
            return d
        else:
            # Check for uncompressed plot to delete
            logging.debug(f"Checking for uncompressed plot to delete in {host}:{d}")
            cmd_find_unc = ['ssh', host, f'find "{d}" -maxdepth 1 -name "*.plot" -size +90G -print -quit']
            try:
                unc_plot = subprocess.check_output(cmd_find_unc).decode().strip()
                if unc_plot:
                    logging.info(f"Deleting uncompressed plot {unc_plot} on {host}:{d}")
                    subprocess.check_call(['ssh', host, f'rm "{unc_plot}"'])
                    free = get_free_space(host, d)
                    if free >= needed_space:
                        logging.info(f"Space freed, now suitable: {d} with {free} bytes free")
                        return d
                else:
                    logging.debug(f"No uncompressed plots found in {host}:{d}")
                    # No uncompressed, full with compressed, mark for removal
                    if free < needed_space:
                        logging.warning(f"Directory {d} on {host} is full with compressed plots, removing from active list")
                        dirs_to_remove.append(d)
            except subprocess.CalledProcessError as e:
                logging.error(f"Error finding/deleting uncompressed plot on {host}:{d}: {e}")
    # Remove full directories after checking all
    for d in dirs_to_remove:
        active_dirs[host].remove(d)
    logging.debug(f"No suitable directory found on {host}")
    return None

def transfer(plot, host, target_dir, host_busy):
    logging.info(f"Starting transfer of {plot} to {host}:{target_dir}")
    local_path = os.path.join(TMP_DIR, plot)
    tmp_name = plot + '.tmp'
    local_tmp = os.path.join(TMP_DIR, tmp_name)
    remote_tmp = os.path.join(target_dir, tmp_name)
    remote_final = os.path.join(target_dir, plot)
    try:
        logging.debug(f"Renaming local file to tmp: {local_path} -> {local_tmp}")
        os.rename(local_path, local_tmp)
        logging.debug(f"SCP transferring {local_tmp} to {host}:{remote_tmp}")
        subprocess.check_call(['scp', local_tmp, f'{host}:{remote_tmp}'])
        logging.debug(f"Renaming remote file: {remote_tmp} -> {remote_final}")
        subprocess.check_call(['ssh', host, f'mv "{remote_tmp}" "{remote_final}"'])
        logging.debug(f"Removing local tmp file: {local_tmp}")
        os.remove(local_tmp)
        logging.info(f"Transfer completed successfully for {plot} to {host}:{target_dir}")
    except Exception as e:
        logging.error(f"Transfer error for {plot} to {host}:{target_dir}: {e}")
        if os.path.exists(local_tmp):
            try:
                logging.debug(f"Renaming back failed transfer: {local_tmp} -> {local_path}")
                os.rename(local_tmp, local_path)
            except Exception as rename_e:
                logging.error(f"Failed to rename back {local_tmp}: {rename_e}")
    finally:
        host_busy[host] = False
        logging.debug(f"Host {host} is now available")

def main():
    logging.info("Starting the Chia plot transfer script")
    host_busy = {h: False for h in HOSTS}
    active_dirs = {h: get_plot_dirs(h) for h in HOSTS}
    
    while True:
        logging.debug(f"Scanning for plots in {TMP_DIR}")
        plots = [f for f in os.listdir(TMP_DIR) if f.endswith('.plot')]
        if not plots:
            logging.debug("No plots found, sleeping for 10 seconds")
            time.sleep(10)
            continue
        
        # Sort plots by modification time (oldest first)
        plots.sort(key=lambda f: os.path.getmtime(os.path.join(TMP_DIR, f)))
        logging.debug(f"Found {len(plots)} plots, processing oldest: {plots[0]}")
        
        # Try to assign one plot per iteration to an available host
        plot = plots[0]
        local_path = os.path.join(TMP_DIR, plot)
        needed_space = os.path.getsize(local_path)
        
        if needed_space >= COMPRESSED_MAX:
            logging.warning(f"Skipping large plot {plot} (>85GB, size: {needed_space} bytes)")
            time.sleep(10)
            continue
        
        assigned = False
        for host in HOSTS:
            if not host_busy[host] and active_dirs[host]:
                logging.debug(f"Checking host {host} for availability")
                suitable_dir = find_suitable_dir(host, needed_space, active_dirs)
                if suitable_dir:
                    host_busy[host] = True
                    logging.info(f"Assigning transfer to thread for {host}")
                    thread = threading.Thread(target=transfer, args=(plot, host, suitable_dir, host_busy))
                    thread.start()
                    assigned = True
                    break
        
        if not assigned:
            logging.debug("No available host found, sleeping for 10 seconds")
            time.sleep(10)

if __name__ == '__main__':
    main()