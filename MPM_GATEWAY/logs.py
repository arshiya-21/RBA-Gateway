import os
import datetime
import re

# --- CONFIG: Add your folders here ---
LOG_FOLDERS = [
    r"/home/manoj/Desktop/MPM_GATEWAY/RBA_Additive_data_transfer/logs",
    r"/home/manoj/Desktop/MPM_GATEWAY/RBA_lab_compact_data_transfer/logs",
    r"/home/manoj/Desktop/MPM_GATEWAY/RBA_lab_permeability_data_transfer/logs",
    r"/home/manoj/Desktop/MPM_GATEWAY/RBA_lab_shear_strength/logs",
    r"/home/manoj/Desktop/MPM_GATEWAY/RBA_lab_specimen_height/logs",
    r"/home/manoj/Desktop/MPM_GATEWAY/RBA_lab_strength_data_transfer/logs",
    r"/home/manoj/Desktop/MPM_GATEWAY/RBA_lab_wts_ws_data_transfer/logs",
    r"/home/manoj/Desktop/MPM_GATEWAY/RBA_mould_data/logs",
    r"/home/manoj/Desktop/MPM_GATEWAY/RBA_scada_plc_data_transfer/logs"
]

# Regex to extract date from file name (YYYY-MM-DD.log)
DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})\.log$")

DAYS_TO_KEEP = 7


def clean_old_logs():
    today = datetime.date.today()
    min_allowed_date = today - datetime.timedelta(days=DAYS_TO_KEEP)

    print(f"[INFO] Keeping logs from {min_allowed_date} to {today}")

    for folder in LOG_FOLDERS:
        if not os.path.exists(folder):
            print(f"[WARN] Folder not found: {folder}")
            continue

        print(f"[INFO] Checking folder: {folder}")

        for file in os.listdir(folder):
            file_path = os.path.join(folder, file)

            if not os.path.isfile(file_path):
                continue

            match = DATE_PATTERN.search(file)
            if not match:
                continue

            try:
                file_date = datetime.datetime.strptime(
                    match.group(1), "%Y-%m-%d"
                ).date()
            except ValueError:
                continue

            if file_date < min_allowed_date:
                try:
                    os.remove(file_path)
                    print(f"[DELETE] {file_path}")
                except Exception as e:
                    print(f"[ERROR] Failed to delete {file_path}: {e}")

        print(f"[DONE] Cleanup completed for: {folder}")


if __name__ == "__main__":
    clean_old_logs()
