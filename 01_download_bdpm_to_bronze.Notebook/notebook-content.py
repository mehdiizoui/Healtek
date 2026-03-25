# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "06c01244-5bc6-4d47-aa99-c6c422118521",
# META       "default_lakehouse_name": "LH_Pharma",
# META       "default_lakehouse_workspace_id": "45867f3a-45d6-429e-ad15-b6025ac19b91",
# META       "known_lakehouses": [
# META         {
# META           "id": "06c01244-5bc6-4d47-aa99-c6c422118521"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Microsoft Fabric / PySpark Notebook
# 01 - Download BDPM raw files to Bronze
# Source: https://base-donnees-publique.medicaments.gouv.fr/
# Strategy: urllib with Mozilla/5.0 User-Agent

import os
import time
import urllib.request
from urllib.error import HTTPError, URLError

BASE = "https://base-donnees-publique.medicaments.gouv.fr/download/file/"
FICHIERS = [
    "CIS_bdpm.txt",
    "CIS_CIP_bdpm.txt",
    "CIS_COMPO_bdpm.txt",
    "CIS_HAS_SMR_bdpm.txt",
    "CIS_HAS_ASMR_bdpm.txt",
    "HAS_LiensPageCT_bdpm.txt",
    "CIS_GENER_bdpm.txt",
    "CIS_CPD_bdpm.txt",
    "CIS_MITM.txt",
    "CIS_CIP_Dispo_Spec.txt",
]

# Adjust these Lakehouse paths in Fabric if needed
LOCAL_TMP_DIR = "/tmp/bdpm_raw"
BRONZE_BASE_PATH = "Files/bdpm/bronze/raw"

os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

opener = urllib.request.build_opener()
opener.addheaders = [("User-Agent", "Mozilla/5.0")]
urllib.request.install_opener(opener)

for nom in FICHIERS:
    src = BASE + nom
    local_dest = os.path.join(LOCAL_TMP_DIR, nom)
    print(f"[DL] {nom} ...")
    try:
        urllib.request.urlretrieve(src, local_dest)
        print(f"[OK] Downloaded {nom} ({os.path.getsize(local_dest)//1024} KB)")
    except (HTTPError, URLError) as e:
        print(f"[ERROR] {nom} -> {e}")
        raise
    time.sleep(0.5)

# Copy raw files into Lakehouse bronze area
for nom in FICHIERS:
    src_file = f"file:{os.path.join(LOCAL_TMP_DIR, nom)}"
    dest_dir = f"{BRONZE_BASE_PATH}/{nom.replace('.txt','')}"
    mssparkutils.fs.mkdirs(dest_dir)
    mssparkutils.fs.cp(src_file, dest_dir, True)
    print(f"[COPY] {nom} -> {dest_dir}")

print("Bronze raw download completed.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
