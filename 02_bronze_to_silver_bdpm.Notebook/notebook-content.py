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

from pyspark.sql import functions as F

BRONZE_BASE_PATH = "Files/bronze_bdpm"

def read_bdpm_txt(file_name, columns):
    path = f"{BRONZE_BASE_PATH}/{file_name}"
    n = len(columns)

    df_raw = (
        spark.read.text(path)
        .filter(F.col("value").isNotNull())
        .filter(F.trim(F.col("value")) != "")
    )

    cleaned_value = F.regexp_replace(F.col("value"), "^\ufeff", "")
    parts = F.split(cleaned_value, "\t", n)

    return df_raw.select(
        *[
            F.when(F.size(parts) > i, F.trim(parts.getItem(i)))
             .otherwise(F.lit(None))
             .alias(columns[i])
            for i in range(n)
        ]
    )

# =========================
# CIS
# =========================
cis_cols = [
    "CIS","Denomination","FormePharmaceutique","VoiesAdministration",
    "StatutAMM","TypeProcedureAMM","EtatCommercialisation",
    "DateAMM","StatutBDM","NumeroAutorisationEuropeenne",
    "Titulaire","SurveillanceRenforcee"
]

silver_cis = read_bdpm_txt("CIS_bdpm.txt", cis_cols) \
    .withColumn("DateAMM", F.to_date("DateAMM", "dd/MM/yyyy")) \
    .dropDuplicates(["CIS"])

silver_cis.write.mode("overwrite").format("delta").saveAsTable("dbo.silver_cis")

# =========================
# CIP
# =========================
cip_cols = [
    "CIS","CIP7","LibellePresentation","StatutAdministratifPresentation",
    "EtatCommercialisationPresentation","DateDeclarationCommercialisation",
    "CIP13","AgrementCollectivites","TauxRemboursement",
    "PrixMedicament","IndicationsRemboursement"
]

silver_cip = read_bdpm_txt("CIS_CIP_bdpm.txt", cip_cols) \
    .withColumn("DateDeclarationCommercialisation", F.to_date("DateDeclarationCommercialisation", "dd/MM/yyyy")) \
    .withColumn("PrixMedicament", F.regexp_replace("PrixMedicament", ",", ".")) \
    .withColumn("PrixMedicament", F.regexp_extract("PrixMedicament", r"([0-9]+(?:\.[0-9]+)?)", 1).cast("double")) \
    .withColumn("TauxRemboursement", F.regexp_extract("TauxRemboursement", r"(\d+)", 1).cast("int"))

silver_cip.write.mode("overwrite").format("delta").saveAsTable("dbo.silver_cip")

# =========================
# COMPOSITION
# =========================
compo_cols = [
    "CIS","DesignationElement","CodeSubstance","DenominationSubstance",
    "Dosage","ReferenceDosage","NatureComposant","NumeroLiaison"
]

silver_compo = read_bdpm_txt("CIS_COMPO_bdpm.txt", compo_cols)
silver_compo.write.mode("overwrite").format("delta").saveAsTable("dbo.silver_compo")

# =========================
# SMR
# =========================
smr_cols = [
    "CIS","CodeDossierHAS","MotifEvaluation","DateAvis",
    "ValeurSMR","LibelleSMR"
]

silver_smr = read_bdpm_txt("CIS_HAS_SMR_bdpm.txt", smr_cols) \
    .withColumn("DateAvis", F.to_date("DateAvis", "dd/MM/yyyy"))

silver_smr.write.mode("overwrite").format("delta").saveAsTable("dbo.silver_smr")

# =========================
# ASMR
# =========================
asmr_cols = [
    "CIS","CodeDossierHAS","MotifEvaluation","DateAvis",
    "ValeurASMR","LibelleASMR"
]

silver_asmr = read_bdpm_txt("CIS_HAS_ASMR_bdpm.txt", asmr_cols) \
    .withColumn("DateAvis", F.to_date("DateAvis", "dd/MM/yyyy"))

silver_asmr.write.mode("overwrite").format("delta").saveAsTable("dbo.silver_asmr")

# =========================
# LIENS PAGE CT
# =========================
liens_cols = ["CodeDossierHAS", "LienPageCT"]

silver_liens = read_bdpm_txt("HAS_LiensPageCT_bdpm.txt", liens_cols)
silver_liens.write.mode("overwrite").format("delta").saveAsTable("dbo.silver_liens_ct")

# =========================
# GENER
# =========================
gener_cols = [
    "IdentifiantGroupe","LibelleGroupe","CIS",
    "TypeGenerique","NumeroTri"
]

silver_gener = read_bdpm_txt("CIS_GENER_bdpm.txt", gener_cols)
silver_gener.write.mode("overwrite").format("delta").saveAsTable("dbo.silver_gener")

# =========================
# CPD
# =========================
cpd_cols = ["CIS","ConditionPrescription"]

silver_cpd = read_bdpm_txt("CIS_CPD_bdpm.txt", cpd_cols)
silver_cpd.write.mode("overwrite").format("delta").saveAsTable("dbo.silver_cpd")

# =========================
# MITM
# =========================
mitm_cols = ["CIS","StatutMITM"]

silver_mitm = read_bdpm_txt("CIS_MITM.txt", mitm_cols)
silver_mitm.write.mode("overwrite").format("delta").saveAsTable("dbo.silver_mitm")

# =========================
# DISPO
# =========================
dispo_cols = [
    "CIS","CIP7","CodeStatut","DateDebut","DateMiseAJour",
    "DateRemiseDispo","LienANSM"
]

silver_dispo = read_bdpm_txt("CIS_CIP_Dispo_Spec.txt", dispo_cols) \
    .withColumn("DateDebut", F.to_date("DateDebut", "dd/MM/yyyy")) \
    .withColumn("DateMiseAJour", F.to_date("DateMiseAJour", "dd/MM/yyyy")) \
    .withColumn("DateRemiseDispo", F.to_date("DateRemiseDispo", "dd/MM/yyyy"))

silver_dispo.write.mode("overwrite").format("delta").saveAsTable("dbo.silver_dispo")

print("Tables Silver créées avec succès")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
