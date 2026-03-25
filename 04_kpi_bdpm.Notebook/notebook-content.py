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

gold = spark.table("dbo.gold_bdpm_cis")

# =========================
# KPI GLOBAL
# =========================
kpi_global = gold.agg(
    F.countDistinct("CIS").cast("double").alias("KPI_01_NbMedicaments"),
    F.sum(F.coalesce(F.col("FlagCommercialise"), F.lit(0))).cast("double").alias("KPI_02_NbMedicamentsCommercialises"),
    F.sum(F.coalesce(F.col("FlagRemboursable"), F.lit(0))).cast("double").alias("KPI_03_NbMedicamentsRemboursables"),
    F.sum(F.coalesce(F.col("FlagSurveillanceRenforcee"), F.lit(0))).cast("double").alias("KPI_04_NbSousSurveillanceRenforcee"),
    F.sum(F.coalesce(F.col("FlagMITM"), F.lit(0))).cast("double").alias("KPI_05_NbMITM"),
    F.sum(F.coalesce(F.col("FlagAlerteDispo"), F.lit(0))).cast("double").alias("KPI_06_NbMedicamentsAvecAlerteDispo"),
    F.avg("PrixMoyen").cast("double").alias("KPI_07_PrixMoyenGlobal"),
    F.max("PrixMax").cast("double").alias("KPI_08_PrixMaximumObserve"),
    F.avg("NbPresentations").cast("double").alias("KPI_09_NbPresentationsMoyen"),
    F.avg("NbSubstancesDistinctes").cast("double").alias("KPI_10_NbSubstancesMoyen"),
    F.sum(F.coalesce(F.col("EstDansGroupeGenerique"), F.lit(0))).cast("double").alias("KPI_11_NbMedicamentsDansUnGroupeGenerique"),
    F.sum(F.when(F.col("FlagEvaluationHAS") == 1, 1).otherwise(0)).cast("double").alias("KPI_12_NbMedicamentsAvecEvaluationHAS"),
    F.sum(F.when(F.col("SMR_DateAvis_Recent").isNotNull(), 1).otherwise(0)).cast("double").alias("KPI_13_NbMedicamentsAvecSMR"),
    F.sum(F.when(F.col("ASMR_DateAvis_Recent").isNotNull(), 1).otherwise(0)).cast("double").alias("KPI_14_NbMedicamentsAvecASMR"),
    F.avg("DureeMoyenneIndispoJours").cast("double").alias("KPI_15_DureeMoyenneIndispoJours")
)

kpi_global = (
    kpi_global
    .withColumn("KPI_16_TauxMedicamentsCommercialises",
                F.col("KPI_02_NbMedicamentsCommercialises") / F.col("KPI_01_NbMedicaments"))
    .withColumn("KPI_17_TauxMedicamentsRemboursables",
                F.col("KPI_03_NbMedicamentsRemboursables") / F.col("KPI_01_NbMedicaments"))
    .withColumn("KPI_18_TauxMedicamentsAvecAlerteDispo",
                F.col("KPI_06_NbMedicamentsAvecAlerteDispo") / F.col("KPI_01_NbMedicaments"))
    .withColumn("KPI_19_TauxMedicamentsAvecEvaluationHAS",
                F.col("KPI_12_NbMedicamentsAvecEvaluationHAS") / F.col("KPI_01_NbMedicaments"))
    .withColumn("KPI_20_TauxMedicamentsDansUnGroupeGenerique",
                F.col("KPI_11_NbMedicamentsDansUnGroupeGenerique") / F.col("KPI_01_NbMedicaments"))
)

# =========================
# DROP puis CREATE
# =========================
spark.sql("DROP TABLE IF EXISTS dbo.kpi_bdpm_global")
spark.sql("DROP TABLE IF EXISTS dbo.kpi_bdpm_long")

kpi_global.write.format("delta").saveAsTable("dbo.kpi_bdpm_global")

# =========================
# KPI LONG
# =========================
kpi_long = kpi_global.selectExpr("""
stack(20,
'Nb médicaments', cast(KPI_01_NbMedicaments as double), 'volume',
'Nb médicaments commercialisés', cast(KPI_02_NbMedicamentsCommercialises as double), 'volume',
'Nb médicaments remboursables', cast(KPI_03_NbMedicamentsRemboursables as double), 'volume',
'Nb sous surveillance renforcée', cast(KPI_04_NbSousSurveillanceRenforcee as double), 'volume',
'Nb MITM', cast(KPI_05_NbMITM as double), 'volume',
'Nb médicaments avec alerte disponibilité', cast(KPI_06_NbMedicamentsAvecAlerteDispo as double), 'volume',
'Prix moyen global', cast(KPI_07_PrixMoyenGlobal as double), 'prix',
'Prix maximum observé', cast(KPI_08_PrixMaximumObserve as double), 'prix',
'Nb présentations moyen', cast(KPI_09_NbPresentationsMoyen as double), 'moyenne',
'Nb substances moyen', cast(KPI_10_NbSubstancesMoyen as double), 'moyenne',
'Nb médicaments dans un groupe générique', cast(KPI_11_NbMedicamentsDansUnGroupeGenerique as double), 'volume',
'Nb médicaments avec évaluation HAS', cast(KPI_12_NbMedicamentsAvecEvaluationHAS as double), 'volume',
'Nb médicaments avec SMR', cast(KPI_13_NbMedicamentsAvecSMR as double), 'volume',
'Nb médicaments avec ASMR', cast(KPI_14_NbMedicamentsAvecASMR as double), 'volume',
'Durée moyenne indispo jours', cast(KPI_15_DureeMoyenneIndispoJours as double), 'durée',
'Taux médicaments commercialisés', cast(KPI_16_TauxMedicamentsCommercialises as double), 'taux',
'Taux médicaments remboursables', cast(KPI_17_TauxMedicamentsRemboursables as double), 'taux',
'Taux médicaments avec alerte dispo', cast(KPI_18_TauxMedicamentsAvecAlerteDispo as double), 'taux',
'Taux médicaments avec évaluation HAS', cast(KPI_19_TauxMedicamentsAvecEvaluationHAS as double), 'taux',
'Taux médicaments dans groupe générique', cast(KPI_20_TauxMedicamentsDansUnGroupeGenerique as double), 'taux'
) as (NomKPI, ValeurKPI, TypeKPI)
""")

kpi_long.write.format("delta").saveAsTable("dbo.kpi_bdpm_long")

print("Tables KPI créées avec succès")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
# =========================================================
# NOTEBOOK COMPLET : DIMENSIONS + FACT + STORYTELLING
# Source : dbo.gold_bdpm_cis_enriched
# =========================================================

gold = spark.table("dbo.gold_bdpm_cis_enriched")

# =========================================================
# 0. VERIFICATION RAPIDE
# =========================================================
print("Schema de la table enrichie :")
gold.printSchema()

print("Nombre de lignes :", gold.count())

# =========================================================
# 1. DIMENSIONS ANALYTIQUES
# =========================================================

# -------------------------
# Dimension forme pharmaceutique
# -------------------------
dim_forme = (
    gold.select(F.trim(F.col("FormePharmaceutique")).alias("FormePharmaceutique"))
        .filter(F.col("FormePharmaceutique").isNotNull())
        .filter(F.col("FormePharmaceutique") != "")
        .dropDuplicates()
        .orderBy("FormePharmaceutique")
        .withColumn("FormeKey", F.row_number().over(Window.orderBy("FormePharmaceutique")))
        .select("FormeKey", "FormePharmaceutique")
)

# -------------------------
# Dimension titulaire
# -------------------------
dim_titulaire = (
    gold.select(F.trim(F.col("Titulaire")).alias("Titulaire"))
        .filter(F.col("Titulaire").isNotNull())
        .filter(F.col("Titulaire") != "")
        .dropDuplicates()
        .orderBy("Titulaire")
        .withColumn("TitulaireKey", F.row_number().over(Window.orderBy("Titulaire")))
        .select("TitulaireKey", "Titulaire")
)

# -------------------------
# Dimension innovation
# -------------------------
dim_innovation = (
    gold.select(F.trim(F.col("InnovationLevel")).alias("InnovationLevel"))
        .filter(F.col("InnovationLevel").isNotNull())
        .filter(F.col("InnovationLevel") != "")
        .dropDuplicates()
        .orderBy("InnovationLevel")
        .withColumn("InnovationKey", F.row_number().over(Window.orderBy("InnovationLevel")))
        .select("InnovationKey", "InnovationLevel")
)

# -------------------------
# Dimension risque sanitaire
# -------------------------
dim_risque = (
    gold.select(F.trim(F.col("RisqueSanitaire")).alias("RisqueSanitaire"))
        .filter(F.col("RisqueSanitaire").isNotNull())
        .filter(F.col("RisqueSanitaire") != "")
        .dropDuplicates()
        .orderBy("RisqueSanitaire")
        .withColumn("RisqueKey", F.row_number().over(Window.orderBy("RisqueSanitaire")))
        .select("RisqueKey", "RisqueSanitaire")
)

# -------------------------
# Dimension remboursement
# -------------------------
dim_remboursement = (
    gold.select(F.trim(F.col("RemboursementCategory")).alias("RemboursementCategory"))
        .filter(F.col("RemboursementCategory").isNotNull())
        .filter(F.col("RemboursementCategory") != "")
        .dropDuplicates()
        .orderBy("RemboursementCategory")
        .withColumn("RemboursementKey", F.row_number().over(Window.orderBy("RemboursementCategory")))
        .select("RemboursementKey", "RemboursementCategory")
)

# -------------------------
# Dimension range de prix
# -------------------------
dim_prix_range = (
    gold.select(F.trim(F.col("PrixRange")).alias("PrixRange"))
        .filter(F.col("PrixRange").isNotNull())
        .filter(F.col("PrixRange") != "")
        .dropDuplicates()
        .orderBy("PrixRange")
        .withColumn("PrixRangeKey", F.row_number().over(Window.orderBy("PrixRange")))
        .select("PrixRangeKey", "PrixRange")
)

# -------------------------
# Dimension ancienneté AMM
# -------------------------
dim_anciennete = (
    gold.select(F.trim(F.col("AncienneteAMM")).alias("AncienneteAMM"))
        .filter(F.col("AncienneteAMM").isNotNull())
        .filter(F.col("AncienneteAMM") != "")
        .dropDuplicates()
        .orderBy("AncienneteAMM")
        .withColumn("AncienneteKey", F.row_number().over(Window.orderBy("AncienneteAMM")))
        .select("AncienneteKey", "AncienneteAMM")
)

# -------------------------
# Dimension complexité
# -------------------------
dim_complexite = (
    gold.select(F.trim(F.col("Complexite")).alias("Complexite"))
        .filter(F.col("Complexite").isNotNull())
        .filter(F.col("Complexite") != "")
        .dropDuplicates()
        .orderBy("Complexite")
        .withColumn("ComplexiteKey", F.row_number().over(Window.orderBy("Complexite")))
        .select("ComplexiteKey", "Complexite")
)

# -------------------------
# Dimension niveau SMR
# -------------------------
dim_smr_level = (
    gold.select(F.trim(F.col("SMR_Level")).alias("SMR_Level"))
        .filter(F.col("SMR_Level").isNotNull())
        .filter(F.col("SMR_Level") != "")
        .dropDuplicates()
        .orderBy("SMR_Level")
        .withColumn("SMRLevelKey", F.row_number().over(Window.orderBy("SMR_Level")))
        .select("SMRLevelKey", "SMR_Level")
)

# =========================================================
# 2. ECRITURE DES DIMENSIONS
# =========================================================

from pyspark.sql.window import Window

tables_to_drop = [
    "dbo.dim_forme_pharmaceutique",
    "dbo.dim_titulaire",
    "dbo.dim_innovation",
    "dbo.dim_risque_sanitaire",
    "dbo.dim_remboursement",
    "dbo.dim_prix_range",
    "dbo.dim_anciennete_amm",
    "dbo.dim_complexite",
    "dbo.dim_smr_level"
]

for t in tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

dim_forme.write.format("delta").saveAsTable("dbo.dim_forme_pharmaceutique")
dim_titulaire.write.format("delta").saveAsTable("dbo.dim_titulaire")
dim_innovation.write.format("delta").saveAsTable("dbo.dim_innovation")
dim_risque.write.format("delta").saveAsTable("dbo.dim_risque_sanitaire")
dim_remboursement.write.format("delta").saveAsTable("dbo.dim_remboursement")
dim_prix_range.write.format("delta").saveAsTable("dbo.dim_prix_range")
dim_anciennete.write.format("delta").saveAsTable("dbo.dim_anciennete_amm")
dim_complexite.write.format("delta").saveAsTable("dbo.dim_complexite")
dim_smr_level.write.format("delta").saveAsTable("dbo.dim_smr_level")

print("Dimensions créées avec succès")

# =========================================================
# 3. RELECTURE DES DIMENSIONS
# =========================================================

dim_forme = spark.table("dbo.dim_forme_pharmaceutique")
dim_titulaire = spark.table("dbo.dim_titulaire")
dim_innovation = spark.table("dbo.dim_innovation")
dim_risque = spark.table("dbo.dim_risque_sanitaire")
dim_remboursement = spark.table("dbo.dim_remboursement")
dim_prix_range = spark.table("dbo.dim_prix_range")
dim_anciennete = spark.table("dbo.dim_anciennete_amm")
dim_complexite = spark.table("dbo.dim_complexite")
dim_smr_level = spark.table("dbo.dim_smr_level")

# =========================================================
# 4. FACT TABLE POUR POWER BI
# =========================================================

fact_bdpm = (
    gold
    .join(dim_forme, on="FormePharmaceutique", how="left")
    .join(dim_titulaire, on="Titulaire", how="left")
    .join(dim_innovation, on="InnovationLevel", how="left")
    .join(dim_risque, on="RisqueSanitaire", how="left")
    .join(dim_remboursement, on="RemboursementCategory", how="left")
    .join(dim_prix_range, on="PrixRange", how="left")
    .join(dim_anciennete, on="AncienneteAMM", how="left")
    .join(dim_complexite, on="Complexite", how="left")
    .join(dim_smr_level, on="SMR_Level", how="left")
    .select(
        "CIS",
        "Denomination",
        "DateAMM",
        "GoldEnrichmentLoadDate",
        "FormeKey",
        "TitulaireKey",
        "InnovationKey",
        "RisqueKey",
        "RemboursementKey",
        "PrixRangeKey",
        "AncienneteKey",
        "ComplexiteKey",
        "SMRLevelKey",
        "PrixMin",
        "PrixMax",
        "PrixMoyen",
        "TauxRemboursementMax",
        "NbPresentations",
        "NbPresentationsCommercialisees",
        "NbSubstancesDistinctes",
        "NbAvisSMR",
        "NbAvisASMR",
        "DureeMoyenneIndispoJours",
        "FlagMITM",
        "FlagAlerteDispo",
        "FlagRemboursable",
        "FlagCommercialise",
        "FlagEvaluationHAS",
        "FlagInnovationForte",
        "FlagMeToo",
        "FlagPrixEleve",
        "FlagCritiqueGlobal",
        "AgeAMM_Annees"
    )
)

spark.sql("DROP TABLE IF EXISTS dbo.fact_bdpm_cis")
fact_bdpm.write.format("delta").saveAsTable("dbo.fact_bdpm_cis")

print("Fact table dbo.fact_bdpm_cis créée avec succès")

# =========================================================
# 5. TABLES STORYTELLING / INSIGHTS
# =========================================================

# -------------------------
# Top laboratoires
# -------------------------
insight_top_laboratoires = (
    gold.groupBy("Titulaire")
        .agg(
            F.countDistinct("CIS").alias("NbMedicaments"),
            F.round(F.avg("PrixMoyen"), 2).alias("PrixMoyenPortefeuille"),
            F.sum(F.coalesce(F.col("FlagCritiqueGlobal"), F.lit(0))).alias("NbMedicamentsCritiques"),
            F.sum(F.coalesce(F.col("FlagInnovationForte"), F.lit(0))).alias("NbInnovationForte"),
            F.sum(F.coalesce(F.col("FlagMeToo"), F.lit(0))).alias("NbMeToo")
        )
        .orderBy(F.desc("NbMedicaments"))
)

# -------------------------
# Distribution innovation
# -------------------------
insight_innovation = (
    gold.groupBy("InnovationLevel")
        .agg(
            F.countDistinct("CIS").alias("NbMedicaments"),
            F.round(F.avg("PrixMoyen"), 2).alias("PrixMoyen"),
            F.round(F.avg("NbPresentations"), 2).alias("NbPresentationsMoyen"),
            F.round(F.avg("NbSubstancesDistinctes"), 2).alias("NbSubstancesMoyen")
        )
        .orderBy(F.desc("NbMedicaments"))
)

# -------------------------
# Distribution risque
# -------------------------
insight_risque = (
    gold.groupBy("RisqueSanitaire")
        .agg(
            F.countDistinct("CIS").alias("NbMedicaments"),
            F.round(F.avg("PrixMoyen"), 2).alias("PrixMoyen"),
            F.round(F.avg("DureeMoyenneIndispoJours"), 2).alias("DureeMoyenneIndispoJours")
        )
        .orderBy(F.desc("NbMedicaments"))
)

# -------------------------
# Répartition prix
# -------------------------
insight_prix = (
    gold.groupBy("PrixRange")
        .agg(
            F.countDistinct("CIS").alias("NbMedicaments"),
            F.round(F.avg("TauxRemboursementMax"), 2).alias("TauxRemboursementMoyen"),
            F.round(F.avg("NbPresentations"), 2).alias("NbPresentationsMoyen")
        )
        .orderBy("PrixRange")
)

# -------------------------
# Répartition remboursement
# -------------------------
insight_remboursement = (
    gold.groupBy("RemboursementCategory")
        .agg(
            F.countDistinct("CIS").alias("NbMedicaments"),
            F.round(F.avg("PrixMoyen"), 2).alias("PrixMoyen"),
            F.round(F.avg("NbSubstancesDistinctes"), 2).alias("NbSubstancesMoyen")
        )
        .orderBy(F.desc("NbMedicaments"))
)

# -------------------------
# Ancienneté vs innovation
# -------------------------
insight_anciennete_innovation = (
    gold.groupBy("AncienneteAMM", "InnovationLevel")
        .agg(F.countDistinct("CIS").alias("NbMedicaments"))
        .orderBy("AncienneteAMM", "InnovationLevel")
)

# -------------------------
# Complexité vs risque
# -------------------------
insight_complexite_risque = (
    gold.groupBy("Complexite", "RisqueSanitaire")
        .agg(F.countDistinct("CIS").alias("NbMedicaments"))
        .orderBy("Complexite", "RisqueSanitaire")
)

# -------------------------
# Forme pharmaceutique
# -------------------------
insight_forme = (
    gold.groupBy("FormePharmaceutique")
        .agg(
            F.countDistinct("CIS").alias("NbMedicaments"),
            F.round(F.avg("PrixMoyen"), 2).alias("PrixMoyen"),
            F.sum(F.coalesce(F.col("FlagRemboursable"), F.lit(0))).alias("NbRemboursables")
        )
        .orderBy(F.desc("NbMedicaments"))
)

# -------------------------
# Vue executive summary
# -------------------------
insight_executive_summary = gold.agg(
    F.countDistinct("CIS").alias("NbMedicaments"),
    F.round(F.avg("PrixMoyen"), 2).alias("PrixMoyen"),
    F.sum(F.coalesce(F.col("FlagInnovationForte"), F.lit(0))).alias("NbInnovationForte"),
    F.sum(F.coalesce(F.col("FlagMeToo"), F.lit(0))).alias("NbMeToo"),
    F.sum(F.coalesce(F.col("FlagCritiqueGlobal"), F.lit(0))).alias("NbMedicamentsCritiques"),
    F.sum(F.when(F.col("RemboursementCategory") == "Fort", 1).otherwise(0)).alias("NbRemboursementFort"),
    F.sum(F.coalesce(F.col("FlagPrixEleve"), F.lit(0))).alias("NbPrixEleve"),
    F.round(F.avg("NbSubstancesDistinctes"), 2).alias("NbSubstancesMoyen"),
    F.round(F.avg("NbPresentations"), 2).alias("NbPresentationsMoyen")
)

# =========================================================
# 6. ECRITURE DES INSIGHTS
# =========================================================

insight_tables_to_drop = [
    "dbo.insight_top_laboratoires",
    "dbo.insight_innovation",
    "dbo.insight_risque",
    "dbo.insight_prix",
    "dbo.insight_remboursement",
    "dbo.insight_anciennete_innovation",
    "dbo.insight_complexite_risque",
    "dbo.insight_forme",
    "dbo.insight_executive_summary"
]

for t in insight_tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

insight_top_laboratoires.write.format("delta").saveAsTable("dbo.insight_top_laboratoires")
insight_innovation.write.format("delta").saveAsTable("dbo.insight_innovation")
insight_risque.write.format("delta").saveAsTable("dbo.insight_risque")
insight_prix.write.format("delta").saveAsTable("dbo.insight_prix")
insight_remboursement.write.format("delta").saveAsTable("dbo.insight_remboursement")
insight_anciennete_innovation.write.format("delta").saveAsTable("dbo.insight_anciennete_innovation")
insight_complexite_risque.write.format("delta").saveAsTable("dbo.insight_complexite_risque")
insight_forme.write.format("delta").saveAsTable("dbo.insight_forme")
insight_executive_summary.write.format("delta").saveAsTable("dbo.insight_executive_summary")

print("Tables storytelling créées avec succès")

# =========================================================
# 7. VUES KPI READY POUR POWER BI
# =========================================================

# -------------------------
# KPI par titulaire
# -------------------------
kpi_by_titulaire = (
    gold.groupBy("Titulaire")
        .agg(
            F.countDistinct("CIS").alias("NbMedicaments"),
            F.round(F.avg("PrixMoyen"), 2).alias("PrixMoyen"),
            F.sum(F.coalesce(F.col("FlagCritiqueGlobal"), F.lit(0))).alias("NbCritiques"),
            F.sum(F.coalesce(F.col("FlagInnovationForte"), F.lit(0))).alias("NbInnovationForte")
        )
        .orderBy(F.desc("NbMedicaments"))
)

# -------------------------
# KPI par forme
# -------------------------
kpi_by_forme = (
    gold.groupBy("FormePharmaceutique")
        .agg(
            F.countDistinct("CIS").alias("NbMedicaments"),
            F.round(F.avg("PrixMoyen"), 2).alias("PrixMoyen"),
            F.sum(F.coalesce(F.col("FlagRemboursable"), F.lit(0))).alias("NbRemboursables"),
            F.sum(F.coalesce(F.col("FlagAlerteDispo"), F.lit(0))).alias("NbAlerteDispo")
        )
        .orderBy(F.desc("NbMedicaments"))
)

# -------------------------
# KPI par innovation
# -------------------------
kpi_by_innovation = (
    gold.groupBy("InnovationLevel")
        .agg(
            F.countDistinct("CIS").alias("NbMedicaments"),
            F.round(F.avg("PrixMoyen"), 2).alias("PrixMoyen"),
            F.round(F.avg("NbPresentations"), 2).alias("NbPresentationsMoyen")
        )
        .orderBy(F.desc("NbMedicaments"))
)

# -------------------------
# KPI par risque
# -------------------------
kpi_by_risque = (
    gold.groupBy("RisqueSanitaire")
        .agg(
            F.countDistinct("CIS").alias("NbMedicaments"),
            F.round(F.avg("DureeMoyenneIndispoJours"), 2).alias("DureeMoyenneIndispoJours"),
            F.round(F.avg("PrixMoyen"), 2).alias("PrixMoyen")
        )
        .orderBy(F.desc("NbMedicaments"))
)

# =========================================================
# 8. ECRITURE KPI READY
# =========================================================

kpi_ready_tables = [
    "dbo.kpi_by_titulaire",
    "dbo.kpi_by_forme",
    "dbo.kpi_by_innovation",
    "dbo.kpi_by_risque"
]

for t in kpi_ready_tables:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

kpi_by_titulaire.write.format("delta").saveAsTable("dbo.kpi_by_titulaire")
kpi_by_forme.write.format("delta").saveAsTable("dbo.kpi_by_forme")
kpi_by_innovation.write.format("delta").saveAsTable("dbo.kpi_by_innovation")
kpi_by_risque.write.format("delta").saveAsTable("dbo.kpi_by_risque")

print("Tables KPI ready créées avec succès")

# =========================================================
# 9. CONTROLES FINAUX
# =========================================================

print("Tables disponibles dans dbo :")
spark.sql("SHOW TABLES IN dbo").show(200, truncate=False)

print("Aperçu fact table :")
spark.table("dbo.fact_bdpm_cis").show(5, truncate=False)

print("Aperçu insight executive summary :")
spark.table("dbo.insight_executive_summary").show(truncate=False)

print("Aperçu top laboratoires :")
spark.table("dbo.insight_top_laboratoires").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

gold = spark.table("dbo.gold_bdpm_cis")

# =========================
# C. DIMENSIONS ANALYTIQUES
# =========================

dim_forme = (
    gold.select("FormePharmaceutique")
    .dropDuplicates()
    .filter(F.col("FormePharmaceutique").isNotNull())
    .withColumn("FormeKey", F.monotonically_increasing_id() + 1)
    .select("FormeKey", "FormePharmaceutique")
)

dim_titulaire = (
    gold.select("Titulaire")
    .dropDuplicates()
    .filter(F.col("Titulaire").isNotNull())
    .withColumn("TitulaireKey", F.monotonically_increasing_id() + 1)
    .select("TitulaireKey", "Titulaire")
)

dim_innovation = (
    gold.select("InnovationLevel")
    .dropDuplicates()
    .filter(F.col("InnovationLevel").isNotNull())
    .withColumn("InnovationKey", F.monotonically_increasing_id() + 1)
    .select("InnovationKey", "InnovationLevel")
)

dim_risque = (
    gold.select("RisqueSanitaire")
    .dropDuplicates()
    .filter(F.col("RisqueSanitaire").isNotNull())
    .withColumn("RisqueKey", F.monotonically_increasing_id() + 1)
    .select("RisqueKey", "RisqueSanitaire")
)

dim_remboursement = (
    gold.select("RemboursementCategory")
    .dropDuplicates()
    .filter(F.col("RemboursementCategory").isNotNull())
    .withColumn("RemboursementKey", F.monotonically_increasing_id() + 1)
    .select("RemboursementKey", "RemboursementCategory")
)

dim_prix = (
    gold.select("PrixRange")
    .dropDuplicates()
    .filter(F.col("PrixRange").isNotNull())
    .withColumn("PrixRangeKey", F.monotonically_increasing_id() + 1)
    .select("PrixRangeKey", "PrixRange")
)

dim_anciennete = (
    gold.select("AncienneteAMM")
    .dropDuplicates()
    .filter(F.col("AncienneteAMM").isNotNull())
    .withColumn("AncienneteKey", F.monotonically_increasing_id() + 1)
    .select("AncienneteKey", "AncienneteAMM")
)

for table_name in [
    "dbo.dim_forme_pharmaceutique",
    "dbo.dim_titulaire",
    "dbo.dim_innovation",
    "dbo.dim_risque_sanitaire",
    "dbo.dim_remboursement",
    "dbo.dim_prix_range",
    "dbo.dim_anciennete_amm"
]:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

dim_forme.write.format("delta").saveAsTable("dbo.dim_forme_pharmaceutique")
dim_titulaire.write.format("delta").saveAsTable("dbo.dim_titulaire")
dim_innovation.write.format("delta").saveAsTable("dbo.dim_innovation")
dim_risque.write.format("delta").saveAsTable("dbo.dim_risque_sanitaire")
dim_remboursement.write.format("delta").saveAsTable("dbo.dim_remboursement")
dim_prix.write.format("delta").saveAsTable("dbo.dim_prix_range")
dim_anciennete.write.format("delta").saveAsTable("dbo.dim_anciennete_amm")

print("Dimensions analytiques créées avec succès")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
