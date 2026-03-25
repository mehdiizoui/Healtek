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
from pyspark.sql.window import Window

# =========================================
# SILVER -> GOLD (1 ligne = 1 CIS)
# Tables sources attendues dans dbo :
# silver_cis, silver_cip, silver_compo, silver_smr, silver_asmr,
# silver_liens_ct, silver_gener, silver_cpd, silver_mitm, silver_dispo
# =========================================

# ---------- lecture silver ----------
scis   = spark.table("dbo.silver_cis")
scip   = spark.table("dbo.silver_cip")
scompo = spark.table("dbo.silver_compo")
ssmr   = spark.table("dbo.silver_smr")
sasmr  = spark.table("dbo.silver_asmr")
sliens = spark.table("dbo.silver_liens_ct")
sgener = spark.table("dbo.silver_gener")
scpd   = spark.table("dbo.silver_cpd")
smitm  = spark.table("dbo.silver_mitm")
sdispo = spark.table("dbo.silver_dispo")

# ---------- helpers ----------
def latest_by(df, partition_cols, order_col):
    w = Window.partitionBy(*partition_cols).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

# ---------- base maître ----------
gold = scis.alias("c")

# ---------- agrégats CIP ----------
cip_agg = (
    scip.groupBy("CIS")
    .agg(
        F.count("*").alias("NbPresentations"),
        F.countDistinct("CIP7").alias("NbCIP7"),
        F.countDistinct("CIP13").alias("NbCIP13"),
        F.sum(F.when(F.col("EtatCommercialisationPresentation").isNotNull(), 1).otherwise(0)).alias("NbPresentationsEtatRenseigne"),
        F.sum(F.when(F.upper(F.col("EtatCommercialisationPresentation")).like("%COMMERCIALIS%"), 1).otherwise(0)).alias("NbPresentationsCommercialisees"),
        F.min("PrixMedicament").alias("PrixMin"),
        F.max("PrixMedicament").alias("PrixMax"),
        F.avg("PrixMedicament").alias("PrixMoyen"),
        F.max("TauxRemboursement").alias("TauxRemboursementMax"),
        F.min("DateDeclarationCommercialisation").alias("DatePremiereDeclarationCommercialisation"),
        F.max("DateDeclarationCommercialisation").alias("DateDerniereDeclarationCommercialisation"),
        F.concat_ws(" | ", F.slice(F.array_sort(F.collect_set("LibellePresentation")), 1, 5)).alias("TopPresentations"),
        F.first("CIP13", ignorenulls=True).alias("CIP13_Exemple")
    )
)

# ---------- agrégats composition ----------
compo_agg = (
    scompo.groupBy("CIS")
    .agg(
        F.count("*").alias("NbLignesComposition"),
        F.countDistinct("CodeSubstance").alias("NbSubstancesDistinctes"),
        F.concat_ws(" | ", F.slice(F.array_sort(F.collect_set("DenominationSubstance")), 1, 15)).alias("ListeSubstances"),
        F.concat_ws(" | ", F.slice(F.array_sort(F.collect_set("NatureComposant")), 1, 10)).alias("ListeNatureComposant")
    )
)

# ---------- SMR latest + agrégats ----------
smr_latest = latest_by(ssmr, ["CIS"], "DateAvis") \
    .select(
        "CIS",
        F.col("CodeDossierHAS").alias("SMR_CodeDossierHAS"),
        F.col("MotifEvaluation").alias("SMR_MotifEvaluation_Recent"),
        F.col("DateAvis").alias("SMR_DateAvis_Recent"),
        F.col("ValeurSMR").alias("SMR_Valeur_Recent"),
        F.col("LibelleSMR").alias("SMR_Libelle_Recent")
    )

smr_agg = (
    ssmr.groupBy("CIS")
    .agg(
        F.count("*").alias("NbAvisSMR"),
        F.max("DateAvis").alias("DerniereDateAvisSMR")
    )
)

# ---------- ASMR latest + agrégats ----------
asmr_latest = latest_by(sasmr, ["CIS"], "DateAvis") \
    .select(
        "CIS",
        F.col("CodeDossierHAS").alias("ASMR_CodeDossierHAS"),
        F.col("MotifEvaluation").alias("ASMR_MotifEvaluation_Recent"),
        F.col("DateAvis").alias("ASMR_DateAvis_Recent"),
        F.col("ValeurASMR").alias("ASMR_Valeur_Recent"),
        F.col("LibelleASMR").alias("ASMR_Libelle_Recent")
    )

asmr_agg = (
    sasmr.groupBy("CIS")
    .agg(
        F.count("*").alias("NbAvisASMR"),
        F.max("DateAvis").alias("DerniereDateAvisASMR")
    )
)

# ---------- liens CT ----------
liens_smr = smr_latest.join(sliens, smr_latest["SMR_CodeDossierHAS"] == sliens["CodeDossierHAS"], "left") \
    .select(smr_latest["CIS"], F.col("LienPageCT").alias("LienPageCT_SMR"))

liens_asmr = asmr_latest.join(sliens, asmr_latest["ASMR_CodeDossierHAS"] == sliens["CodeDossierHAS"], "left") \
    .select(asmr_latest["CIS"], F.col("LienPageCT").alias("LienPageCT_ASMR"))

# ---------- génériques ----------
gener_agg = (
    sgener.groupBy("CIS")
    .agg(
        F.count("*").alias("NbLignesGenerique"),
        F.max(F.when(F.col("IdentifiantGroupe").isNotNull(), 1).otherwise(0)).alias("EstDansGroupeGenerique"),
        F.first("IdentifiantGroupe", ignorenulls=True).alias("IdentifiantGroupeGenerique"),
        F.first("LibelleGroupe", ignorenulls=True).alias("LibelleGroupeGenerique"),
        F.concat_ws(" | ", F.slice(F.array_sort(F.collect_set("TypeGenerique")), 1, 10)).alias("TypesGenerique")
    )
)

# ---------- conditions prescription ----------
cpd_agg = (
    scpd.groupBy("CIS")
    .agg(
        F.count("*").alias("NbConditionsPrescription"),
        F.concat_ws(" | ", F.slice(F.collect_set("ConditionPrescription"), 1, 20)).alias("ConditionsPrescription")
    )
)

# ---------- MITM ----------
mitm_agg = (
    smitm.groupBy("CIS")
    .agg(
        F.max(F.when(F.col("StatutMITM").isNotNull(), 1).otherwise(0)).alias("FlagMITM"),
        F.first("StatutMITM", ignorenulls=True).alias("StatutMITM")
    )
)

# ---------- disponibilité ----------
dispo_latest = latest_by(sdispo, ["CIS"], "DateMiseAJour") \
    .select(
        "CIS",
        F.col("CodeStatut").alias("Dispo_CodeStatut_Recent"),
        F.col("DateDebut").alias("Dispo_DateDebut_Recent"),
        F.col("DateMiseAJour").alias("Dispo_DateMiseAJour_Recent"),
        F.col("DateRemiseDispo").alias("Dispo_DateRemiseDispo_Recent"),
        F.col("LienANSM").alias("Dispo_LienANSM_Recent")
    )

sdispo_calc = sdispo.withColumn(
    "DureeIndispoJours",
    F.when(
        F.col("DateDebut").isNotNull(),
        F.datediff(F.coalesce(F.col("DateRemiseDispo"), F.current_date()), F.col("DateDebut"))
    )
)

dispo_agg = (
    sdispo_calc.groupBy("CIS")
    .agg(
        F.count("*").alias("NbEpisodesDispo"),
        F.max("DateMiseAJour").alias("DerniereDateMAJDispo"),
        F.avg("DureeIndispoJours").alias("DureeMoyenneIndispoJours"),
        F.max(
            F.when(
                F.upper(F.col("CodeStatut")).like("%RUPTURE%") |
                F.upper(F.col("CodeStatut")).like("%TENSION%") |
                F.upper(F.col("CodeStatut")).like("%INDISPON%"), 1
            ).otherwise(0)
        ).alias("FlagAlerteDispo")
    )
)

# ---------- enrichissements métier ----------
gold = (
    gold
    .join(cip_agg, "CIS", "left")
    .join(compo_agg, "CIS", "left")
    .join(smr_latest, "CIS", "left")
    .join(smr_agg, "CIS", "left")
    .join(asmr_latest, "CIS", "left")
    .join(asmr_agg, "CIS", "left")
    .join(liens_smr, "CIS", "left")
    .join(liens_asmr, "CIS", "left")
    .join(gener_agg, "CIS", "left")
    .join(cpd_agg, "CIS", "left")
    .join(mitm_agg, "CIS", "left")
    .join(dispo_latest, "CIS", "left")
    .join(dispo_agg, "CIS", "left")
    .withColumn("FlagCommercialise", F.when(F.upper(F.col("EtatCommercialisation")).like("%COMMERCIALIS%"), 1).otherwise(0))
    .withColumn("FlagSurveillanceRenforcee", F.when(F.upper(F.coalesce(F.col("SurveillanceRenforcee"), F.lit(""))).like("%OUI%"), 1).otherwise(0))
    .withColumn("FlagRemboursable", F.when(F.coalesce(F.col("TauxRemboursementMax"), F.lit(0)) > 0, 1).otherwise(0))
    .withColumn("FlagPrixRenseigne", F.when(F.col("PrixMoyen").isNotNull(), 1).otherwise(0))
    .withColumn("FlagEvaluationHAS", F.when(F.col("NbAvisSMR").isNotNull() | F.col("NbAvisASMR").isNotNull(), 1).otherwise(0))
    .withColumn("GoldLoadDate", F.current_date())
)

# ---------- écriture ----------
gold.write.mode("overwrite").format("delta").saveAsTable("dbo.gold_bdpm_cis")

print("Table Gold créée : dbo.gold_bdpm_cis")
spark.sql("SELECT COUNT(*) AS nb_lignes FROM dbo.gold_bdpm_cis").show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Toujours relire la table au début du notebook
gold = spark.table("dbo.gold_bdpm_cis")

# =========================
# ENRICHISSEMENTS METIER
# =========================
gold_enriched = (
    gold

    # 1. Segment prix
    .withColumn(
        "PrixRange",
        F.when(F.col("PrixMoyen").isNull(), F.lit("Non renseigné"))
         .when(F.col("PrixMoyen") < 10, F.lit("Low"))
         .when(F.col("PrixMoyen") < 50, F.lit("Medium"))
         .otherwise(F.lit("High"))
    )

    # 2. Catégorie remboursement
    .withColumn(
        "RemboursementCategory",
        F.when(F.col("TauxRemboursementMax").isNull(), F.lit("Non renseigné"))
         .when(F.col("TauxRemboursementMax") >= 65, F.lit("Fort"))
         .when(F.col("TauxRemboursementMax") >= 30, F.lit("Moyen"))
         .when(F.col("TauxRemboursementMax") > 0, F.lit("Faible"))
         .otherwise(F.lit("Non remboursable"))
    )

    # 3. Niveau innovation
    .withColumn(
        "InnovationLevel",
        F.when(F.upper(F.coalesce(F.col("ASMR_Valeur_Recent").cast("string"), F.lit(""))).isin("I", "II", "III", "1", "2", "3"), F.lit("Innovation forte"))
         .when(F.upper(F.coalesce(F.col("ASMR_Valeur_Recent").cast("string"), F.lit(""))).isin("IV", "4"), F.lit("Innovation modérée"))
         .when(F.upper(F.coalesce(F.col("ASMR_Valeur_Recent").cast("string"), F.lit(""))).isin("V", "5"), F.lit("Me-too"))
         .when(F.col("ASMR_Valeur_Recent").isNull(), F.lit("Non évalué"))
         .otherwise(F.lit("Autre"))
    )

    # 4. Niveau SMR simplifié
    .withColumn(
        "SMR_Level",
        F.when(F.upper(F.coalesce(F.col("SMR_Libelle_Recent"), F.lit(""))).contains("IMPORTANT"), F.lit("Important"))
         .when(F.upper(F.coalesce(F.col("SMR_Libelle_Recent"), F.lit(""))).contains("MOD"), F.lit("Modéré"))
         .when(F.upper(F.coalesce(F.col("SMR_Libelle_Recent"), F.lit(""))).contains("FAIBLE"), F.lit("Faible"))
         .when(F.upper(F.coalesce(F.col("SMR_Libelle_Recent"), F.lit(""))).contains("INSUFF"), F.lit("Insuffisant"))
         .otherwise(F.lit("Non évalué"))
    )

    # 5. Risque sanitaire
    .withColumn(
        "RisqueSanitaire",
        F.when(
            (F.coalesce(F.col("FlagMITM"), F.lit(0)) == 1) &
            (F.coalesce(F.col("FlagAlerteDispo"), F.lit(0)) == 1),
            F.lit("Critique")
        )
         .when(F.coalesce(F.col("FlagAlerteDispo"), F.lit(0)) == 1, F.lit("Tension"))
         .when(F.coalesce(F.col("FlagMITM"), F.lit(0)) == 1, F.lit("Sous surveillance"))
         .otherwise(F.lit("Stable"))
    )

    # 6. Complexité produit
    .withColumn(
        "Complexite",
        F.when(F.col("NbSubstancesDistinctes").isNull(), F.lit("Non renseigné"))
         .when(F.col("NbSubstancesDistinctes") >= 3, F.lit("Complexe"))
         .when(F.col("NbSubstancesDistinctes") == 2, F.lit("Moyenne"))
         .when(F.col("NbSubstancesDistinctes") == 1, F.lit("Simple"))
         .otherwise(F.lit("Non renseigné"))
    )

    # 7. Âge AMM en années
    .withColumn(
        "AgeAMM_Annees",
        F.when(
            F.col("DateAMM").isNotNull(),
            F.round(F.months_between(F.current_date(), F.col("DateAMM")) / 12, 1)
        ).otherwise(F.lit(None))
    )

    # 8. Ancienneté AMM
    .withColumn(
        "AncienneteAMM",
        F.when(F.col("AgeAMM_Annees").isNull(), F.lit("Non renseigné"))
         .when(F.col("AgeAMM_Annees") < 5, F.lit("Récent"))
         .when(F.col("AgeAMM_Annees") < 15, F.lit("Intermédiaire"))
         .otherwise(F.lit("Ancien"))
    )

    # 9. Intensité portefeuille
    .withColumn(
        "IntensitePortefeuille",
        F.when(F.col("NbPresentations").isNull(), F.lit("Non renseigné"))
         .when(F.col("NbPresentations") >= 10, F.lit("Très large"))
         .when(F.col("NbPresentations") >= 5, F.lit("Large"))
         .when(F.col("NbPresentations") >= 2, F.lit("Moyen"))
         .otherwise(F.lit("Réduit"))
    )

    # 10. Indicateurs booléens utiles
    .withColumn(
        "FlagInnovationForte",
        F.when(F.col("InnovationLevel") == "Innovation forte", F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "FlagMeToo",
        F.when(F.col("InnovationLevel") == "Me-too", F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "FlagPrixEleve",
        F.when(F.col("PrixMoyen") >= 50, F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "FlagCritiqueGlobal",
        F.when(F.col("RisqueSanitaire") == "Critique", F.lit(1)).otherwise(F.lit(0))
    )

    # 11. Date de chargement gold enrichi
    .withColumn("GoldEnrichmentLoadDate", F.current_timestamp())
)

# On écrit dans une NOUVELLE table pour éviter tout conflit de schéma
spark.sql("DROP TABLE IF EXISTS dbo.gold_bdpm_cis_enriched")
gold_enriched.write.format("delta").saveAsTable("dbo.gold_bdpm_cis_enriched")

print("Table dbo.gold_bdpm_cis_enriched créée avec succès")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
