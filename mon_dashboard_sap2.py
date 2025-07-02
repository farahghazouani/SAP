import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
import io
import re
import plotly.figure_factory as ff

# --- Chemins vers vos fichiers de données ---
# IMPORTANT : Les chemins ont été modifiés pour être RELATIFS au script Python.
# Cela suppose que tous les fichiers Excel sont dans le MÊME DOSSIER que ce script.
# Si vos fichiers sont dans un sous-dossier (ex: 'data/'), ajustez les chemins comme 'data/memory_final_cleaned_clean.xlsx'.
DATA_PATHS = {
    "memory": "memory_final_cleaned_clean.xlsx",
    "hitlist_db": "HITLIST_DATABASE_final_cleaned_clean.xlsx",
    "times": "Times_final_cleaned_clean.xlsx",
    "tasktimes": "TASKTIMES_final_cleaned_clean.xlsx",
    "usertcode": "USERTCODE_cleaned.xlsx",
    "performance": "AL_GET_PERFORMANCE_final_cleaned_clean.xlsx",
    "sql_trace_summary": "performance_trace_summary_final_cleaned_clean.xlsx",
    "usr02": "usr02_data.xlsx",
}

# --- Configuration de la page Streamlit ---
st.set_page_config(layout="wide", page_title="Dashboard SAP Complet Multi-Sources")

# Note sur le thème : Streamlit utilise par défaut un thème clair. Si l'arrière-plan apparaît sombre,
# cela peut être dû aux paramètres de votre navigateur ou à une configuration globale de Streamlit (.streamlit/config.toml).
# Ce script ne force pas un thème sombre.

# --- Fonctions de Nettoyage et Chargement des Données (avec cache) ---

@st.cache_data
def load_and_process_data(file_key, path):
    """Charge et nettoie un fichier Excel/CSV."""
    df = pd.DataFrame()
    try:
        if path.lower().endswith('.xlsx'):
            df = pd.read_excel(path)
        elif path.lower().endswith('.csv'):
            df = pd.read_csv(path)
        else:
            st.error(f"Format de fichier non supporté pour {file_key}: {path}")
            return pd.DataFrame()

        df = clean_column_names(df.copy())

        # Apply specific cleaning based on file_key
        if file_key == "memory":
            numeric_cols = ['MEMSUM', 'PRIVSUM', 'USEDBYTES', 'MAXBYTES', 'MAXBYTESDI', 'PRIVCOUNT', 'RESTCOUNT', 'COUNTER']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = clean_numeric_with_comma(df[col]) # Use the improved numeric cleaner
            
            if 'ACCOUNT' in df.columns:
                df['ACCOUNT'] = clean_string_column(df['ACCOUNT'], 'Compte Inconnu')
            if 'MANDT' in df.columns:
                df['MANDT'] = clean_string_column(df['MANDT'], 'MANDT Inconnu')
            if 'TASKTYPE' in df.columns:
                df['TASKTYPE'] = clean_string_column(df['TASKTYPE'], 'Type de Tâche Inconnu')

            if 'ENDDATE' in df.columns and 'ENDTIME' in df.columns:
                # Ensure ENDTIME is treated as string for zfill
                df['ENDTIME_STR'] = df['ENDTIME'].astype(str).str.zfill(6)
                df['FULL_DATETIME'] = pd.to_datetime(df['ENDDATE'].astype(str) + df['ENDTIME_STR'], format='%Y%m%d%H%M%S', errors='coerce')
                df.drop(columns=['ENDTIME_STR'], inplace=True, errors='ignore')
            
            # Drop rows where critical columns for memory analysis are NaN
            subset_cols_memory = []
            if 'USEDBYTES' in df.columns:
                subset_cols_memory.append('USEDBYTES')
            if 'ACCOUNT' in df.columns:
                subset_cols_memory.append('ACCOUNT')
            if subset_cols_memory:
                # Only drop if the columns exist and have NaNs
                df.dropna(subset=subset_cols_memory, inplace=True)


        elif file_key == "hitlist_db":
            numeric_cols = [
                'GENERATETI', 'REPLOADTI', 'CUALOADTI', 'DYNPLOADTI', 'QUETI', 'DDICTI', 'CPICTI',
                'LOCKCNT', 'LOCKTI', 'BTCSTEPNR', 'RESPTI', 'PROCTI', 'CPUTI', 'QUEUETI', 'ROLLWAITTI',
                'GUITIME', 'GUICNT', 'GUINETTIME', 'DBP_COUNT', 'DBP_TIME', 'DSQLCNT', 'QUECNT',
                'CPICCNT', 'SLI_CNT', 'TAB1DIRCNT', 'TAB1SEQCNT', 'TAB1UPDCNT', 'TAB2DIRCNT',
                'TAB2SEQCNT', 'TAB2UPDCNT', 'TAB3DIRCNT', 'TAB3SEQCNT', 'TAB3UPDCNT', 'TAB4DIRCNT',
                'TAB4SEQCNT', 'TAB4UPDCNT', 'TAB5DIRCNT', 'TAB5SEQCNT', 'TAB5UPDCNT',
                'READDIRCNT', 'READDIRTI', 'READDIRBUF', 'READDIRREC', 'READSEQCNT', 'READSEQTI',
                'READSEQBUF', 'READSEQREC', 'PHYREADCNT', 'INSCNT', 'INSTI', 'INSREC', 'PHYINSCNT',
                'UPDCNT', 'UPDTI', 'UPDREC', 'PHYUPDCNT', 'DELCNT', 'DELTI', 'DELREC', 'PHYDELCNT',
                'DBCALLS', 'COMMITTI', 'INPUTLEN', 'OUTPUTLEN', 'MAXROLL', 'MAXPAGE',
                'ROLLINCNT', 'ROLLINTI', 'ROLLOUTCNT', 'ROLLOUTTI', 'ROLLED_OUT', 'PRIVSUM',
                'USEDBYTES', 'MAXBYTES', 'MAXBYTESDI', 'RFCRECEIVE', 'RFCSEND',
                'RFCEXETIME', 'RFCCALLTIM', 'RFCCALLS', 'VMC_CALL_COUNT', 'VMC_CPU_TIME', 'VMC_ELAP_TIME'
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = clean_numeric_with_comma(df[col])
            
            if 'ENDDATE' in df.columns and 'ENDTIME' in df.columns:
                df['ENDTIME_STR'] = df['ENDTIME'].astype(str).str.zfill(6)
                df['FULL_DATETIME'] = pd.to_datetime(df['ENDDATE'].astype(str) + df['ENDTIME_STR'], format='%Y%m%d%H%M%S', errors='coerce')
                df.drop(columns=['ENDTIME_STR'], inplace=True, errors='ignore')

            subset_cols_hitlist = []
            if 'RESPTI' in df.columns: subset_cols_hitlist.append('RESPTI')
            if 'PROCTI' in df.columns: subset_cols_hitlist.append('PROCTI')
            if 'CPUTI' in df.columns: subset_cols_hitlist.append('CPUTI')
            if 'DBCALLS' in df.columns: subset_cols_hitlist.append('DBCALLS')
            if subset_cols_hitlist:
                df.dropna(subset=subset_cols_hitlist, inplace=True)
            if 'FULL_DATETIME' in df.columns:
                df.dropna(subset=['FULL_DATETIME'], inplace=True)

            for col in ['WPID', 'ACCOUNT', 'REPORT', 'ROLLKEY', 'PRIVMODE', 'WPRESTART', 'TASKTYPE']:
                if col in df.columns:
                    df[col] = clean_string_column(df[col])


        elif file_key == "times":
            numeric_cols = [
                'COUNT', 'LUW_COUNT', 'RESPTI', 'PROCTI', 'CPUTI', 'QUEUETI', 'ROLLWAITTI',
                'GUITIME', 'GUICNT', 'GUINETTIME', 'DBP_COUNT', 'DBP_TIME', 'READDIRCNT',
                'READDIRTI', 'READDIRBUF', 'READDIRREC', 'READSEQCNT', 'READSEQTI',
                'READSEQBUF', 'READSEQREC', 'CHNGCNT', 'CHNGTI', 'CHNGREC', 'PHYREADCNT',
                'PHYCHNGREC', 'PHYCALLS', 'VMC_CALL_COUNT', 'VMC_CPU_TIME', 'VMC_ELAP_TIME'
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = clean_numeric_with_comma(df[col])
            
            subset_cols_times = []
            if 'RESPTI' in df.columns: subset_cols_times.append('RESPTI')
            if 'PHYCALLS' in df.columns: subset_cols_times.append('PHYCALLS')
            if 'COUNT' in df.columns: subset_cols_times.append('COUNT')
            if subset_cols_times:
                df.dropna(subset=subset_cols_times, inplace=True)
            
            if 'TIME' in df.columns:
                df['TIME'] = clean_string_column(df['TIME'])
            if 'TASKTYPE' in df.columns:
                df['TASKTYPE'] = clean_string_column(df['TASKTYPE'])
            if 'ENTRY_ID' in df.columns:
                df['ENTRY_ID'] = clean_string_column(df[col])

        elif file_key == "tasktimes":
            numeric_cols = [
                'COUNT', 'RESPTI', 'PROCTI', 'CPUTI', 'QUEUETI', 'ROLLWAITTI', 'GUITIME',
                'GUICNT', 'GUINETTIME', 'DBP_COUNT', 'DBP_TIME', 'READDIRCNT', 'READDIRTI',
                'READDIRBUF', 'READDIRREC', 'READSEQCNT', 'READSEQTI',
                'READSEQBUF', 'READSEQREC', 'CHNGCNT', 'CHNGTI', 'CHNGREC', 'PHYREADCNT',
                'PHYCHNGREC', 'PHYCALLS', 'CNT001', 'CNT002', 'CNT003', 'CNT004', 'CNT005', 'CNT006', 'CNT007', 'CNT008', 'CNT009'
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = clean_numeric_with_comma(df[col])
            
            subset_cols_tasktimes = []
            if 'COUNT' in df.columns: subset_cols_tasktimes.append('COUNT')
            if 'RESPTI' in df.columns: subset_cols_tasktimes.append('RESPTI')
            if 'CPUTI' in df.columns: subset_cols_tasktimes.append('CPUTI')
            if subset_cols_tasktimes:
                df.dropna(subset=subset_cols_tasktimes, inplace=True)
            
            if 'TASKTYPE' in df.columns:
                df['TASKTYPE'] = clean_string_column(df['TASKTYPE'], 'Type de tâche non spécifié')
            if 'TIME' in df.columns:
                df['TIME'] = clean_string_column(df['TIME'])


        elif file_key == "usertcode":
            numeric_cols = [
                'COUNT', 'DCOUNT', 'UCOUNT', 'BCOUNT', 'ECOUNT', 'SCOUNT', 'LUW_COUNT',
                'TMBYTESIN', 'TMBYTESOUT', 'RESPTI', 'PROCTI', 'CPUTI', 'QUEUETI',
                'ROLLWAITTI', 'GUITIME', 'GUICNT', 'GUINETTIME', 'DBP_COUNT', 'DBP_TIME',
                'READDIRCNT', 'READDIRTI', 'READDIRBUF', 'READDIRREC', 'READSEQCNT',
                'READSEQTI', 'READSEQBUF', 'READSEQREC', 'CHNGCNT', 'CHNGTI', 'CHNGREC',
                'PHYREADCNT', 'PHYCHNGREC', 'PHYCALLS', 'DSQLCNT', 'QUECNT', 'CPICCNT',
                'SLI_CNT', 'VMC_CALL_COUNT', 'VMC_CPU_TIME', 'VMC_ELAP_TIME'
            ]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = clean_numeric_with_comma(df[col])
            
            if 'ENDDATE' in df.columns and 'ENDTIME' in df.columns:
                df['ENDTIME_STR'] = df['ENDTIME'].astype(str).str.zfill(6)
                df['FULL_DATETIME'] = pd.to_datetime(df['ENDDATE'].astype(str) + df['ENDTIME_STR'], format='%Y%m%d%H%M%S', errors='coerce')
                df.drop(columns=['ENDTIME_STR'], inplace=True, errors='ignore')

            critical_usertcode_cols = []
            if 'RESPTI' in df.columns: critical_usertcode_cols.append('RESPTI')
            if 'ACCOUNT' in df.columns: critical_usertcode_cols.append('ACCOUNT')
            if 'COUNT' in df.columns: critical_usertcode_cols.append('COUNT')
            
            if critical_usertcode_cols:
                df.dropna(subset=critical_usertcode_cols, inplace=True)
            
            for col in ['TASKTYPE', 'ENTRY_ID', 'ACCOUNT']:
                if col in df.columns:
                    df[col] = clean_string_column(df[col])

        elif file_key == "performance":
            if 'WP_CPU' in df.columns:
                df['WP_CPU_SECONDS'] = df['WP_CPU'].apply(convert_mm_ss_to_seconds)
            
            if 'WP_IWAIT' in df.columns:
                df['WP_IWAIT'] = clean_numeric_with_comma(df['WP_IWAIT'])
                df['WP_IWAIT_SECONDS'] = df['WP_IWAIT'] / 1000.0 
            else:
                df['WP_IWAIT_SECONDS'] = 0

            for col in ['WP_SEMSTAT', 'WP_IACTION', 'WP_ITYPE', 'WP_RESTART', 'WP_ISTATUS', 'WP_TYP', 'WP_STATUS']:
                if col in df.columns:
                    df[col] = clean_string_column(df[col])
            
            numeric_cols_perf = ['WP_NO', 'WP_IRESTRT', 'WP_PID', 'WP_INDEX']
            for col in numeric_cols_perf:
                if col in df.columns:
                    df[col] = clean_numeric_with_comma(df[col])
            
            subset_cols_perf = []
            if 'WP_CPU_SECONDS' in df.columns: subset_cols_perf.append('WP_CPU_SECONDS')
            if 'WP_STATUS' in df.columns: subset_cols_perf.append('WP_STATUS')
            if subset_cols_perf:
                df.dropna(subset=subset_cols_perf, inplace=True)
        
        elif file_key == "sql_trace_summary":
            numeric_cols_sql = ['TOTALEXEC', 'IDENTSEL', 'EXECTIME', 'RECPROCNUM', 'TIMEPEREXE', 'RECPEREXE', 'AVGTPERREC', 'MINTPERREC']
            for col in numeric_cols_sql:
                if col in df.columns:
                    df[col] = clean_numeric_with_comma(df[col])
            
            for col in ['SQLSTATEM', 'SERVERNAME', 'TRANS_ID']:
                if col in df.columns:
                    df[col] = clean_string_column(df[col])
            
            subset_cols_sql = []
            if 'EXECTIME' in df.columns: subset_cols_sql.append('EXECTIME')
            if 'TOTALEXEC' in df.columns: subset_cols_sql.append('TOTALEXEC')
            if 'SQLSTATEM' in df.columns: subset_cols_sql.append('SQLSTATEM')
            if subset_cols_sql:
                df.dropna(subset=subset_cols_sql, inplace=True)

        elif file_key == "usr02":
            for col in ['BNAME', 'USTYP']:
                if col in df.columns:
                    df[col] = clean_string_column(df[col])
            
            if 'GLTGB' in df.columns:
                # Replace '00000000' with NaN before converting to datetime
                df['GLTGB'] = df['GLTGB'].astype(str).replace('00000000', np.nan)
                df['GLTGB_DATE'] = pd.to_datetime(df['GLTGB'], format='%Y%m%d', errors='coerce')
            else:
                df['GLTGB_DATE'] = pd.NaT # Assign NaT if column doesn't exist

        return df

    except FileNotFoundError:
        st.error(f"Erreur: Le fichier '{path}' pour '{file_key}' est introuvable. Veuillez vérifier le chemin.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Une erreur est survenue lors du traitement du fichier '{file_key}' : {e}. Détails : {e}")
        return pd.DataFrame()

def clean_string_column(series, default_value="Non défini"):
    """
    Nettoie une série de type string : supprime espaces, remplace NaN/vides/caractères non imprimables.
    """
    # Convert to string, strip whitespace, replace non-printable chars, then strip again
    cleaned_series = series.astype(str).str.strip()
    cleaned_series = cleaned_series.apply(lambda x: re.sub(r'[^\x20-\x7E\s]+', ' ', x).strip())
    # Replace 'nan' string (from NaN conversion), empty strings, and strings with only spaces
    cleaned_series = cleaned_series.replace({'nan': default_value, '': default_value, ' ': default_value})
    return cleaned_series

def clean_column_names(df):
    """
    Nettoyage des noms de colonnes : supprime les espaces, les caractères invisibles,
    et s'assure qu'ils sont valides pour l'accès.
    """
    new_columns = []
    for col in df.columns:
        # Remove non-printable ASCII characters and strip whitespace
        cleaned_col = re.sub(r'[\x00-\x1F\x7F-\x9F]', '', str(col)).strip()
        # Replace any remaining non-alphanumeric/underscore with underscore, and collapse multiple underscores
        cleaned_col = re.sub(r'[^a-zA-Z0-9_]', '_', cleaned_col)
        cleaned_col = re.sub(r'_+', '_', cleaned_col)
        # Remove leading/trailing underscores
        cleaned_col = cleaned_col.strip('_')
        new_columns.append(cleaned_col)
    df.columns = new_columns
    return df

def convert_mm_ss_to_seconds(time_str):
    """
    Convertit une chaîne de caractères au format MM:SS en secondes.
    Gère les cas où les minutes ou secondes sont manquantes ou invalides.
    """
    if pd.isna(time_str) or not isinstance(time_str, str):
        return 0
    try:
        parts = time_str.split(':')
        if len(parts) == 2:
            minutes = float(parts[0])
            seconds = float(parts[1])
            return int(minutes * 60 + seconds)
        elif len(parts) == 1:
            return int(float(parts[0]))
        else:
            return 0
    except ValueError:
        return 0

def clean_numeric_with_comma(series):
    """
    Nettoyage d'une série de chaînes numériques qui peuvent contenir des virgules
    comme séparateurs de milliers ou décimaux, et conversion en float.
    Gère également les parenthèses pour les nombres négatifs et les espaces.
    """
    if series.dtype == 'object': # Only process if it's a string/object column
        # Convert to string, strip whitespace
        cleaned_series = series.astype(str).str.strip()
        # Handle negative numbers in parentheses, e.g., (123.45) -> -123.45
        cleaned_series = cleaned_series.str.replace(r'^\((.*)\)$', r'-\1', regex=True)
        # Remove thousands separators (space, comma, dot) - be careful with decimal dot
        # This regex removes commas and spaces, and then handles dots as potential thousands separators
        cleaned_series = cleaned_series.str.replace(r'[,\s]', '', regex=True)
        # Convert to numeric, coerce errors to NaN, then fill NaN with 0
        return pd.to_numeric(cleaned_series, errors='coerce').fillna(0)
    return pd.to_numeric(series, errors='coerce').fillna(0)


# --- Chargement de TOUTES les données ---
dfs = {}
for key, path in DATA_PATHS.items():
    dfs[key] = load_and_process_data(key, path)

# --- Contenu principal du Dashboard ---
st.title("📊 Tableau de Bord SAP Complet Multi-Sources")
st.markdown("Explorez les performances, l'utilisation mémoire, les transactions utilisateurs et la santé du système à travers différentes sources de données.")

# --- Affichage des KPIs ---
st.markdown("---")
st.subheader("Indicateurs Clés de Performance (KPIs)")
kpi_cols = st.columns(5)

# KPI 1: Temps de Réponse Moyen Global (Hitlist DB)
avg_resp_time = 0
if not dfs['hitlist_db'].empty and 'RESPTI' in dfs['hitlist_db'].columns:
    avg_resp_time = dfs['hitlist_db']['RESPTI'].mean() / 1000
kpi_cols[0].metric("Temps de Réponse Moyen (s)", f"{avg_resp_time:.2f}")

# KPI 2: Utilisation Mémoire Moyenne (USEDBYTES)
avg_memory_usage = 0
if not dfs['memory'].empty and 'USEDBYTES' in dfs['memory'].columns:
    avg_memory_usage = dfs['memory']['USEDBYTES'].mean() / (1024 * 1024)
kpi_cols[1].metric("Mémoire Moyenne (Mo)", f"{avg_memory_usage:.2f}")

# KPI 3: Total des Appels Base de Données (Hitlist DB)
total_db_calls = 0
if not dfs['hitlist_db'].empty and 'DBCALLS' in dfs['hitlist_db'].columns:
    total_db_calls = dfs['hitlist_db']['DBCALLS'].sum()
kpi_cols[2].metric("Total Appels DB", f"{int(total_db_calls):,}".replace(",", " "))

# KPI 4: Total des Exécutions SQL (performance_trace_summary)
total_sql_executions = 0
if not dfs['sql_trace_summary'].empty and 'TOTALEXEC' in dfs['sql_trace_summary'].columns:
    total_sql_executions = dfs['sql_trace_summary']['TOTALEXEC'].sum()
kpi_cols[3].metric("Total Exécutions SQL", f"{int(total_sql_executions):,}".replace(",", " "))

# KPI 5: Temps CPU Moyen Global (Hitlist DB)
avg_cpu_time = 0
if not dfs['hitlist_db'].empty and 'CPUTI' in dfs['hitlist_db'].columns:
    avg_cpu_time = dfs['hitlist_db']['CPUTI'].mean() / 1000
kpi_cols[4].metric("Temps CPU Moyen (s)", f"{avg_cpu_time:.2f}")

st.markdown("---")

# --- Initialisation de l'état de la session pour la navigation ---
tab_titles = [
    "Analyse Mémoire",
    "Transactions Utilisateurs",
    "Statistiques Horaires",
    "Décomposition des Tâches",
    "Insights Hitlist DB",
    "Performance des Processus de Travail",
    "Résumé des Traces de Performance SQL",
    "Analyse des Utilisateurs"
]

if 'current_section_label' not in st.session_state:
    st.session_state.current_section_label = tab_titles[0] # Défaut au premier onglet

# --- Sidebar pour les filtres globaux et la navigation rapide ---
st.sidebar.header("Filtres")

# Filtre par ACCOUNT (commun à memory, usertcode, hitlist_db)
all_accounts = pd.Index([])
if not dfs['memory'].empty and 'ACCOUNT' in dfs['memory'].columns:
    all_accounts = all_accounts.union(dfs['memory']['ACCOUNT'].dropna().unique())
if not dfs['usertcode'].empty and 'ACCOUNT' in dfs['usertcode'].columns:
    all_accounts = all_accounts.union(dfs['usertcode']['ACCOUNT'].dropna().unique())
if not dfs['hitlist_db'].empty and 'ACCOUNT' in dfs['hitlist_db'].columns:
    all_accounts = all_accounts.union(dfs['hitlist_db']['ACCOUNT'].dropna().unique())

selected_accounts = []
if not all_accounts.empty:
    selected_accounts = st.sidebar.multiselect(
        "Sélectionner des Comptes",
        options=sorted(all_accounts.tolist()),
        default=[]
    )
    # Apply filter to copies of original dataframes for each section
    # This ensures filters are applied consistently across sections
    for key in ['memory', 'usertcode', 'hitlist_db']:
        if not dfs[key].empty and 'ACCOUNT' in dfs[key].columns and selected_accounts:
            dfs[key] = dfs[key][dfs[key]['ACCOUNT'].isin(selected_accounts)]

# Filtre par REPORT (commun à hitlist_db)
selected_reports = []
if not dfs['hitlist_db'].empty and 'REPORT' in dfs['hitlist_db'].columns:
    all_reports = dfs['hitlist_db']['REPORT'].dropna().unique().tolist()
    selected_reports = st.sidebar.multiselect(
        "Sélectionner des Rapports (Hitlist DB)",
        options=sorted(all_reports),
        default=[]
    )
    if selected_reports:
        dfs['hitlist_db'] = dfs['hitlist_db'][dfs['hitlist_db']['REPORT'].isin(selected_reports)]

# Filtre par TASKTYPE (commun à usertcode, times, tasktimes, hitlist_db)
all_tasktypes = pd.Index([])
if not dfs['usertcode'].empty and 'TASKTYPE' in dfs['usertcode'].columns:
    all_tasktypes = all_tasktypes.union(dfs['usertcode']['TASKTYPE'].dropna().unique())
if not dfs['times'].empty and 'TASKTYPE' in dfs['times'].columns:
    all_tasktypes = all_tasktypes.union(dfs['times']['TASKTYPE'].dropna().unique())
if not dfs['tasktimes'].empty and 'TASKTYPE' in dfs['tasktimes'].columns:
    all_tasktypes = all_tasktypes.union(dfs['tasktimes']['TASKTYPE'].dropna().unique())
if not dfs['hitlist_db'].empty and 'TASKTYPE' in dfs['hitlist_db'].columns:
    all_tasktypes = all_tasktypes.union(dfs['hitlist_db']['TASKTYPE'].dropna().unique())

selected_tasktypes = []
if not all_tasktypes.empty:
    selected_tasktypes = st.sidebar.multiselect(
        "Sélectionner des Types de Tâches",
        options=sorted(all_tasktypes.tolist()),
        default=[]
    )
    if selected_tasktypes:
        for key in ['usertcode', 'times', 'tasktimes', 'hitlist_db']:
            if not dfs[key].empty and 'TASKTYPE' in dfs[key].columns:
                dfs[key] = dfs[key][dfs[key]['TASKTYPE'].isin(selected_tasktypes)]

# Filtre spécifique pour WP_TYP dans le dataset 'performance'
selected_wp_types = []
if not dfs['performance'].empty and 'WP_TYP' in dfs['performance'].columns:
    all_wp_types = dfs['performance']['WP_TYP'].dropna().unique().tolist()
    selected_wp_types = st.sidebar.multiselect(
        "Sélectionner des Types de Processus de Travail (Performance)",
        options=sorted(all_wp_types),
        default=[]
    )
    if selected_wp_types:
        dfs['performance'] = dfs['performance'][dfs['performance']['WP_TYP'].isin(selected_wp_types)]

# --- Fonction de rappel pour la synchronisation des onglets et de la radio ---
def set_active_section_from_radio():
    # Cette fonction est appelée quand la sélection de la radio change
    st.session_state.current_section_label = st.session_state.sidebar_radio_selection

# --- Navigation Rapide (Sidebar Radio) ---
st.sidebar.header("Navigation Rapide")
# Déterminer l'index initial de la radio pour qu'il corresponde à l'onglet actif
try:
    initial_radio_index = tab_titles.index(st.session_state.current_section_label)
except ValueError:
    initial_radio_index = 0 # Fallback si le label n'est pas trouvé

selected_radio_label = st.sidebar.radio(
    "Accéder à la section :",
    tab_titles,
    index=initial_radio_index,
    key="sidebar_radio_selection", # Clé unique pour le widget
    on_change=set_active_section_from_radio # Callback pour mettre à jour l'état
)

# --- Barre de navigation rapide (Onglets) ---
# Création des onglets.
# Le `st.tabs` est affiché, et le contenu est rendu en fonction de `st.session_state.current_section_label`.
# Note : Le *visuel* de l'onglet actif dans la barre d'onglets ne changera pas automatiquement si c'est la radio qui a initié le changement.
# Cependant, le *contenu* affiché sera correct et la navigation se fera en un clic.

tabs = st.tabs(tab_titles)

# Vérifier si au moins une source de données a été chargée pour afficher le dashboard
if all(df.empty for df in dfs.values()):
    st.error("Aucune source de données n'a pu être chargée. Le dashboard ne peut pas s'afficher. Veuillez vérifier les chemins et les fichiers.")
else:
    # Boucle pour afficher le contenu de la section active
    for i, tab_label in enumerate(tab_titles):
        # N'afficher le contenu que si c'est la section actuellement sélectionnée
        if st.session_state.current_section_label == tab_label:
            with tabs[i]: # Utiliser l'objet tab pour le contexte
                if tab_label == "Analyse Mémoire":
                    # --- Onglet 1: Analyse Mémoire (memory_final_cleaned_clean.xlsx) ---
                    st.header("🧠 Analyse de l'Utilisation Mémoire")
                    st.markdown("Cette section fournit des insights détaillés sur la consommation mémoire de votre système SAP.")
                    df_mem = dfs['memory'].copy() # Work with a copy after global filters

                    if not df_mem.empty:
                        with st.expander("🔬 Données Mémoire Filtrées (Aperçu pour débogage)"):
                            st.dataframe(df_mem.head())
                            st.write(f"Nombre de lignes après filtres globaux: {len(df_mem)}")
                            st.write(f"Colonnes disponibles: {df_mem.columns.tolist()}")

                        st.subheader("Top 10 Utilisateurs par Utilisation Mémoire (USEDBYTES)")
                        st.markdown("Ce graphique identifie les comptes utilisateurs qui consomment le plus de mémoire, vous aidant à cibler les optimisations.")
                        required_cols = ['ACCOUNT', 'USEDBYTES', 'MAXBYTES', 'PRIVSUM']
                        if all(col in df_mem.columns for col in required_cols):
                            if df_mem['USEDBYTES'].sum() > 0:
                                top_users_mem = df_mem.groupby('ACCOUNT')[required_cols].sum().nlargest(10, 'USEDBYTES')
                                fig_top_users_mem = px.bar(top_users_mem.reset_index(),
                                                           x='ACCOUNT', y='USEDBYTES',
                                                           title="Top 10 Comptes par USEDBYTES Total",
                                                           labels={'USEDBYTES': 'Utilisation Mémoire (Octets)', 'ACCOUNT': 'Compte Utilisateur'},
                                                           hover_data=['MAXBYTES', 'PRIVSUM'],
                                                           color='USEDBYTES', color_continuous_scale=px.colors.sequential.Plasma)
                                st.plotly_chart(fig_top_users_mem, use_container_width=True)
                            else:
                                st.info("La colonne 'USEDBYTES' est présente mais sa somme est zéro/vide après filtrage, impossible de générer le graphique 'Top 10 Utilisateurs par Utilisation Mémoire'.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_mem.columns.tolist()}")

                        st.subheader("Moyenne de USEDBYTES par Client (ACCOUNT)")
                        st.markdown("Ce graphique montre la consommation moyenne de mémoire par client SAP, utile pour l'analyse des tendances par client.")
                        required_cols = ['ACCOUNT', 'USEDBYTES']
                        if all(col in df_mem.columns for col in required_cols):
                            if df_mem['USEDBYTES'].sum() > 0:
                                df_mem_account_clean = df_mem[df_mem['ACCOUNT'] != 'Compte Inconnu'].copy()
                                
                                if not df_mem_account_clean.empty:
                                    df_mem_account_clean['ACCOUNT_DISPLAY'] = df_mem_account_clean['ACCOUNT'].astype(str)

                                    account_counts = df_mem_account_clean['ACCOUNT_DISPLAY'].nunique()
                                    if account_counts > 6:
                                        top_accounts = df_mem_account_clean['ACCOUNT_DISPLAY'].value_counts().nlargest(6).index
                                        df_mem_account_filtered_for_plot = df_mem_account_clean.loc[df_mem_account_clean['ACCOUNT_DISPLAY'].isin(top_accounts)].copy()
                                    else:
                                        df_mem_account_filtered_for_plot = df_mem_account_clean.copy()

                                    avg_mem_account = df_mem_account_filtered_for_plot.groupby('ACCOUNT_DISPLAY')['USEDBYTES'].mean().sort_values(ascending=False)
                                    if not avg_mem_account.empty and avg_mem_account.sum() > 0:
                                        fig_avg_mem_account = px.bar(avg_mem_account.reset_index(),
                                                                x='ACCOUNT_DISPLAY', y='USEDBYTES',
                                                                title="Moyenne de USEDBYTES par Client SAP (Top 6 ou tous)",
                                                                labels={'USEDBYTES': 'Moyenne USEDBYTES (Octets)', 'ACCOUNT_DISPLAY': 'Client SAP'},
                                                                color='USEDBYTES', color_continuous_scale=px.colors.sequential.Viridis)
                                        fig_avg_mem_account.update_xaxes(type='category') 
                                        st.plotly_chart(fig_avg_mem_account, use_container_width=True)
                                    else:
                                        st.info("Pas de données valides pour la moyenne de USEDBYTES par Client SAP après filtrage (peut-être tous 'Compte Inconnu' ou USEDBYTES est zéro).")
                                else:
                                    st.info("Aucune donnée valide pour les clients (ACCOUNT) après filtrage pour le graphique 'Moyenne de USEDBYTES par Client'.")
                            else:
                                st.info("La colonne 'USEDBYTES' est présente mais sa somme est zéro/vide après filtrage, impossible de générer le graphique 'Moyenne de USEDBYTES par Client'.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_mem.columns.tolist()}")

                        st.subheader("Distribution de l'Utilisation Mémoire (USEDBYTES) - Courbe de Densité")
                        st.markdown("Cette courbe montre la répartition de l'utilisation mémoire, aidant à identifier les pics ou les anomalies et la normalité de la consommation.")
                        required_col = 'USEDBYTES'
                        if required_col in df_mem.columns:
                            if df_mem[required_col].sum() > 0 and df_mem[required_col].nunique() > 1:
                                fig_dist_mem = ff.create_distplot([df_mem[required_col].dropna()], [required_col],
                                                                 bin_size=df_mem[required_col].std()/5 if df_mem[required_col].std() > 0 else 1,
                                                                 show_rug=False, show_hist=False)
                                fig_dist_mem.update_layout(title_text="Distribution de l'Utilisation Mémoire (USEDBYTES) - Courbe de Densité",
                                                           xaxis_title='Utilisation Mémoire (Octets)',
                                                           yaxis_title='Densité')
                                fig_dist_mem.data[0].line.color = 'lightcoral'
                                st.plotly_chart(fig_dist_mem, use_container_width=True)
                            else:
                                st.info(f"La colonne '{required_col}' est présente mais sa somme est zéro/vide ou contient des valeurs uniques après filtrage, impossible de créer une courbe de densité.")
                        else:
                            st.info(f"Colonne '{required_col}' manquante pour ce graphique. Colonnes disponibles: {df_mem.columns.tolist()}")

                        st.subheader("Tendance Moyenne USEDBYTES par Heure")
                        st.markdown("Visualisez l'évolution de la consommation moyenne de mémoire au cours de la journée pour détecter les schémas d'utilisation.")
                        required_cols = ['FULL_DATETIME', 'USEDBYTES']
                        if all(col in df_mem.columns for col in required_cols):
                            if pd.api.types.is_datetime64_any_dtype(df_mem['FULL_DATETIME']) and not df_mem['FULL_DATETIME'].isnull().all() and df_mem['USEDBYTES'].sum() > 0:
                                hourly_mem_usage = df_mem.set_index('FULL_DATETIME')['USEDBYTES'].resample('H').mean().dropna()
                                if not hourly_mem_usage.empty:
                                    fig_hourly_mem = px.line(hourly_mem_usage.reset_index(), x='FULL_DATETIME', y='USEDBYTES',
                                                             title="Tendance Moyenne USEDBYTES par Heure",
                                                             labels={'FULL_DATETIME': 'Heure', 'USEDBYTES': 'Moyenne USEDBYTES'},
                                                             color_discrete_sequence=['purple'])
                                    fig_hourly_mem.update_xaxes(dtick="H1", tickformat="%H:%M")
                                    st.plotly_chart(fig_hourly_mem, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour la tendance horaire de USEDBYTES après filtrage.")
                            else:
                                st.info("Données de date/heure ('FULL_DATETIME') ou de mémoire ('USEDBYTES') insuffisantes/invalides ou USEDBYTES total est zéro/vide après filtrage pour la tendance horaire.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_mem.columns.tolist()}")
                        
                        st.subheader("Comparaison des Métriques Mémoire (USEDBYTES, MAXBYTES, PRIVSUM) par Compte Utilisateur")
                        st.markdown("Comparez les différentes métriques d'utilisation mémoire (utilisée, maximale, privée) pour les principaux comptes utilisateurs afin d'identifier les profils de consommation.")
                        mem_metrics_cols = ['USEDBYTES', 'MAXBYTES', 'PRIVSUM']
                        required_cols = ['ACCOUNT'] + mem_metrics_cols
                        if all(col in df_mem.columns for col in required_cols):
                            if df_mem[mem_metrics_cols].sum().sum() > 0:
                                account_mem_summary = df_mem.groupby('ACCOUNT')[mem_metrics_cols].sum().nlargest(10, 'USEDBYTES').reset_index()
                                
                                if not account_mem_summary.empty and account_mem_summary[mem_metrics_cols].sum().sum() > 0:
                                    fig_mem_comparison = px.bar(account_mem_summary,
                                                                 x='ACCOUNT', y=mem_metrics_cols,
                                                                 title="Comparaison des Métriques Mémoire par Compte Utilisateur (Top 10 USEDBYTES)",
                                                                 labels={'value': 'Quantité (Octets)', 'variable': 'Métrique Mémoire', 'ACCOUNT': 'Compte Utilisateur'},
                                                                 barmode='group',
                                                                 color_discrete_sequence=px.colors.qualitative.Pastel)
                                    st.plotly_chart(fig_mem_comparison, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour la comparaison des métriques mémoire par compte utilisateur après filtrage.")
                            else:
                                st.info("Les sommes des colonnes de métriques mémoire ('USEDBYTES', 'MAXBYTES', 'PRIVSUM') sont zéro/vides après filtrage pour la comparaison.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_mem.columns.tolist()}")

                        st.subheader("Top Types de Tâches (TASKTYPE) par Utilisation Mémoire (USEDBYTES)")
                        st.markdown("Découvrez quels types de tâches sont les plus gourmands en mémoire, ce qui peut guider les efforts d'optimisation.")
                        required_cols = ['TASKTYPE', 'USEDBYTES']
                        if all(col in df_mem.columns for col in required_cols):
                            if df_mem['USEDBYTES'].sum() > 0:
                                top_tasktype_mem = df_mem.groupby('TASKTYPE')['USEDBYTES'].sum().nlargest(3).reset_index()
                                if not top_tasktype_mem.empty and top_tasktype_mem['USEDBYTES'].sum() > 0:
                                    fig_top_tasktype_mem = px.bar(top_tasktype_mem,
                                                                  x='TASKTYPE', y='USEDBYTES',
                                                                  title="Top 3 Types de Tâches par Utilisation Mémoire (USEDBYTES)",
                                                                  labels={'USEDBYTES': 'Utilisation Mémoire Totale (Octets)', 'TASKTYPE': 'Type de Tâche'},
                                                                  color='USEDBYTES', color_continuous_scale=px.colors.sequential.Greys)
                                    st.plotly_chart(fig_top_tasktype_mem, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour les Top Types de Tâches par Utilisation Mémoire après filtrage.")
                            else:
                                st.info("La colonne 'USEDBYTES' est présente mais sa somme est zéro/vide après filtrage pour les types de tâches mémoire.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_mem.columns.tolist()}")
                    else:
                        st.warning("Données mémoire non disponibles ou filtrées à vide. Veuillez vérifier les filtres globaux ou le fichier source 'memory_final_cleaned_clean.xlsx'.")

                elif tab_label == "Transactions Utilisateurs":
                    # --- Onglet 2: Transactions Utilisateurs (USERTCODE_cleaned.xlsx) ---
                    st.header("👤 Analyse des Transactions Utilisateurs")
                    st.markdown("Cette section analyse les performances et les comportements des transactions effectuées par les utilisateurs.")
                    df_user = dfs['usertcode'].copy() # Work with a copy after global filters

                    if not df_user.empty:
                        with st.expander("🔬 Données Utilisateurs Filtrées (Aperçu pour débogage)"):
                            st.dataframe(df_user.head())
                            st.write(f"Nombre de lignes après filtres globaux: {len(df_user)}")
                            st.write(f"Colonnes disponibles: {df_user.columns.tolist()}")

                        st.subheader("Top Types de Tâches (TASKTYPE) par Temps de Réponse Moyen")
                        st.markdown("Identifie les types de tâches qui ont le temps de réponse moyen le plus élevé, signalant des potentiels goulots d'étranglement.")
                        required_cols = ['TASKTYPE', 'RESPTI']
                        if all(col in df_user.columns for col in required_cols):
                            if df_user['RESPTI'].sum() > 0:
                                top_tasktype_resp = df_user.groupby('TASKTYPE')['RESPTI'].mean().nlargest(6).sort_values(ascending=False) / 1000.0
                                if not top_tasktype_resp.empty:
                                    fig_top_tasktype_resp = px.bar(top_tasktype_resp.reset_index(),
                                                                   x='TASKTYPE', y='RESPTI',
                                                                   title="Top 6 TASKTYPE par Temps de Réponse Moyen (s)",
                                                                   labels={'RESPTI': 'Temps de Réponse Moyen (s)', 'TASKTYPE': 'Type de Tâche'},
                                                                   color='RESPTI', color_continuous_scale=px.colors.sequential.Oranges)
                                    st.plotly_chart(fig_top_tasktype_resp, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour les Top Types de Tâches par Temps de Réponse Moyen après filtrage.")
                            else:
                                st.info("La colonne 'RESPTI' est présente mais sa somme est zéro/vide après filtrage pour 'Top Types de Tâches par Temps de Réponse Moyen'.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_user.columns.tolist()}")
                        
                        transaction_types = ['COUNT', 'DCOUNT', 'UCOUNT', 'BCOUNT', 'ECOUNT', 'SCOUNT']
                        available_trans_types = [col for col in transaction_types if col in df_user.columns]

                        st.subheader("Nombre Total de Transactions par Type")
                        st.markdown("Affiche le volume total de chaque type de transaction (ex: Dialog, Update, Background), donnant un aperçu de l'activité système.")
                        if available_trans_types:
                            if not df_user.empty and df_user[available_trans_types].sum().sum() > 0:
                                transactions_sum = df_user[available_trans_types].sum().sort_values(ascending=False)
                                if not transactions_sum.empty and transactions_sum.sum() > 0:
                                    fig_transactions_sum = px.bar(transactions_sum.reset_index(),
                                                                  x='index', y=0,
                                                                  title="Nombre Total de Transactions par Type",
                                                                  labels={'index': 'Type de Transaction', '0': 'Nombre Total'},
                                                                  color=0, color_continuous_scale=px.colors.sequential.Blues)
                                    st.plotly_chart(fig_transactions_sum, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour le nombre total de transactions par type après filtrage.")
                            else:
                                st.info("Les sommes des colonnes de type de transaction sont zéro/vides après filtrage pour 'Nombre Total de Transactions par Type'.")
                        else:
                            st.info(f"Aucune des colonnes de type de transaction attendues ({', '.join(transaction_types)}) n'est disponible. Colonnes disponibles: {df_user.columns.tolist()}")
                        
                        st.subheader("Top Comptes Utilisateurs et Opérations Associées aux Longues Durées")
                        st.markdown("Identifie les utilisateurs et les opérations qui contribuent le plus aux temps de réponse élevés, facilitant le dépannage des performances.")
                        required_cols = ['RESPTI', 'ACCOUNT', 'ENTRY_ID']
                        if all(col in df_user.columns for col in required_cols):
                            if df_user['RESPTI'].sum() > 0:
                                response_time_threshold = df_user['RESPTI'].quantile(0.90)
                                long_duration_users = df_user[df_user['RESPTI'] > response_time_threshold]

                                if not long_duration_users.empty:
                                    st.write(f"Seuil de temps de réponse élevé (90ème percentile) : {response_time_threshold / 1000:.2f} secondes")
                                    
                                    st.markdown("**Top Comptes (ACCOUNT) avec temps de réponse élevé :**")
                                    top_accounts_long_resp = long_duration_users['ACCOUNT'].value_counts().nlargest(10).reset_index()
                                    top_accounts_long_resp.columns = ['ACCOUNT', 'Occurrences']
                                    fig_top_acc_long = px.bar(top_accounts_long_resp, x='ACCOUNT', y='Occurrences',
                                                              title="Top Comptes avec Temps de Réponse Élevé",
                                                              color='Occurrences', color_continuous_scale=px.colors.sequential.Greens)
                                    st.plotly_chart(fig_top_acc_long, use_container_width=True)
                                    
                                    st.markdown("**Top Opérations (ENTRY_ID) avec temps de réponse élevé :**")
                                    top_entry_id_long_resp = long_duration_users['ENTRY_ID'].value_counts().nlargest(10).reset_index()
                                    top_entry_id_long_resp.columns = ['ENTRY_ID', 'Occurrences']
                                    fig_top_entry_long = px.bar(top_entry_id_long_resp, x='ENTRY_ID', y='Occurrences',
                                                                title="Top ENTRY_ID avec Temps de Réponse Élevé",
                                                                color='Occurrences', color_continuous_scale=px.colors.sequential.Teal)
                                    st.plotly_chart(fig_top_entry_long, use_container_width=True)
                                else:
                                    st.info("Aucune transaction avec un temps de réponse élevé (au-dessus du 90ème percentile) après filtrage.")
                            else:
                                st.info("La colonne 'RESPTI' est présente mais sa somme est zéro/vide après filtrage pour l'analyse des longues durées.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_user.columns.tolist()}")
                        
                        st.subheader("Tendance du Temps de Réponse Moyen par Heure")
                        st.markdown("Suivez l'évolution du temps de réponse moyen des transactions au cours de la journée pour identifier les périodes de pointe.")
                        required_cols = ['FULL_DATETIME', 'RESPTI']
                        if all(col in df_user.columns for col in required_cols):
                            if pd.api.types.is_datetime64_any_dtype(df_user['FULL_DATETIME']) and not df_user['FULL_DATETIME'].isnull().all() and df_user['RESPTI'].sum() > 0:
                                hourly_resp_time = df_user.set_index('FULL_DATETIME')['RESPTI'].resample('H').mean().dropna() / 1000.0
                                if not hourly_resp_time.empty:
                                    fig_hourly_resp = px.line(hourly_resp_time.reset_index(), x='FULL_DATETIME', y='RESPTI',
                                                              title="Tendance du Temps de Réponse Moyen par Heure (s)",
                                                              labels={'FULL_DATETIME': 'Heure', 'RESPTI': 'Temps de Réponse Moyen (s)'},
                                                              color_discrete_sequence=['red'])
                                    fig_hourly_resp.update_xaxes(dtick="H1", tickformat="%H:%M")
                                    st.plotly_chart(fig_hourly_resp, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour la tendance horaire du temps de réponse après filtrage.")
                            else:
                                st.info("Colonnes 'FULL_DATETIME' ou 'RESPTI' insuffisantes/invalides ou RESPTI total est zéro/vide après filtrage pour la tendance.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_user.columns.tolist()}")
                        
                        st.subheader("Corrélation entre Temps de Réponse et Temps CPU")
                        st.markdown("""
                            Ce graphique explore la relation entre le temps de réponse total d'une transaction et le temps CPU qu'elle consomme.
                            * Chaque point représente une transaction.
                            * Une tendance à la hausse (points allant de bas à gauche vers haut à droite) suggère que plus une transaction utilise de CPU, plus son temps de réponse est long.
                            * Les points éloignés de la tendance peuvent indiquer d'autres facteurs influençant le temps de réponse (par exemple, des attentes E/S, des verrous, etc.).
                            * La couleur des points indique le type de tâche, aidant à identifier les catégories de transactions qui se comportent différemment.
                            """)
                        
                        hover_data_cols = []
                        if 'ACCOUNT' in df_user.columns:
                            hover_data_cols.append('ACCOUNT')
                        if 'TASKTYPE' in df_user.columns:
                            hover_data_cols.append('TASKTYPE')
                        if 'ENTRY_ID' in df_user.columns:
                            hover_data_cols.append('ENTRY_ID')

                        required_cols = ['RESPTI', 'CPUTI']
                        if all(col in df_user.columns for col in required_cols):
                            if df_user['CPUTI'].sum() > 0 and df_user['RESPTI'].sum() > 0:
                                fig_resp_cpu_corr = px.scatter(df_user, x='CPUTI', y='RESPTI',
                                                               title="Temps de Réponse vs. Temps CPU",
                                                               labels={'CPUTI': 'Temps CPU (ms)', 'RESPTI': 'Temps de Réponse (ms)'},
                                                               hover_data=hover_data_cols,
                                                               color='TASKTYPE' if 'TASKTYPE' in df_user.columns else None,
                                                               log_x=True,
                                                               log_y=True,
                                                               trendline="ols",
                                                               color_discrete_sequence=px.colors.qualitative.Alphabet)
                                st.plotly_chart(fig_resp_cpu_corr, use_container_width=True)
                            else:
                                st.info("Les sommes des colonnes 'RESPTI' ou 'CPUTI' sont zéro/vides après filtrage pour la corrélation.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_user.columns.tolist()}")
                        
                        io_detailed_metrics_counts = ['READDIRCNT', 'READSEQCNT', 'CHNGCNT', 'PHYREADCNT']
                        required_cols = ['TASKTYPE'] + io_detailed_metrics_counts
                        if all(col in df_user.columns for col in required_cols):
                            if df_user[io_detailed_metrics_counts].sum().sum() > 0:
                                st.subheader("Total des Opérations de Lecture/Écriture (Comptes) par Type de Tâche")
                                st.markdown("""
                                    Ce graphique présente le total des opérations de lecture et d'écriture par type de tâche.
                                    * **READDIRCNT** : Nombre de lectures directes (accès spécifiques à des blocs de données).
                                    * **READSEQCNT** : Nombre de lectures séquentielles (accès consécutifs aux données).
                                    * **CHNGCNT** : Nombre de changements (écritures) d'enregistrements.
                                    * **PHYREADCNT** : Nombre total de lectures physiques (lectures réelles depuis le disque).
                                    Ces métriques sont cruciales pour comprendre l'intensité des interactions de chaque tâche avec la base de données ou le système de fichiers.
                                    """)
                                df_io_counts = df_user.groupby('TASKTYPE')[io_detailed_metrics_counts].sum().nlargest(10, 'PHYREADCNT').reset_index()
                                if not df_io_counts.empty and df_io_counts[io_detailed_metrics_counts].sum().sum() > 0:
                                    fig_io_counts = px.bar(df_io_counts, x='TASKTYPE', y=io_detailed_metrics_counts,
                                                           title="Total des Opérations de Lecture/Écriture (Comptes) par Type de Tâche (Top 10)",
                                                           labels={'value': 'Nombre d\'Opérations', 'variable': 'Type d\'Opération', 'TASKTYPE': 'Type de Tâche'},
                                                           barmode='group', color_discrete_sequence=px.colors.sequential.Blues)
                                    st.plotly_chart(fig_io_counts, use_container_width=True)
                                else:
                                    st.info("Données insuffisantes pour les opérations de lecture/écriture (comptes) après filtrage.")
                            else:
                                st.info("Les sommes des colonnes d'opérations de lecture/écriture sont zéro/vides après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_user.columns.tolist()}")

                        io_detailed_metrics_buffers_records = ['READDIRBUF', 'READDIRREC', 'READSEQBUF', 'READSEQREC', 'CHNGREC', 'PHYCHNGREC']
                        required_cols = ['TASKTYPE'] + io_detailed_metrics_buffers_records
                        if all(col in df_user.columns for col in required_cols):
                            if df_user[io_detailed_metrics_buffers_records].sum().sum() > 0:
                                st.subheader("Utilisation des Buffers et Enregistrements par Type de Tâche")
                                st.markdown("""
                                    Ce graphique détaille l'efficacité des opérations d'E/S en montrant l'utilisation des tampons et le nombre d'enregistrements traités.
                                    * **READDIRBUF** : Nombre de lectures directes via buffer.
                                    * **READDIRREC** : Nombre d'enregistrements lus directement.
                                    * **READSEQBUF** : Nombre de lectures séquentielles via buffer.
                                    * **READSEQREC** : Nombre d'enregistrements lus séquentiellement.
                                    * **CHNGREC** : Nombre d'enregistrements modifiés.
                                    * **PHYCHNGREC** : Nombre total d'enregistrements physiquement modifiés.
                                    Ces métriques aident à évaluer si les tâches tirent parti de la mise en cache (buffers) et l'ampleur des données traitées.
                                    """)
                                df_io_buffers_records = df_user.groupby('TASKTYPE')[io_detailed_metrics_buffers_records].sum().nlargest(10, 'READDIRREC').reset_index()
                                if not df_io_buffers_records.empty and df_io_buffers_records[io_detailed_metrics_buffers_records].sum().sum() > 0:
                                    fig_io_buffers_records = px.bar(df_io_buffers_records, x='TASKTYPE', y=io_detailed_metrics_buffers_records,
                                                                     title="Utilisation des Buffers et Enregistrements par Type de Tâche (Top 10)",
                                                                     labels={'value': 'Nombre', 'variable': 'Métrique', 'TASKTYPE': 'Type de Tâche'},
                                                                     barmode='group', color_discrete_sequence=px.colors.sequential.Plasma)
                                    st.plotly_chart(fig_io_buffers_records, use_container_width=True)
                                else:
                                    st.info("Données insuffisantes pour l'utilisation des buffers et enregistrements après filtrage.")
                            else:
                                st.info("Les sommes des colonnes d'utilisation des buffers/enregistrements sont zéro/vides après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_user.columns.tolist()}")


                        comm_metrics_filtered = ['DSQLCNT', 'SLI_CNT']
                        required_cols = ['TASKTYPE'] + comm_metrics_filtered
                        if all(col in df_user.columns for col in required_cols):
                            if df_user[comm_metrics_filtered].sum().sum() > 0:
                                st.subheader("Analyse des Communications et Appels Système par Type de Tâche (DSQLCNT et SLI_CNT)")
                                st.markdown("""
                                    Ce graphique se concentre sur deux métriques clés pour les interactions des tâches avec d'autres systèmes :
                                    * **DSQLCNT** : Nombre d'appels SQL dynamiques (requêtes de base de données générées dynamiquement). Un nombre élevé peut indiquer une forte interaction avec la base de données.
                                    * **SLI_CNT** : Nombre d'appels SLI (System Level Interface). Ces appels représentent les interactions de bas niveau avec le système d'exploitation ou d'autres composants système.
                                    Ces métriques sont essentielles pour diagnostiquer les problèmes de communication ou les dépendances externes.
                                    """)
                                df_comm_metrics = df_user.groupby('TASKTYPE')[comm_metrics_filtered].sum().nlargest(4, 'DSQLCNT').reset_index()
                                if not df_comm_metrics.empty and df_comm_metrics[comm_metrics_filtered].sum().sum() > 0:
                                    fig_comm_metrics = px.bar(df_comm_metrics, x='TASKTYPE', y=comm_metrics_filtered,
                                                              title="Communications et Appels Système par Type de Tâche (Top 4)",
                                                              labels={'value': 'Nombre / Temps (ms)', 'variable': 'Métrique', 'TASKTYPE': 'Type de Tâche'},
                                                              barmode='group', color_discrete_sequence=px.colors.qualitative.Bold)
                                    st.plotly_chart(fig_comm_metrics, use_container_width=True)
                                else:
                                    st.info("Données insuffisantes pour les métriques de communication et d'appels système après filtrage.")
                            else:
                                st.info("Les sommes des colonnes de communication ('DSQLCNT', 'SLI_CNT') sont zéro/vides après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_user.columns.tolist()}")
                    else:
                        st.warning("Données utilisateurs non disponibles ou filtrées à vide. Veuillez vérifier les filtres globaux ou le fichier source 'USERTCODE_cleaned.xlsx'.")

                elif tab_label == "Statistiques Horaires":
                    # --- Onglet 3: Statistiques Horaires (Times_final_cleaned_clean.xlsx) ---
                    st.header("⏰ Statistiques Horaires du Système")
                    st.markdown("Cette section présente l'activité du système et les performances agrégées par tranches horaires.")
                    df_times_data = dfs['times'].copy() # Work with a copy after global filters
                        
                    if not df_times_data.empty:
                        with st.expander("🔬 Données Horaires Filtrées (Aperçu pour débogage)"):
                            st.dataframe(df_times_data.head())
                            st.write(f"Nombre de lignes après filtres globaux: {len(df_times_data)}")
                            st.write(f"Colonnes disponibles: {df_times_data.columns.tolist()}")

                        st.subheader("Évolution du Nombre Total d'Appels Physiques (PHYCALLS) par Tranche Horaire")
                        st.markdown("Ce graphique montre l'activité physique du système (appels I/O) par tranche horaire, révélant les périodes d'intense activité.")
                        required_cols = ['TIME', 'PHYCALLS']
                        if all(col in df_times_data.columns for col in required_cols):
                            if df_times_data['PHYCALLS'].sum() > 0:
                                df_times_data['HOUR_OF_DAY'] = df_times_data['TIME'].apply(lambda x: str(x).split(':')[0].zfill(2) if ':' in str(x) else str(x).zfill(2)[:2])
                                hourly_counts = df_times_data.groupby('HOUR_OF_DAY')['PHYCALLS'].sum().reindex([
                                    '00--06', '06--07', '07--08', '08--09', '09--10', '10--11', '11--12', '12--13',
                                    '13--14', '14--15', '15--16', '16--17', '17--18', '18--19', '19--20', '20--21',
                                    '21--22', '22--23', '23--00'
                                ], fill_value=0)
                                if not hourly_counts.empty and hourly_counts.sum() > 0:
                                    fig_phycalls = px.line(hourly_counts.reset_index(),
                                                           x='HOUR_OF_DAY', y='PHYCALLS',
                                                           title="Total Appels Physiques par Tranche Horaire",
                                                           labels={'HOUR_OF_DAY': 'Tranche Horaire', 'PHYCALLS': 'Total Appels Physiques'},
                                                           color_discrete_sequence=px.colors.sequential.Cividis,
                                                           markers=True)
                                    st.plotly_chart(fig_phycalls, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour les appels physiques par tranche horaire après filtrage.")
                            else:
                                st.info("La colonne 'PHYCALLS' est présente mais sa somme est zéro/vide après filtrage pour 'Évolution du Nombre Total d'Appels Physiques'.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_times_data.columns.tolist()}")

                        st.subheader("Top 5 Tranches Horaires les plus Chargées (Opérations d'E/S)")
                        st.markdown("Identifie les périodes de la journée avec la plus forte activité d'entrée/sortie, crucial pour la planification des ressources.")
                        io_cols = ['READDIRCNT', 'READSEQCNT', 'CHNGCNT']
                        required_cols = ['TIME'] + io_cols
                        if all(col in df_times_data.columns for col in required_cols):
                            if df_times_data[io_cols].sum().sum() > 0:
                                df_times_data['TOTAL_IO'] = df_times_data['READDIRCNT'] + df_times_data['READSEQCNT'] + df_times_data['CHNGCNT']
                                top_io_times = df_times_data.groupby('TIME')['TOTAL_IO'].sum().nlargest(5).sort_values(ascending=False)
                                if not top_io_times.empty and top_io_times.sum() > 0:
                                    fig_top_io = px.bar(top_io_times.reset_index(),
                                                        x='TIME', y='TOTAL_IO',
                                                        title="Top 5 Tranches Horaires par Total Opérations I/O",
                                                        labels={'TIME': 'Tranche Horaire', 'TOTAL_IO': 'Total Opérations I/O'},
                                                        color='TOTAL_IO', color_continuous_scale=px.colors.sequential.Inferno)
                                    st.plotly_chart(fig_top_io, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour les opérations I/O après filtrage.")
                            else:
                                st.info("Les sommes des colonnes I/O ('READDIRCNT', 'READSEQCNT', 'CHNGCNT') sont zéro/vides après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_times_data.columns.tolist()}")

                        st.subheader("Temps Moyen de Réponse / CPU / Traitement par Tranche Horaire")
                        perf_cols = ["RESPTI", "CPUTI", "PROCTI"]
                        required_cols = ['TIME'] + perf_cols
                        if all(col in df_times_data.columns for col in required_cols):
                            if df_times_data[perf_cols].sum().sum() > 0:
                                avg_times_by_hour = df_times_data.groupby("TIME")[perf_cols].mean() / 1000.0
                                avg_times_by_hour = avg_times_by_hour.reindex([
                                    '00--06', '06--07', '07--08', '08--09', '09--10', '10--11', '11--12', '12--13',
                                    '13--14', '14--15', '15--16', '16--17', '17--18', '18--19', '19--20', '20--21',
                                    '21--22', '22--23', '23--00'
                                ], fill_value=0)
                                
                                if not avg_times_by_hour.empty and avg_times_by_hour.sum().sum() > 0:
                                    fig_avg_times = px.line(avg_times_by_hour.reset_index(),
                                                            x='TIME', y=perf_cols,
                                                            title="Temps Moyen (s) par Tranche Horaire",
                                                            labels={'value': 'Temps Moyen (s)', 'variable': 'Métrique', 'TIME': 'Tranche Horaire'},
                                                            color_discrete_sequence=px.colors.qualitative.Set1,
                                                            markers=True)
                                    st.plotly_chart(fig_avg_times, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour les temps moyens après filtrage.")
                            else:
                                st.info("Les sommes des colonnes de performance ('RESPTI', 'CPUTI', 'PROCTI') sont zéro/vides après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_times_data.columns.tolist()}")
                    else:
                        st.warning("Données horaires (Times) non disponibles ou filtrées à vide. Veuillez vérifier les filtres globaux ou le fichier source 'Times_final_cleaned_clean.xlsx'.")

                elif tab_label == "Décomposition des Tâches":
                    # --- Onglet 4: Décomposition des Tâches (TASKTIMES_final_cleaned_clean.xlsx) ---
                    st.header("⚙️ Décomposition des Types de Tâches")
                    st.markdown("Cette section offre une vue détaillée de la répartition et de la performance des différents types de tâches système.")
                    df_task = dfs['tasktimes'].copy() # Work with a copy after global filters

                    if not df_task.empty:
                        with st.expander("🔬 Données des Temps de Tâches Filtrées (Aperçu pour débogage)"):
                            st.dataframe(df_task.head())
                            st.write(f"Nombre de lignes après filtres globaux: {len(df_task)}")
                            st.write(f"Colonnes disponibles: {df_task.columns.tolist()}")

                        st.subheader("Répartition des Types de Tâches (TASKTYPE)")
                        st.markdown("Ce graphique en forme de beignet montre la proportion de chaque type de tâche dans le système, offrant une vue d'ensemble de l'activité.")
                        required_cols = ['TASKTYPE', 'COUNT']
                        if all(col in df_task.columns for col in required_cols):
                            if df_task['COUNT'].sum() > 0:
                                task_counts = df_task.groupby('TASKTYPE')['COUNT'].sum().reset_index()
                                task_counts.columns = ['TASKTYPE', 'Count']
                                
                                min_count_for_pie = task_counts['Count'].sum() * 0.01
                                significant_tasks = task_counts[task_counts['Count'] >= min_count_for_pie]
                                other_tasks_count = task_counts[task_counts['Count'] < min_count_for_pie]['Count'].sum()

                                if other_tasks_count > 0:
                                    significant_tasks = pd.concat([significant_tasks, pd.DataFrame([{'TASKTYPE': 'Autres Petites Tâches', 'Count': other_tasks_count}])])

                                if not significant_tasks.empty and significant_tasks['Count'].sum() > 0:
                                    fig_task_dist = px.pie(significant_tasks, values='Count', names='TASKTYPE',
                                                           title="Répartition des Types de Tâches",
                                                           hole=0.3,
                                                           color_discrete_sequence=px.colors.sequential.RdBu)
                                    st.plotly_chart(fig_task_dist, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour la répartition des types de tâches après filtrage.")
                            else:
                                st.info("La colonne 'COUNT' est présente mais sa somme est zéro/vide après filtrage pour 'Répartition des Types de Tâches'.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_task.columns.tolist()}")

                        st.subheader("Top 10 TASKTYPE par Temps de Réponse (RESPTI) et CPU (CPUTI)")
                        st.markdown("Compare les temps de réponse et CPU moyens pour les types de tâches les plus importants, permettant d'identifier les tâches coûteuses.")
                        perf_cols_task = ['RESPTI', 'CPUTI']
                        required_cols = ['TASKTYPE'] + perf_cols_task
                        if all(col in df_task.columns for col in required_cols):
                            if df_task[perf_cols_task].sum().sum() > 0:
                                task_perf = df_task.groupby('TASKTYPE')[perf_cols_task].mean().nlargest(10, 'RESPTI') / 1000.0
                                if not task_perf.empty and task_perf.sum().sum() > 0:
                                    fig_task_perf = px.bar(task_perf.reset_index(), x='TASKTYPE', y=perf_cols_task,
                                                           title="Top 10 TASKTYPE par Temps de Réponse et CPU (s)",
                                                           labels={'value': 'Temps Moyen (s)', 'variable': 'Métrique', 'TASKTYPE': 'Type de Tâche'},
                                                           barmode='group', color_discrete_sequence=px.colors.qualitative.Bold)
                                    st.plotly_chart(fig_task_perf, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour les temps de performance des tâches après filtrage.")
                            else:
                                st.info("Les sommes des colonnes de performance ('RESPTI', 'CPUTI') sont zéro/vides après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_task.columns.tolist()}")

                        st.subheader("Décomposition des Temps d'Attente et GUI par Type de Tâche")
                        st.markdown("""
                            Ce graphique détaille où le temps est passé au-delà du traitement CPU pour les tâches.
                            * **QUEUETI (Temps d'Attente en File)** : Temps passé par la tâche en attente dans une file d'attente. Un temps élevé peut indiquer une surcharge du système ou des goulots d'étranglement.
                            * **ROLLWAITTI (Temps d'Attente de Roll-in/out)** : Temps passé par la tâche en attente de chargement ou de déchargement de la mémoire (roll-in/out).
                            * **GUITIME (Temps GUI)** : Temps passé par la tâche dans l'interface graphique utilisateur.
                            * **GUINETTIME (Temps Réseau GUI)** : Temps passé sur le réseau pour les interactions de l'interface graphique utilisateur.
                            Ces métriques aident à identifier les causes de lenteur qui ne sont pas directement liées au CPU, comme les attentes de ressources ou les problèmes réseau.
                            """)
                        wait_gui_metrics = ['QUEUETI', 'ROLLWAITTI', 'GUITIME', 'GUINETTIME']
                        required_cols = ['TASKTYPE'] + wait_gui_metrics
                        if all(col in df_task.columns for col in required_cols):
                            if df_task[wait_gui_metrics].sum().sum() > 0:
                                df_wait_gui = df_task.groupby('TASKTYPE')[wait_gui_metrics].sum().nlargest(10, 'QUEUETI').reset_index()
                                if not df_wait_gui.empty and df_wait_gui[wait_gui_metrics].sum().sum() > 0:
                                    fig_wait_gui = px.bar(df_wait_gui, x='TASKTYPE', y=wait_gui_metrics,
                                                          title="Temps d'Attente et GUI par Type de Tâche (Top 10)",
                                                          labels={'value': 'Temps (ms)', 'variable': 'Métrique de Temps', 'TASKTYPE': 'Type de Tâche'},
                                                          barmode='group', color_discrete_sequence=px.colors.qualitative.Pastel)
                                    st.plotly_chart(fig_wait_gui, use_container_width=True)
                                else:
                                    st.info("Données insuffisantes pour la décomposition des temps d'attente et GUI après filtrage.")
                            else:
                                st.info("Les sommes des colonnes d'attente/GUI sont zéro/vides après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_task.columns.tolist()}")

                        st.subheader("Analyse des Opérations d'E/S (Lectures/Écritures) par Type de Tâche")
                        st.markdown("""
                            Ce graphique fournit des détails sur les opérations d'entrée/sortie (E/S) spécifiques aux tâches.
                            * **READDIRCNT (Lectures Directes)** : Nombre de lectures directes d'enregistrements.
                            * **READSEQCNT (Lectures Séquentielles)** : Nombre de lectures séquentielles d'enregistrements.
                            * **CHNGCNT (Changements)** : Nombre de changements (écritures) d'enregistrements.
                            * **PHYREADCNT (Lectures Physiques)** : Nombre total de lectures physiques (sur le disque).
                            * **PHYCHNGREC (Changements Physiques)** : Nombre total d'enregistrements physiquement modifiés.
                            Ces métriques sont essentielles pour identifier les tâches gourmandes en E/S et évaluer l'efficacité de l'accès aux données.
                            """)
                        io_metrics_tasktimes = ['READDIRCNT', 'READSEQCNT', 'CHNGCNT', 'PHYREADCNT', 'PHYCHNGREC']
                        required_cols = ['TASKTYPE'] + io_metrics_tasktimes
                        if all(col in df_task.columns for col in required_cols):
                            if df_task[io_metrics_tasktimes].sum().sum() > 0:
                                df_io_tasktimes = df_task.groupby('TASKTYPE')[io_metrics_tasktimes].sum().nlargest(10, 'PHYREADCNT').reset_index()
                                if not df_io_tasktimes.empty and df_io_tasktimes[io_metrics_tasktimes].sum().sum() > 0:
                                    fig_io_tasktimes = px.bar(df_io_tasktimes, x='TASKTYPE', y=io_metrics_tasktimes,
                                                              title="Opérations d'E/S par Type de Tâche (Top 10)",
                                                              labels={'value': 'Nombre d\'Opérations', 'variable': 'Métrique E/S', 'TASKTYPE': 'Type de Tâche'},
                                                              barmode='group', color_discrete_sequence=px.colors.sequential.Greens)
                                    st.plotly_chart(fig_io_tasktimes, use_container_width=True)
                                else:
                                    st.info("Données insuffisantes pour l'analyse des opérations d'E/S après filtrage.")
                            else:
                                st.info("Les sommes des colonnes d'E/S sont zéro/vides après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_task.columns.tolist()}")
                    else:
                        st.warning("Données des temps de tâches non disponibles ou filtrées à vide. Veuillez vérifier les filtres globaux ou le fichier source 'TASKTIMES_final_cleaned_clean.xlsx'.")

                elif tab_label == "Insights Hitlist DB":
                    # --- Onglet 5: Insights Hitlist Database (HITLIST_DATABASE_final_cleaned_clean.xlsx) ---
                    st.header("🔍 Insights de la Base de Données Hitlist")
                    st.markdown("Cette section explore les métriques clés de performance de la base de données Hitlist, y compris les temps de réponse et les appels DB.")
                    df_hitlist = dfs['hitlist_db'].copy() # Work with a copy after global filters

                    if not df_hitlist.empty:
                        with st.expander("🔬 Données Hitlist DB Filtrées (Aperçu pour débogage)"):
                            st.dataframe(df_hitlist.head())
                            st.write(f"Nombre de lignes après filtres globaux: {len(df_hitlist)}")
                            st.write(f"Colonnes disponibles: {df_hitlist.columns.tolist()}")

                        if 'FULL_DATETIME' in df_hitlist.columns and pd.notna(df_hitlist['FULL_DATETIME'].min()) and pd.notna(df_hitlist['FULL_DATETIME'].max()):
                            st.info(f"Données affichées pour la période: "
                                    f"**{df_hitlist['FULL_DATETIME'].min().strftime('%Y-%m-%d %H:%M')}** à "
                                    f"**{df_hitlist['FULL_DATETIME'].max().strftime('%Y-%m-%d %H:%M')}**")
                        else:
                            st.info("La plage de dates pour HITLIST_DATABASE n'a pas pu être déterminée ou est vide.")

                        st.subheader("Tendance du Temps de Réponse Moyen et Temps CPU par Heure (Hitlist DB)")
                        st.markdown("Suivez l'évolution des temps de réponse et CPU moyens de la base de données au fil du temps pour identifier les pics de charge.")
                        hitlist_perf_cols = ['RESPTI', 'CPUTI']
                        required_cols = ['FULL_DATETIME'] + hitlist_perf_cols
                        if all(col in df_hitlist.columns for col in required_cols):
                            if pd.api.types.is_datetime64_any_dtype(df_hitlist['FULL_DATETIME']) and not df_hitlist['FULL_DATETIME'].isnull().all() and df_hitlist[hitlist_perf_cols].sum().sum() > 0:
                                hourly_metrics = df_hitlist.set_index('FULL_DATETIME')[hitlist_perf_cols].resample('H').mean().dropna()
                                if not hourly_metrics.empty and hourly_metrics.sum().sum() > 0:
                                    fig_hourly_perf = px.line(hourly_metrics.reset_index(), x='FULL_DATETIME', y=hitlist_perf_cols,
                                                              title="Tendance Horaire du Temps de Réponse et CPU (s)",
                                                              labels={'FULL_DATETIME': 'Heure', 'value': 'Temps Moyen (s)', 'variable': 'Métrique'},
                                                              color_discrete_sequence=px.colors.qualitative.Dark2)
                                    fig_hourly_perf.update_xaxes(dtick="H1", tickformat="%H:%M")
                                    st.plotly_chart(fig_hourly_perf, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour la tendance horaire de performance Hitlist DB après filtrage.")
                            else:
                                st.info("Colonnes 'FULL_DATETIME', 'RESPTI' ou 'CPUTI' insuffisantes/invalides dans Hitlist DB ou leurs totaux sont zéro/vide.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_hitlist.columns.tolist()}")

                        st.subheader("Top 10 Rapports (REPORT) par Appels Base de Données (DBCALLS)")
                        st.markdown("Identifie les rapports SAP qui génèrent le plus d'appels à la base de données, indiquant les zones de forte interaction avec la DB.")
                        required_cols = ['REPORT', 'DBCALLS']
                        if all(col in df_hitlist.columns for col in required_cols):
                            if df_hitlist['DBCALLS'].sum() > 0:
                                top_reports_dbcalls = df_hitlist.groupby('REPORT')['DBCALLS'].sum().nlargest(10)
                                if not top_reports_dbcalls.empty and top_reports_dbcalls.sum() > 0:
                                    fig_top_reports_db = px.bar(top_reports_dbcalls.reset_index(), x='REPORT', y='DBCALLS',
                                                                title="Top 10 Rapports par Total Appels DB",
                                                                labels={'REPORT': 'Rapport', 'DBCALLS': 'Total Appels DB'},
                                                                color='DBCALLS', color_continuous_scale=px.colors.sequential.dense)
                                    st.plotly_chart(fig_top_reports_db, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour les Top 10 Rapports par Appels DB Hitlist après filtrage.")
                            else:
                                st.info("La colonne 'DBCALLS' est présente mais sa somme est zéro/vide après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_hitlist.columns.tolist()}")

                        st.subheader("Temps Moyen de Traitement (PROCTI) par Top 5 Types de Tâches (TASKTYPE)")
                        st.markdown("Analyse le temps de traitement moyen par type de tâche pour les tâches les plus fréquentes dans la base de données Hitlist.")
                        required_cols = ['TASKTYPE', 'PROCTI']
                        if all(col in df_hitlist.columns for col in required_cols):
                            if df_hitlist['PROCTI'].sum() > 0:
                                top_5_tasktypes = df_hitlist['TASKTYPE'].value_counts().nlargest(5).index.tolist()
                                df_filtered_tasktype = df_hitlist.loc[df_hitlist['TASKTYPE'].isin(top_5_tasktypes)].copy()
                                
                                if not df_filtered_tasktype.empty:
                                    avg_procti_by_tasktype = df_filtered_tasktype.groupby('TASKTYPE')['PROCTI'].mean().sort_values(ascending=False) / 1000.0
                                    if not avg_procti_by_tasktype.empty and avg_procti_by_tasktype.sum() > 0:
                                        fig_procti_bar = px.bar(avg_procti_by_tasktype.reset_index(), x='TASKTYPE', y='PROCTI',
                                                                title="Temps Moyen de Traitement (s) par Top 5 TASKTYPE",
                                                                labels={'TASKTYPE': 'Type de Tâche', 'PROCTI': 'Temps Moyen de Traitement (s)'},
                                                                color='PROCTI', color_continuous_scale=px.colors.sequential.Sunset)
                                        st.plotly_chart(fig_procti_bar, use_container_width=True)
                                    else:
                                        st.info("Pas de données valides pour le temps moyen de traitement par TASKTYPE après filtrage.")
                                else:
                                    st.info("Pas de données pour les Top 5 TASKTYPE pour le graphique (Hitlist DB) après filtrage.")
                            else:
                                st.info("La colonne 'PROCTI' est présente mais sa somme est zéro/vide après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_hitlist.columns.tolist()}")
                    else:
                        st.warning("Données Hitlist Database non disponibles ou filtrées à vide. Veuillez vérifier les filtres globaux ou le fichier source 'HITLIST_DATABASE_final_cleaned_clean.xlsx'.")

                elif tab_label == "Performance des Processus de Travail":
                    # --- Onglet 6: Performance des Processus de Travail (AL_GET_PERFORMANCE) ---
                    st.header("⚡ Performance des Processus de Travail")
                    st.markdown("Cette section fournit des informations sur la performance des processus de travail SAP, y compris l'utilisation du CPU et les redémarrages.")
                    df_perf = dfs['performance'].copy() # Work with a copy after global filters

                    if not df_perf.empty:
                        with st.expander("🔬 Données de Performance Filtrées (Aperçu pour débogage)"):
                            st.dataframe(df_perf.head())
                            st.write(f"Nombre de lignes après filtres globaux: {len(df_perf)}")
                            st.write(f"Colonnes disponibles: {df_perf.columns.tolist()}")

                        st.subheader("Distribution du Temps CPU des Processus de Travail (en secondes)")
                        st.markdown("Visualise la répartition du temps CPU consommé par les processus de travail, aidant à détecter les anomalies de performance.")
                        required_col = 'WP_CPU_SECONDS'
                        if required_col in df_perf.columns:
                            if df_perf[required_col].sum() > 0 and df_perf[required_col].nunique() > 1:
                                fig_cpu_dist = ff.create_distplot([df_perf[required_col].dropna()], [required_col],
                                                                  bin_size=df_perf[required_col].std()/5 if df_perf[required_col].std() > 0 else 1,
                                                                  show_rug=False, show_hist=False)
                                fig_cpu_dist.update_layout(title_text="Distribution du Temps CPU des Processus de Travail",
                                                           xaxis_title='Temps CPU (secondes)',
                                                           yaxis_title='Densité')
                                fig_cpu_dist.data[0].line.color = 'darkblue'
                                st.plotly_chart(fig_cpu_dist, use_container_width=True)
                            else:
                                st.info(f"La colonne '{required_col}' est présente mais sa somme est zéro/vide ou contient des valeurs uniques après filtrage, impossible de créer une courbe de densité.")
                        else:
                            st.info(f"Colonne '{required_col}' manquante pour ce graphique. Colonnes disponibles: {df_perf.columns.tolist()}")

                        st.subheader("Répartition des Processus de Travail par Statut (WP_STATUS)")
                        st.markdown("Affiche la proportion des processus de travail par leur statut actuel (ex: Running, Waiting), utile pour la surveillance de l'état du système.")
                        required_col = 'WP_STATUS'
                        if required_col in df_perf.columns:
                            if not df_perf[required_col].empty and df_perf[required_col].value_counts().sum() > 0:
                                status_counts = df_perf[required_col].value_counts().reset_index()
                                status_counts.columns = ['Statut', 'Count']
                                if not status_counts.empty and status_counts['Count'].sum() > 0:
                                    fig_status_pie = px.pie(status_counts, values='Count', names='Statut',
                                                            title="Répartition des Processus de Travail par Statut",
                                                            hole=0.3, color_discrete_sequence=px.colors.qualitative.Pastel)
                                    st.plotly_chart(fig_status_pie, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour la répartition par statut des processus de travail après filtrage.")
                            else:
                                st.info(f"La colonne '{required_col}' est vide ou ne contient pas de données valides après filtrage.")
                        else:
                            st.info(f"Colonne '{required_col}' manquante pour ce graphique. Colonnes disponibles: {df_perf.columns.tolist()}")

                        st.subheader("Nombre de Processus de Travail par Type (WP_TYP)")
                        st.markdown("Compte le nombre de processus de travail pour chaque type (ex: Dialogue, Batch), donnant une vue de la configuration des processus.")
                        required_col = 'WP_TYP'
                        if required_col in df_perf.columns:
                            if not df_perf[required_col].empty and df_perf[required_col].value_counts().sum() > 0:
                                type_counts = df_perf[required_col].value_counts().reset_index()
                                type_counts.columns = ['Type', 'Count']
                                if not type_counts.empty and type_counts['Count'].sum() > 0:
                                    fig_type_bar = px.bar(type_counts, x='Type', y='Count',
                                                          title="Nombre de Processus de Travail par Type",
                                                          labels={'Type': 'Type de Processus', 'Count': 'Nombre'},
                                                          color='Count', color_continuous_scale=px.colors.sequential.Viridis)
                                    st.plotly_chart(fig_type_bar, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour le nombre de processus de travail par type après filtrage.")
                            else:
                                st.info(f"La colonne '{required_col}' est vide ou ne contient pas de données valides après filtrage.")
                        else:
                            st.info(f"Colonne '{required_col}' manquante pour ce graphique. Colonnes disponibles: {df_perf.columns.tolist()}")

                        st.subheader("Temps CPU Moyen par Type de Processus de Travail (en secondes)")
                        st.markdown("Compare le temps CPU moyen consommé par les différents types de processus de travail, aidant à identifier les types de processus les plus coûteux en ressources.")
                        required_cols = ['WP_TYP', 'WP_CPU_SECONDS']
                        if all(col in df_perf.columns for col in required_cols):
                            if df_perf['WP_CPU_SECONDS'].sum() > 0:
                                avg_cpu_by_type = df_perf.groupby('WP_TYP')['WP_CPU_SECONDS'].mean().reset_index()
                                if not avg_cpu_by_type.empty and avg_cpu_by_type['WP_CPU_SECONDS'].sum() > 0:
                                    fig_avg_cpu_type = px.bar(avg_cpu_by_type, x='WP_TYP', y='WP_CPU_SECONDS',
                                                              title="Temps CPU Moyen par Type de Processus de Travail",
                                                              labels={'WP_TYP': 'Type de Processus', 'WP_CPU_SECONDS': 'Temps CPU Moyen (s)'},
                                                              color='WP_CPU_SECONDS', color_continuous_scale=px.colors.sequential.Plasma)
                                    st.plotly_chart(fig_avg_cpu_type, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour le temps CPU moyen par type de processus de travail après filtrage.")
                            else:
                                st.info("La colonne 'WP_CPU_SECONDS' est présente mais sa somme est zéro/vide après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_perf.columns.tolist()}")

                        st.subheader("Nombre Total de Redémarrages par Type de Processus de Travail (WP_IRESTRT)")
                        st.markdown("Suivez les redémarrages des processus de travail pour identifier les instabilités du système et les processus problématiques.")
                        required_cols = ['WP_TYP', 'WP_IRESTRT']
                        if all(col in df_perf.columns for col in required_cols):
                            if df_perf['WP_IRESTRT'].sum() > 0:
                                restarts_by_type = df_perf.groupby('WP_TYP')['WP_IRESTRT'].sum().nlargest(10).reset_index()
                                if not restarts_by_type.empty and restarts_by_type['WP_IRESTRT'].sum() > 0:
                                    fig_restarts_type = px.bar(restarts_by_type, x='WP_TYP', y='WP_IRESTRT',
                                                               title="Nombre Total de Redémarrages par Type de Processus de Travail",
                                                               labels={'WP_TYP': 'Type de Processus', 'WP_IRESTRT': 'Nombre Total de Redémarrages'},
                                                               color='WP_IRESTRT', color_continuous_scale=px.colors.sequential.OrRd)
                                    st.plotly_chart(fig_restarts_type, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour le nombre de redémarrages par type de processus de travail après filtrage.")
                            else:
                                st.info("La colonne 'WP_IRESTRT' est présente mais sa somme est zéro/vide après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_perf.columns.tolist()}")
                    else:
                        st.warning("Données de performance non disponibles ou filtrées à vide. Veuillez vérifier les filtres globaux ou le fichier source 'AL_GET_PERFORMANCE_final_cleaned_clean.xlsx'.")
                
                elif tab_label == "Résumé des Traces de Performance SQL":
                    # --- Onglet 7: Résumé des Traces de Performance SQL (performance_trace_summary_final_cleaned_clean.xlsx) ---
                    st.header("📊 Résumé des Traces de Performance SQL")
                    st.markdown("Cette section se concentre sur l'analyse des traces SQL pour identifier les requêtes les plus coûteuses en temps et en ressources.")
                    df_sql_trace = dfs['sql_trace_summary'].copy() # Work with a copy after global filters

                    if not df_sql_trace.empty:
                        with st.expander("🔬 Données de Traces SQL Filtrées (Aperçu pour débogage)"):
                            st.dataframe(df_sql_trace.head())
                            st.write(f"Nombre de lignes après filtres globaux: {len(df_sql_trace)}")
                            st.write(f"Colonnes disponibles: {df_sql_trace.columns.tolist()}")

                        st.subheader("Top 10 Requêtes SQL par Temps d'Exécution Total (EXECTIME)")
                        st.markdown("""
                            Ce graphique identifie les 10 requêtes SQL qui ont consommé le plus de temps d'exécution cumulé.
                            Il est crucial pour repérer les goulots d'étranglement globaux en termes de performance.
                            """)
                        required_cols = ['SQLSTATEM', 'EXECTIME']
                        if all(col in df_sql_trace.columns for col in required_cols):
                            if df_sql_trace['EXECTIME'].sum() > 0:
                                top_sql_by_exectime = df_sql_trace.groupby('SQLSTATEM')['EXECTIME'].sum().nlargest(10, 'EXECTIME').reset_index()
                                top_sql_by_exectime['SQLSTATEM_SHORT'] = top_sql_by_exectime['SQLSTATEM'].apply(lambda x: x[:70] + '...' if len(x) > 70 else x)
                                fig_top_sql_exectime = px.bar(top_sql_by_exectime, y='SQLSTATEM_SHORT', x='EXECTIME', orientation='h',
                                                               title="Top 10 Requêtes SQL par Temps d'Exécution Total",
                                                               labels={'SQLSTATEM_SHORT': 'Instruction SQL', 'EXECTIME': 'Temps d\'Exécution Total'},
                                                               color='EXECTIME', color_continuous_scale=px.colors.sequential.Blues)
                                fig_top_sql_exectime.update_yaxes(autorange="reversed")
                                st.plotly_chart(fig_top_sql_exectime, use_container_width=True)
                            else:
                                st.info("La colonne 'EXECTIME' est présente mais sa somme est zéro/vide après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_sql_trace.columns.tolist()}")

                        st.subheader("Top 10 Requêtes SQL par Nombre Total d'Exécutions (TOTALEXEC)")
                        st.markdown("""
                            Ce graphique met en évidence les 10 requêtes SQL les plus fréquemment exécutées.
                            Il est utile pour identifier les requêtes qui, même si elles ne sont pas individuellement lentes,
                            peuvent avoir un impact significatif sur la performance globale en raison de leur volume d'exécution élevé.
                            """)
                        required_cols = ['SQLSTATEM', 'TOTALEXEC']
                        if all(col in df_sql_trace.columns for col in required_cols):
                            if df_sql_trace['TOTALEXEC'].sum() > 0:
                                top_sql_by_totalexec = df_sql_trace.groupby('SQLSTATEM')['TOTALEXEC'].sum().nlargest(10, 'TOTALEXEC').reset_index()
                                top_sql_by_totalexec['SQLSTATEM_SHORT'] = top_sql_by_totalexec['SQLSTATEM'].apply(lambda x: x[:70] + '...' if len(x) > 70 else x)
                                fig_top_sql_totalexec = px.bar(top_sql_by_totalexec, y='SQLSTATEM_SHORT', x='TOTALEXEC', orientation='h',
                                                                title="Top 10 Requêtes SQL par Nombre Total d'Exécutions",
                                                                labels={'SQLSTATEM_SHORT': 'Instruction SQL', 'TOTALEXEC': 'Nombre Total d\'Exécutions'},
                                                                color='TOTALEXEC', color_continuous_scale=px.colors.sequential.Greens)
                                fig_top_sql_totalexec.update_yaxes(autorange="reversed")
                                st.plotly_chart(fig_top_sql_totalexec, use_container_width=True)
                            else:
                                st.info("La colonne 'TOTALEXEC' est présente mais sa somme est zéro/vide après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_sql_trace.columns.tolist()}")

                        st.subheader("Distribution du Temps par Exécution (TIMEPEREXE)")
                        st.markdown("""
                            Cette courbe de densité montre la répartition des temps d'exécution individuels par requête.
                            Elle permet de comprendre si la plupart des exécutions sont rapides ou si certaines sont significativement plus lentes,
                            indiquant des performances inégales.
                            """)
                        required_col = 'TIMEPEREXE'
                        if required_col in df_sql_trace.columns:
                            if df_sql_trace[required_col].sum() > 0 and df_sql_trace[required_col].nunique() > 1:
                                fig_time_per_exe_dist = ff.create_distplot([df_sql_trace[required_col].dropna()], [required_col],
                                                                           bin_size=df_sql_trace[required_col].std()/5 if df_sql_trace[required_col].std() > 0 else 1,
                                                                           show_rug=False, show_hist=False)
                                fig_time_per_exe_dist.update_layout(title_text="Distribution du Temps par Exécution",
                                                                    xaxis_title='Temps par Exécution',
                                                                    yaxis_title='Densité')
                                fig_time_per_exe_dist.data[0].line.color = 'darkgreen'
                                st.plotly_chart(fig_time_per_exe_dist, use_container_width=True)
                            else:
                                st.info(f"La colonne '{required_col}' est présente mais sa somme est zéro/vide ou contient des valeurs uniques après filtrage, impossible de créer une courbe de densité.")
                        else:
                            st.info(f"Colonne '{required_col}' manquante pour ce graphique. Colonnes disponibles: {df_sql_trace.columns.tolist()}")

                        st.subheader("Distribution du Temps Moyen par Enregistrement (AVGTPERREC) pour le serveur 'ECC-VE7-00'")
                        st.markdown("""
                            Cette courbe de densité montre la répartition du temps moyen par enregistrement spécifiquement pour le serveur "ECC-VE7-00".
                            Elle permet d'analyser la cohérence des performances de ce serveur en termes de traitement des enregistrements.
                            """)
                        required_cols = ['SERVERNAME', 'AVGTPERREC']
                        if all(col in df_sql_trace.columns for col in required_cols):
                            df_ecc_ve7_00 = df_sql_trace[df_sql_trace['SERVERNAME'].astype(str).str.contains('ECC-VE7-00', na=False, case=False)].copy()
                            
                            if not df_ecc_ve7_00.empty:
                                if df_ecc_ve7_00['AVGTPERREC'].sum() > 0:
                                    avg_t_per_rec_data = df_ecc_ve7_00['AVGTPERREC'].dropna()
                                    
                                    if avg_t_per_rec_data.nunique() > 1:
                                        fig_ecc_ve7_00_avg_time_dist = ff.create_distplot([avg_t_per_rec_data], ['AVGTPERREC'],
                                                                                   bin_size=avg_t_per_rec_data.std()/5 if avg_t_per_rec_data.std() > 0 else 1,
                                                                                   show_rug=False, show_hist=False)
                                        fig_ecc_ve7_00_avg_time_dist.update_layout(title_text="Distribution du Temps Moyen par Enregistrement (AVGTPERREC) pour 'ECC-VE7-00'",
                                                                            xaxis_title='Temps Moyen par Enregistrement',
                                                                            yaxis_title='Densité')
                                        fig_ecc_ve7_00_avg_time_dist.data[0].line.color = 'darkblue'
                                        st.plotly_chart(fig_ecc_ve7_00_avg_time_dist, use_container_width=True)
                                    else:
                                        st.info("Données insuffisantes ou valeurs uniques dans 'AVGTPERREC' pour créer une courbe de densité pour 'ECC-VE7-00'.")
                                else:
                                    st.info("La colonne 'AVGTPERREC' pour le serveur 'ECC-VE7-00' est présente mais sa somme est zéro/vide après filtrage.")
                            else:
                                st.info("Aucune donnée valide pour le serveur 'ECC-VE7-00' après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_sql_trace.columns.tolist()}")

                        st.subheader("Top 10 Requêtes SQL par Temps Moyen par Exécution (TIMEPEREXE)")
                        st.markdown("""
                            Ce graphique identifie les 10 requêtes SQL qui prennent le plus de temps en moyenne à chaque exécution.
                            Ceci est utile pour cibler les requêtes intrinsèquement lentes, même si elles ne sont pas exécutées très fréquemment.
                            """)
                        required_cols = ['SQLSTATEM', 'TIMEPEREXE']
                        if all(col in df_sql_trace.columns for col in required_cols):
                            if df_sql_trace['TIMEPEREXE'].sum() > 0:
                                top_sql_by_time_per_exe = df_sql_trace.groupby('SQLSTATEM')['TIMEPEREXE'].mean().nlargest(10, 'TIMEPEREXE').reset_index()
                                top_sql_by_time_per_exe['SQLSTATEM_SHORT'] = top_sql_by_time_per_exe['SQLSTATEM'].apply(lambda x: x[:70] + '...' if len(x) > 70 else x)
                                fig_top_sql_time_per_exe = px.bar(top_sql_by_time_per_exe, y='SQLSTATEM_SHORT', x='TIMEPEREXE', orientation='h',
                                                                   title="Top 10 Requêtes SQL par Temps Moyen par Exécution",
                                                                   labels={'SQLSTATEM_SHORT': 'Instruction SQL', 'TIMEPEREXE': 'Temps Moyen par Exécution'},
                                                                   color='TIMEPEREXE', color_continuous_scale=px.colors.sequential.Oranges)
                                fig_top_sql_time_per_exe.update_yaxes(autorange="reversed")
                                st.plotly_chart(fig_top_sql_time_per_exe, use_container_width=True)
                            else:
                                st.info("La colonne 'TIMEPEREXE' est présente mais sa somme est zéro/vide après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_sql_trace.columns.tolist()}")

                        st.subheader("Top 10 Requêtes SQL par Nombre d'Enregistrements Traités (RECPROCNUM)")
                        st.markdown("""
                            Ce graphique montre les 10 requêtes SQL qui traitent le plus grand nombre d'enregistrements.
                            Cela peut indiquer des requêtes qui accèdent à de grandes quantités de données, potentiellement optimisables
                            par l'ajout d'index ou la refonte de la logique de récupération des données.
                            """)
                        required_cols = ['SQLSTATEM', 'RECPROCNUM']
                        if all(col in df_sql_trace.columns for col in required_cols):
                            if df_sql_trace['RECPROCNUM'].sum() > 0:
                                top_sql_by_recprocnum = df_sql_trace.groupby('SQLSTATEM')['RECPROCNUM'].sum().nlargest(10, 'RECPROCNUM').reset_index()
                                top_sql_by_recprocnum['SQLSTATEM_SHORT'] = top_sql_by_recprocnum['SQLSTATEM'].apply(lambda x: x[:70] + '...' if len(x) > 70 else x)
                                fig_top_sql_recprocnum = px.bar(top_sql_by_recprocnum, y='SQLSTATEM_SHORT', x='RECPROCNUM', orientation='h',
                                                                 title="Top 10 Requêtes SQL par Nombre d'Enregistrements Traités",
                                                                 labels={'SQLSTATEM_SHORT': 'Instruction SQL', 'RECPROCNUM': 'Nombre d\'Enregistrements Traités'},
                                                                 color='RECPROCNUM', color_continuous_scale=px.colors.sequential.Purples)
                                fig_top_sql_recprocnum.update_yaxes(autorange="reversed")
                                st.plotly_chart(fig_top_sql_recprocnum, use_container_width=True)
                            else:
                                st.info("La colonne 'RECPROCNUM' est présente mais sa somme est zéro/vide après filtrage.")
                        else:
                            st.info(f"Colonnes nécessaires ({', '.join(required_cols)}) manquantes pour ce graphique. Colonnes disponibles: {df_sql_trace.columns.tolist()}")
                    else:
                        st.warning("Données de traces SQL non disponibles ou filtrées à vide. Veuillez vérifier les filtres globaux ou le fichier source 'performance_trace_summary_final_cleaned_clean.xlsx'.")

                elif tab_label == "Analyse des Utilisateurs":
                    # --- Nouvelle section: Analyse des Utilisateurs (usr02_data.xlsx) ---
                    st.header("👥 Analyse des Utilisateurs")
                    st.markdown("Cette section fournit des informations sur les utilisateurs du système, y compris leur type et la date de leur dernière connexion.")
                    df_usr02 = dfs['usr02'].copy() # Work with a copy after global filters

                    if not df_usr02.empty:
                        with st.expander("🔬 Données Utilisateurs Filtrées (Aperçu pour débogage)"):
                            st.dataframe(df_usr02.head())
                            st.write(f"Nombre de lignes après filtres globaux: {len(df_usr02)}")
                            st.write(f"Colonnes disponibles: {df_usr02.columns.tolist()}")

                        st.subheader("Répartition des Utilisateurs par Type (USTYP)")
                        st.markdown("Affiche la distribution des utilisateurs selon leur type (ex: Dialogue, Système), permettant de comprendre la composition des utilisateurs du système.")
                        required_col = 'USTYP'
                        if required_col in df_usr02.columns:
                            if not df_usr02[required_col].empty and df_usr02[required_col].value_counts().sum() > 0:
                                user_type_counts = df_usr02[required_col].value_counts().reset_index()
                                user_type_counts.columns = ['Type d\'Utilisateur', 'Nombre']
                                if not user_type_counts.empty and user_type_counts['Nombre'].sum() > 0:
                                    fig_user_type_pie = px.pie(user_type_counts, values='Nombre', names='Type d\'Utilisateur',
                                                               title="Répartition des Utilisateurs par Type",
                                                               hole=0.3, color_discrete_sequence=px.colors.qualitative.Set3)
                                    st.plotly_chart(fig_user_type_pie, use_container_width=True)
                                else:
                                    st.info("Pas de données valides pour la répartition des types d'utilisateurs après filtrage.")
                            else:
                                st.info(f"La colonne '{required_col}' est vide ou ne contient pas de données valides après filtrage.")
                        else:
                            st.info(f"Colonne '{required_col}' manquante pour ce graphique. Colonnes disponibles: {df_usr02.columns.tolist()}")

                        st.subheader("Nombre d'Utilisateurs par Date de Dernier Logon (GLTGB)")
                        st.markdown("""
                            Ce graphique montre le nombre d'utilisateurs ayant enregistré leur dernière connexion à une date donnée.
                            Les dates "00000000" (logon jamais enregistré) sont exclues de cette analyse, ce qui est utile pour l'audit et la gestion des utilisateurs inactifs.
                            """)
                        required_col = 'GLTGB_DATE'
                        if required_col in df_usr02.columns:
                            if not df_usr02[required_col].isnull().all():
                                df_valid_logons = df_usr02.dropna(subset=[required_col]).copy()
                                if not df_valid_logons.empty:
                                    logon_counts = df_valid_logons[required_col].dt.date.value_counts().sort_index().reset_index()
                                    logon_counts.columns = ['Date de Dernier Logon', 'Nombre d\'Utilisateurs']
                                    
                                    fig_logon_dates = px.line(logon_counts, x='Date de Dernier Logon', y='Nombre d\'Utilisateurs',
                                                             title="Nombre d'Utilisateurs par Date de Dernier Logon",
                                                             labels={'Date de Dernier Logon': 'Date', 'Nombre d\'Utilisateurs': 'Nombre d\'Utilisateurs'},
                                                             markers=True,
                                                             color_discrete_sequence=['#6A0DAD'])
                                    
                                    fig_logon_dates.update_xaxes(
                                        tickangle=45,
                                        rangeselector=dict(
                                            buttons=list([
                                                dict(count=1, label="1m", step="month", stepmode="backward"),
                                                dict(count=6, label="6m", step="month", stepmode="backward"),
                                                dict(count=1, label="YTD", step="year", stepmode="todate"),
                                                dict(count=1, label="1y", step="year", stepmode="backward"),
                                                dict(step="all")
                                            ])
                                        ),
                                        rangeslider=dict(visible=True),
                                        type="date"
                                    )
                                    
                                    st.plotly_chart(fig_logon_dates, use_container_width=True)
                                else:
                                    st.info("Aucune donnée de date de dernier logon valide après filtrage.")
                            else:
                                st.info(f"La colonne '{required_col}' est présente mais ne contient pas de dates valides après filtrage.")
                        else:
                            st.info(f"Colonne '{required_col}' manquante pour ce graphique. Colonnes disponibles: {df_usr02.columns.tolist()}")
                    else:
                        st.warning("Données utilisateurs (USR02) non disponibles ou filtrées à vide. Veuillez vérifier les filtres globaux ou le fichier source 'usr02_data.xlsx'.")


# Option pour afficher tous les DataFrames (utile pour le débogage)
with st.expander("🔍 Afficher tous les DataFrames chargés (pour débogage)"):
    for key, df in dfs.items():
        st.subheader(f"DataFrame: {key} (Taille: {len(df)} lignes)")
        st.dataframe(df.head())
        if st.checkbox(f"Afficher les informations de '{key}'", key=f"info_{key}"):
            buffer = io.StringIO()
            df.info(buf=buffer)
            st.text(buffer.getvalue())
