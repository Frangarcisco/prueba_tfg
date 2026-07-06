"""
app.py
======
Aplicación web para la predicción del valor de mercado
de jugadores de LaLiga Fantasy DAZN.

Lanzar con:
    streamlit run app.py
"""

import os
import sys
import joblib
import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go

# ── Añadir carpeta modelo/ al path ────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
MODELO_DIR = os.path.join(BASE_DIR, "modelo")
sys.path.insert(0, MODELO_DIR)

from modelo_tfg import crear_features, FEATURES

# ── Rutas ─────────────────────────────────────────────────────────
DATASET_PATH    = os.path.join(BASE_DIR, "data", "raw", "DATASET_MAESTRO_TFG_corregido.csv")
MODELOS_DIR     = os.path.join(BASE_DIR, "modelos_guardados")
HORIZONTES_DISP = [1, 3, 7]   # modelos entrenados disponibles

# ─────────────────────────────────────────────────────────────────
# CARGA DE DATOS Y MODELOS (cacheados para no repetir en cada click)
# ─────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner="Cargando dataset...")
def cargar_dataset():
    df = pd.read_csv(DATASET_PATH)
    df["date"] = pd.to_datetime(df["date"], dayfirst=True, errors="coerce").dt.normalize()
    df = df.dropna(subset=['date'])
    df = df.sort_values(['player_id', 'date']).reset_index(drop=True)
    df.drop(columns=['bids'], inplace=True, errors='ignore')
    df['equipo']   = df.groupby('player_id')['equipo'].ffill().bfill()
    df['nombre']   = df.groupby('player_id')['nombre'].ffill().bfill()
    df['posicion'] = df.groupby('player_id')['posicion'].ffill().bfill()
    cols_num = df.select_dtypes(include=[np.number]).columns
    df[cols_num] = df[cols_num].fillna(0)
    df = crear_features(df)
    return df


@st.cache_resource(show_spinner="Cargando modelos...")
def cargar_modelos():
    modelos = {}
    for h in HORIZONTES_DISP:
        ruta = os.path.join(MODELOS_DIR, f"modelo_{h}d.pkl")
        if os.path.exists(ruta):
            modelos[h] = joblib.load(ruta)
        else:
            st.error(f"No se encontró el modelo para {h} días en {ruta}. "
                     f"Ejecuta primero guardar_modelos.py")
    return modelos


def limpiar_fila(fila_df):
    """Convierte una fila del dataset a float64 para pasarla al modelo."""
    resultado = pd.DataFrame(index=fila_df.index)
    for col in fila_df.columns:
        try:
            resultado[col] = pd.to_numeric(fila_df[col], errors='coerce').fillna(0).astype('float64')
        except Exception:
            resultado[col] = 0.0
    return resultado


def predecir(df_fila, modelo_pkg):
    """Realiza la predicción sobre una fila del dataset."""
    features    = modelo_pkg["features"]
    modelo      = modelo_pkg["modelo"]
    features_ok = [f for f in features if f in df_fila.columns]
    X           = limpiar_fila(df_fila[features_ok])
    variacion   = float(modelo.predict(X)[0])
    return variacion


def nombre_posicion(pos_id):
    mapa = {10: "Portero", 20: "Defensa", 30: "Centrocampista", 40: "Delantero"}
    return mapa.get(int(pos_id), f"Pos {pos_id}")


# ─────────────────────────────────────────────────────────────────
# INTERFAZ
# ─────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="LaLiga Fantasy Predictor de Precios",
    page_icon="⚽",
    layout="wide",
)



# Carga
df      = cargar_dataset()
modelos = cargar_modelos()

if not modelos:
    st.stop()

# ── Sidebar: controles ────────────────────────────────────────────
with st.sidebar:
    st.header("Parámetros de consulta")

    # Selector de jugador
    jugadores = (
        df[['player_id', 'player_name', 'equipo', 'posicion']]
        .drop_duplicates('player_id')
        .sort_values('player_name')
    )
    opciones_jugador = {
        row['player_name']: row['player_id']
        for _, row in jugadores.iterrows()
    }
    jugador_nombre = st.selectbox(
        "Jugador",
        options=list(opciones_jugador.keys()),
        index=0,
    )
    jugador_id = opciones_jugador[jugador_nombre]

    # Fechas disponibles para ese jugador
    fechas_jugador = sorted(
        df[df['player_id'] == jugador_id]['date'].unique()
    )
    fechas_str = [f.strftime('%Y-%m-%d') for f in pd.to_datetime(fechas_jugador)]

    fecha_sel_str = st.selectbox(
        "Fecha de consulta",
        options=fechas_str,
        index=len(fechas_str) - 1,   # por defecto la más reciente
    )
    fecha_sel = pd.to_datetime(fecha_sel_str).normalize()

    # Horizonte
    horizonte = st.radio(
        "Horizonte de predicción",
        options=HORIZONTES_DISP,
        format_func=lambda h: f"{h} día{'s' if h > 1 else ''}",
        horizontal=True,
    )

    st.divider()
    st.caption("Datos disponibles: 02/07/2025 – 08/02/2026")
    st.caption("Modelos: XGBoost entrenado con split temporal (últimos 30 días como test)")

# ── Panel principal ───────────────────────────────────────────────
df_jugador = df[df['player_id'] == jugador_id].sort_values('date')
fila       = df_jugador[df_jugador['date'] == fecha_sel]

if fila.empty:
    st.warning("No hay datos para ese jugador en la fecha seleccionada.")
    st.stop()

# Datos básicos del jugador
info = fila.iloc[0]
precio_actual = float(info['marketValue'])

col1, col2, col3 = st.columns(3)
col1.metric("Jugador",   jugador_nombre)
col2.metric("Equipo",    str(info.get('equipo', '—')))
col3.metric("Posición",  nombre_posicion(info.get('posicion', 0)))

st.divider()

# ── Predicción ────────────────────────────────────────────────────
modelo_pkg = modelos[horizonte]
variacion  = predecir(fila, modelo_pkg)
precio_pred = precio_actual + variacion
fecha_pred  = fecha_sel + pd.Timedelta(days=horizonte)

# Comprobar si existe el valor real en el dataset
fila_real = df_jugador[df_jugador['date'] == fecha_pred]
precio_real = float(fila_real.iloc[0]['marketValue']) if not fila_real.empty else None

# Métricas
c1, c2, c3, c4 = st.columns(4)
c1.metric(
    label=f"Precio en {fecha_sel_str}",
    value=f"{precio_actual:,.0f} €",
)
c2.metric(
    label=f"Predicción a {horizonte}d ({fecha_pred.strftime('%Y-%m-%d')})",
    value=f"{precio_pred:,.0f} €",
    delta=f"{variacion:+,.0f} €",
    delta_color="normal",
)
c3.metric(
    label="Variación esperada (%)",
    value=f"{variacion / precio_actual * 100:+.2f} %",
)
if precio_real is not None:
    error_abs = abs(precio_real - precio_pred)
    c4.metric(
        label=f"Precio real ({fecha_pred.strftime('%Y-%m-%d')})",
        value=f"{precio_real:,.0f} €",
        delta=f"Error: {error_abs:,.0f} €",
        delta_color="off",
    )
else:
    c4.metric(
        label="Precio real",
        value="No disponible",
        help="La fecha predicha está fuera del rango del dataset.",
    )

st.divider()

# ── Gráfica ───────────────────────────────────────────────────────
st.subheader(f"Evolución del valor de mercado — {jugador_nombre}")

# Ventana de histórico: 60 días antes de la fecha seleccionada
ventana_inicio = fecha_sel - pd.Timedelta(days=30)
df_hist = df_jugador[
    (df_jugador['date'] >= ventana_inicio) &
    (df_jugador['date'] <= fecha_sel)
].copy()

fig = go.Figure()

# Línea histórica
fig.add_trace(go.Scatter(
    x=df_hist['date'],
    y=df_hist['marketValue'],
    mode='lines+markers',
    name='Histórico',
    line=dict(color='#1f77b4', width=2),
    marker=dict(size=4),
))

# Punto de consulta
fig.add_trace(go.Scatter(
    x=[fecha_sel],
    y=[precio_actual],
    mode='markers',
    name='Fecha de consulta',
    marker=dict(color='#ff7f0e', size=12, symbol='circle'),
))

# Línea de predicción (desde fecha_sel hasta fecha_pred)
fig.add_trace(go.Scatter(
    x=[fecha_sel, fecha_pred],
    y=[precio_actual, precio_pred],
    mode='lines+markers',
    name=f'Predicción ({horizonte}d)',
    line=dict(color='#d62728', width=2, dash='dash'),
    marker=dict(size=10, symbol='star'),
))

# Valor real si existe
if precio_real is not None:
    fig.add_trace(go.Scatter(
        x=[fecha_pred],
        y=[precio_real],
        mode='markers',
        name='Valor real',
        marker=dict(color='#2ca02c', size=12, symbol='diamond'),
    ))

fig.update_layout(
    xaxis_title="Fecha",
    yaxis_title="Valor de mercado (€)",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    hovermode="x unified",
    height=420,
    margin=dict(l=20, r=20, t=40, b=20),
)
fig.update_xaxes(range=[ventana_inicio, fecha_pred + pd.Timedelta(days=3)])
fig.update_yaxes(tickformat=",.0f")

st.plotly_chart(fig, use_container_width=True)

# ── Info del modelo ───────────────────────────────────────────────
with st.expander("ℹ️ Información del modelo utilizado"):
    m = modelo_pkg["metricas"]
    st.markdown(f"""
    **Horizonte:** {horizonte} día(s)  
    **MAE (test):** {m['mae']:,.0f} €  
    **RMSE (test):** {m['rmse']:,.0f} €  
    **R² (test):** {m['r2']:.4f}  
    **Fecha de corte train/test:** {modelo_pkg['fecha_corte'].strftime('%Y-%m-%d')}  
    
    *El modelo fue entrenado con datos históricos hasta la fecha de corte 
    y evaluado sobre los últimos 30 días del dataset.*
    """)