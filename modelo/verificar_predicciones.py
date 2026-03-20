"""
=============================================================
  TFG - Verificación de Predicciones vs Realidad
  Compara predicciones del 8 Feb con datos reales del 9 Feb
=============================================================
Uso:
    python modelo/verificar_predicciones.py
=============================================================
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

PREDICCIONES_PATH  = "resultados_modelo/predicciones_mañana.csv"
MERCADO_REAL_PATH  = "data/market/2026-02-09.csv"
OUTPUT_DIR         = "resultados_modelo"

# ─────────────────────────────────────────────
# 1. CARGAR DATOS
# ─────────────────────────────────────────────
print("📂 Cargando predicciones del 8 de febrero...")
df_pred = pd.read_csv(PREDICCIONES_PATH)
print(f"   Jugadores con predicción: {len(df_pred)}")

print("\n📂 Cargando mercado real del 9 de febrero...")
df_real = pd.read_csv(MERCADO_REAL_PATH)
df_real = df_real.rename(columns={
    'name': 'player_name',
    'market_value': 'mv_real_9feb',
    'market_variation': 'var_real_api'
})
print(f"   Jugadores en mercado real: {len(df_real)}")

# ─────────────────────────────────────────────
# 2. CRUZAR POR NOMBRE
# ─────────────────────────────────────────────
print("\n🔄 Cruzando por nombre...")
df = df_pred.merge(
    df_real[['player_name', 'mv_real_9feb', 'var_real_api']],
    on='player_name',
    how='inner'
)
print(f"   Jugadores cruzados: {len(df)}")

# ─────────────────────────────────────────────
# 3. CALCULAR ERRORES
# ─────────────────────────────────────────────
df['mv_real_9feb']   = df['mv_real_9feb'].astype(float)
df['pred_mv_mañana'] = df['pred_mv_mañana'].astype(float)
df['marketValue']    = df['marketValue'].astype(float)

# Variación real
df['var_real']     = df['mv_real_9feb'] - df['marketValue']
df['var_real_pct'] = (df['var_real'] / df['marketValue'] * 100).round(2)

# Error de predicción
df['error_abs']       = abs(df['pred_mv_mañana'] - df['mv_real_9feb'])
df['error_pct']       = (df['error_abs'] / df['mv_real_9feb'] * 100).round(2)
df['error_con_signo'] = df['pred_mv_mañana'] - df['mv_real_9feb']

# Dirección: ¿acertó si sube o baja?
df['pred_sube']         = df['pred_var_1d'] > 0
df['real_sube']         = df['var_real_api'] > 0
df['acierto_direccion'] = df['pred_sube'] == df['real_sube']

# ─────────────────────────────────────────────
# 4. MÉTRICAS GLOBALES
# ─────────────────────────────────────────────
mae     = df['error_abs'].mean()
mae_pct = df['error_pct'].mean()
rmse    = np.sqrt((df['error_con_signo']**2).mean())
aciertos = df['acierto_direccion'].sum()
total    = len(df)
pct_dir  = aciertos / total * 100

print(f"\n{'='*50}")
print(f"📊 RESULTADOS DE VERIFICACIÓN (8 Feb → 9 Feb)")
print(f"{'='*50}")
print(f"   Jugadores analizados:     {total}")
print(f"   MAE:                      {mae:,.0f} €")
print(f"   MAE (%):                  {mae_pct:.2f}%")
print(f"   RMSE:                     {rmse:,.0f} €")
print(f"   Aciertos de dirección:    {aciertos}/{total} ({pct_dir:.1f}%)")

# ─────────────────────────────────────────────
# 5. MEJORES Y PEORES PREDICCIONES
# ─────────────────────────────────────────────
cols = ['player_name', 'marketValue', 'pred_var_1d', 'var_real',
        'pred_mv_mañana', 'mv_real_9feb', 'error_abs', 'error_pct',
        'acierto_direccion']

print(f"\n✅ TOP 15 MEJORES PREDICCIONES (menor error %):")
print(df.nsmallest(15, 'error_pct')[cols].to_string(index=False))

print(f"\n❌ TOP 15 PEORES PREDICCIONES (mayor error %):")
print(df.nlargest(15, 'error_pct')[cols].to_string(index=False))

print(f"\n📈 SUBIDAS PREDICHAS QUE REALMENTE SUBIERON:")
print(df[df['pred_sube'] & df['real_sube']][cols].head(10).to_string(index=False))

print(f"\n📉 BAJADAS PREDICHAS QUE REALMENTE BAJARON:")
print(df[~df['pred_sube'] & ~df['real_sube']][cols].head(10).to_string(index=False))

# ─────────────────────────────────────────────
# 6. GRÁFICOS
# ─────────────────────────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(18, 5))
fig.suptitle(f'Verificación Predicciones 8 Feb → 9 Feb\n'
             f'MAE: {mae:,.0f}€ ({mae_pct:.2f}%)  |  '
             f'Aciertos dirección: {pct_dir:.1f}%  |  '
             f'N={total} jugadores',
             fontsize=13, fontweight='bold')

# 1. Variación predicha vs real
ax = axes[0]
lim = max(df['pred_var_1d'].abs().max(), df['var_real'].abs().max()) * 1.1
ax.scatter(df['var_real'], df['pred_var_1d'], alpha=0.4, s=10, color='steelblue')
ax.plot([-lim, lim], [-lim, lim], 'r--', lw=1.5, label='Predicción perfecta')
ax.axhline(0, color='gray', lw=0.5)
ax.axvline(0, color='gray', lw=0.5)
ax.set_xlabel('Variación Real (€)')
ax.set_ylabel('Variación Predicha (€)')
ax.set_title('Variación Predicha vs Real')
ax.legend()

# 2. Distribución del error
ax = axes[1]
ax.hist(df['error_pct'], bins=40, color='steelblue', edgecolor='white', alpha=0.8)
ax.axvline(mae_pct, color='red', lw=1.5, linestyle='--', label=f'MAE = {mae_pct:.2f}%')
ax.set_xlabel('Error Absoluto (%)')
ax.set_ylabel('Frecuencia')
ax.set_title('Distribución del Error')
ax.legend()

# 3. Aciertos de dirección por posición
ax = axes[2]
pos_nombres = {0: 'Portero', 1: 'Defensa', 2: 'Centrocam.', 3: 'Delantero', 4: 'Otro'}
if 'posicion' in df.columns and len(df) > 0:
    aciertos_pos = df.groupby('posicion')['acierto_direccion'].mean() * 100
    aciertos_pos.index = [pos_nombres.get(int(p), str(p)) for p in aciertos_pos.index]
    aciertos_pos.plot(kind='bar', ax=ax, color='steelblue', edgecolor='white')
    ax.axhline(50, color='red', lw=1.5, linestyle='--', label='Azar (50%)')
    ax.set_xlabel('Posición')
    ax.set_ylabel('% Aciertos dirección')
    ax.set_title('Aciertos por Posición')
    ax.legend()
    ax.tick_params(axis='x', rotation=30)

plt.tight_layout()
ruta = os.path.join(OUTPUT_DIR, 'verificacion_9feb.png')
plt.savefig(ruta, dpi=150, bbox_inches='tight')
plt.close()
print(f"\n💾 Gráfico guardado: {ruta}")

# ─────────────────────────────────────────────
# 7. GUARDAR CSV
# ─────────────────────────────────────────────
ruta_csv = os.path.join(OUTPUT_DIR, 'verificacion_completa.csv')
df.to_csv(ruta_csv, index=False)
print(f"💾 Verificación completa guardada: {ruta_csv}")
print(f"\n✅ ¡Verificación completada!")