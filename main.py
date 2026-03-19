"""
main.py
=======
Punto de entrada único del proyecto TFG.

Expone dos modos de ejecución mediante flags:

  --fetch    Descarga todos los datos crudos de la temporada desde la
             API de LaLiga Fantasy y los guarda en data/raw/.
             (Ejecutar una sola vez, o cuando se quiera actualizar los datos.)

  --process  Limpia las estadísticas y genera el Dataset Maestro en
             data/processed/. Requiere que data/raw/ tenga los tres
             ficheros generados por --fetch.

  (sin flag) Ejecuta ambas fases en orden: fetch → process.

Uso
---
    python main.py           # Descarga + procesamiento completo
    python main.py --fetch   # Solo descarga de datos crudos
    python main.py --process # Solo procesamiento (datos ya descargados)
"""

import sys
import traceback
from datetime import datetime


def run_fetch():
    """Fase 1: extracción de datos desde la API."""
    from scraping.fetch_data import fetch_all
    fetch_all()


def run_process():
    """Fase 2: limpieza de stats + construcción del dataset maestro."""
    from data_processing.clean_stats import clean_stats
    from data_processing.build_master_dataset import build_master_dataset

    print("\n─── Limpieza de estadísticas ───────────────────────────")
    clean_stats()

    print("\n─── Construcción del Dataset Maestro ───────────────────")
    build_master_dataset()


def main():
    args = sys.argv[1:]

    only_fetch   = "--fetch"   in args
    only_process = "--process" in args
    run_all      = not only_fetch and not only_process

    start = datetime.now()
    print("=" * 60)
    print("  TFG — Predicción del Valor de Mercado en LaLiga Fantasy")
    print(f"  {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    try:
        if only_fetch or run_all:
            print("\n─── FASE 1: Extracción de datos ─────────────────────────")
            run_fetch()

        if only_process or run_all:
            print("\n─── FASE 2: Procesamiento ───────────────────────────────")
            run_process()

    except KeyboardInterrupt:
        print("\n\n⚠️  Ejecución interrumpida por el usuario.")
        sys.exit(0)
    except Exception:
        print("\n❌ Error inesperado:")
        traceback.print_exc()
        sys.exit(1)

    elapsed = (datetime.now() - start).seconds
    print(f"\n{'=' * 60}")
    print(f"  ✅ Completado en {elapsed}s.")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
