# Catálogo de Módulos

Este documento describe módulos disponibles, sus entradas y salidas esperadas.

Convenciones:
- Todos los módulos exponen una macro pública `%<modulo>_run(...)`.
- Validaciones de entrada en `<modulo>_contract.sas`.
- Implementación en `impl/`.
- Outputs se escriben en `outputs/runs/<run_id>/...`.

---

## 1) Contexto de ejecución (estándar)

Un módulo debe poder ejecutarse sobre:
- Universo (troncal): `data/processed/troncal_X/<split>/base.sashdat`
- Segmento: `data/processed/troncal_X/<split>/segNNN.sashdat`

Donde `<split>` es `train` u `oot`.

Orden de ejecución recomendado:
- Segmentos primero (si existen), luego universo.

---

## 2) Módulo: Gini

**Ruta**
- `src/modules/gini/`

**API pública**
- `%gini_run(...)`

**Inputs típicos**
- Dataset input (universe o segmento) con:
  - `target` (binario o según definición del control)
  - `pd` o `xb` o score equivalente (según configuración)
  - (Opcional) `monto` si el control requiere ponderación
  - (Opcional) variables de partición (por ejemplo `mes`)

**Validaciones (contract)**
- Existencia del input.
- Presencia de columnas requeridas (al menos `target` y score).
- No vacío (nobs > 0).

**Outputs esperados**
- `outputs/runs/<run_id>/tables/gini_*.sas7bdat` (tabla resumen y/o detalle)
- `outputs/runs/<run_id>/reports/gini_*.xlsx` o HTML (si aplica)
- `outputs/runs/<run_id>/images/gini_*.png` (si aplica)
- Logs en `outputs/runs/<run_id>/logs/`

---

## 3) Módulo: PSI

**Ruta**
- `src/modules/psi/`

**API pública**
- `%psi_run(...)`

**Inputs típicos**
- Dos datasets comparables (por ejemplo baseline vs current) o un dataset con partición temporal.
- Variables numéricas/categóricas definidas por configuración (`var_num_list`, `var_cat_list`).
- Variable de corte (`mes_var`) si se calcula PSI por periodo.

**Validaciones (contract)**
- Existencia de inputs.
- Consistencia de variables entre inputs.
- No vacío (nobs > 0).

**Outputs esperados**
- `outputs/runs/<run_id>/tables/psi_*.sas7bdat`
- `outputs/runs/<run_id>/reports/psi_*.xlsx` o HTML
- `outputs/runs/<run_id>/images/psi_*.png` (si aplica)
- Logs en `outputs/runs/<run_id>/logs/`

---

## 4) Reglas para agregar módulos

Para agregar un módulo nuevo:
1. Crear carpeta `src/modules/<modulo>/`.
2. Implementar:
   - `<modulo>_run.sas` (macro pública)
   - `<modulo>_contract.sas` (validaciones)
   - `impl/<modulo>_compute.sas`
   - `impl/<modulo>_report.sas` (si aplica)
3. Registrar en `configs/registry/modules_registry.sas`.
4. Añadir sección en este documento con:
   - Inputs esperados
   - Outputs generados
   - Validaciones

Recomendación:
- Mantener nombres de outputs que incluyan:
  - troncal_X
  - split (train/oot)
  - scope (base o segNNN)
  - nombre del módulo

Ejemplo:
- `gini_troncal_1_train_base.xlsx`
- `gini_troncal_1_train_seg001.xlsx`
