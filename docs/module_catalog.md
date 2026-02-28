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

**Acceso a datos:** los inputs se leen desde el CASLIB `PROC` (PATH-based, con subdirs habilitado, mapeado a `data/processed/`). No se usa `casuser` para datos operativos; `casuser` es exclusivo para tablas de configuración.

Convención de naming operativo:
- `RAW` para `data/raw/`
- `PROC` para `data/processed/`
- Evitar aliases alternos como `RAWDATA` o `PROCESSED`.

**Regla de orquestación (contexto primero):**
1. Se selecciona/promueve el contexto de datos (`troncal_id`, `scope`, `split`, `seg_id` si aplica).
2. Luego se seleccionan módulos por Método (`Metodo 1..N`).
3. Se ejecuta el subflow de módulos para ese contexto.

**Selección de módulos por Método:**
- Cada Método es independiente (`enabled`, `module_list`).
- Por defecto, la selección de módulos se ejecuta primero sobre **segmento**.
- Universo/troncal se configura en un bloque de contexto separado.

El runner pasa el contexto (`troncal_id`, `split`, `seg_id` opcional, `run_id`) a cada módulo vía `%run_module`.

**Ciclo de vida de CASLIBs en ejecución:**
- `run_methods_segment_context` / `run_methods_universe_context` crean CASLIBs `PROC` y `OUT` al inicio del bloque.
- Por cada contexto, `run_module.sas` promueve el input específico desde `PROC` como tabla `_active_input` (vía `%_promote_castable`), ejecuta el módulo, y dropea la tabla promovida.
- Al final del bloque se dropean `PROC` y `OUT` (archivos en disco persisten).
- Patrón obligatorio: **create → promote → work → drop**.

**Restricción SAS open code:**
- `%if`/`%do` no se permiten fuera de una macro. Todo `.sas` que use lógica condicional la encapsula en `%macro ... %mend;`.
- Aplica tanto a steps (`_step06_validate`, etc.) como al runner (`%macro _main_pipeline`).
- Si un módulo necesita condicionales en su entry point, debe usar el mismo patrón.

**Parámetros específicos de módulos:**
- Parámetros como `threshold`, `num_rounds`, `num_bins` y similares **no** se declaran en `config.sas`.
- Se configuran en los steps de métodos (`steps/07_config_methods_segment.sas`, `steps/10_config_methods_universe.sas`) o como argumentos de la macro `%<modulo>_run(...)`.
- `config.sas` solo contiene parámetros estructurales de troncales/segmentos (identificadores, variables, rangos, listas, segmentación).

Orden de ejecución recomendado:
- Segmentos primero (si existen), luego universo.

### 1.1 Contrato mínimo de contexto para módulos

Todo módulo debe aceptar o derivar estos campos de contexto:
- `troncal_id`
- `scope` (`segmento` | `universo`)
- `split` (`train` | `oot`)
- `seg_id` (obligatorio solo si `scope=segmento`)
- `run_id`

Si el contexto no está completo, el módulo debe fallar temprano con mensaje claro.

### 1.2 Matriz Método → módulos (contrato funcional)

Como contrato de diseño, cada tab/hoja `Metodo N` define una lista de módulos.

Ejemplo referencial:

| Método | enabled | module_list | scope objetivo |
|--------|---------|-------------|----------------|
| Metodo 1 | 1 | estabilidad fillrate missings psi | segmento |
| Metodo 2 | 1 | bivariado correlacion | segmento |
| Metodo 3 | 0 | gini | universo |

La matriz es declarativa; el runner/subflow ejecuta solo métodos `enabled=1`.

---

## 2) Módulo: Gini

**Ruta**
- `src/modules/gini/`

**API pública**
- `%gini_run(...)`
- Parámetros de entrada incluyen:
  - `input_caslib=PROC` — CASLIB de entrada
  - `input_table=_active_input` — tabla promovida por `run_module`
  - `output_caslib=OUT` — CASLIB de salida
  - `troncal_id`, `split`, `scope`, `run_id` — contexto

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
- Parámetros de entrada incluyen:
  - `input_caslib=PROC` — CASLIB de entrada
  - `input_table=_active_input` — tabla promovida por `run_module`
  - `output_caslib=OUT` — CASLIB de salida
  - `troncal_id`, `split`, `scope`, `run_id` — contexto
  - Parámetros específicos del módulo (ej. `threshold`, `num_bins`) se pasan como argumentos adicionales

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
  - Compatibilidad de contexto (`segmento`, `universo`, o ambos)

Recomendación:
- Mantener nombres de outputs que incluyan:
  - troncal_X
  - split (train/oot)
  - scope (base o segNNN)
  - nombre del módulo
- Los outputs se persisten vía CASLIB `OUT` o un CASLIB scoped del módulo (ej. `MOD_GINI_<run_id>`), siguiendo `docs/caslib_lifecycle.md`.
- El módulo debe declarar explícitamente si soporta ejecución en `segmento`, `universo` o ambos.

Ejemplo:
- `gini_troncal_1_train_base.xlsx`
- `gini_troncal_1_train_seg001.xlsx`
