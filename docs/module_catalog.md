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
- Cada swimlane tiene su propio `select_modules.sas` donde se habilitan/deshabilitan módulos.
- Los flags `%let run_<modulo> = 1|0;` controlan qué módulos se ejecutan.
- Cada módulo tiene su propio step file independiente (`step_<modulo>.sas`).
- En el `.flw`, cada step de módulo es un nodo que puede ejecutarse vía background submit.
- Cada step de módulo checa su flag, lee `&ctx_scope`, crea CASLIBs PROC/OUT, itera, y limpia.

**Swimlanes (dos flujos de ejecución):**
- **SEGMENTO**: `steps/segmento/context.sas` → `steps/segmento/select_modules.sas` → módulos (itera troncales + segmentos)
- **UNIVERSO**: `steps/universo/context.sas` → `steps/universo/select_modules.sas` → módulos (itera troncales, solo base)

El runner pasa el contexto (`troncal_id`, `split`, `seg_id` opcional, `run_id`) a cada módulo vía `%run_module`.

**Ciclo de vida de CASLIBs en ejecución:**
- Cada step de módulo (`step_<modulo>.sas`) crea CASLIBs `PROC` y `OUT` al inicio.
- Lee `&ctx_scope` para determinar si itera segmentos (SEGMENTO) o base/troncal (UNIVERSO).
- Por cada contexto, `run_module.sas` promueve el input específico desde `PROC` como tabla `_active_input` (vía `%_promote_castable`), ejecuta el módulo, y dropea la tabla promovida.
- Al final del step se dropean `PROC` y `OUT` (archivos en disco persisten).
- El mismo archivo step se incluye en ambos swimlanes (seg y unv). En cada swimlane, `ctx_scope` tiene un valor diferente.
- Patrón obligatorio: **create → promote → work → drop**.

**Independencia de steps:**
- Cada step es autónomo: carga sus dependencias vía `%include "&fw_root./src/common/common_public.sas";`.
- Todo step que cree CASLIBs operativos (PROC, OUT, RAW) los dropea al finalizar, junto con las tablas promovidas.
- `casuser` (config) no se dropea entre steps; persiste en la sesión CAS.

**Restricción SAS open code:**
- `%if`/`%do` no se permiten fuera de una macro. Todo `.sas` que use lógica condicional la encapsula en `%macro ... %mend;`.
- Aplica tanto a steps (`_ctx_seg_validate`, etc.) como al runner (`%macro _main_pipeline`).
- Si un módulo necesita condicionales en su entry point, debe usar el mismo patrón.

**Parámetros específicos de módulos:**
- Parámetros como `threshold`, `num_rounds`, `num_bins`, `corr_mode` y similares **no** se declaran en `config.sas`.
- Se configuran en el step del módulo correspondiente (`steps/methods/metod_N/step_<modulo>.sas`).
- `config.sas` solo contiene parámetros estructurales de troncales/segmentos.

**Flag de habilitación:**
- Cada step de módulo checa `&run_<modulo>` al inicio. Si vale 0, se salta la ejecución.
- Estos flags se setean en `steps/segmento/select_modules.sas` y `steps/universo/select_modules.sas`.

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

Cada Método agrupa módulos lógicamente. Los steps de módulos están en `steps/methods/metod_N/`.

| Método | Sub-método | Carpeta | Módulos (steps) |
|--------|------------|---------|------------------|
| Metodo 1 | — | `steps/methods/metod_1/` | universe (futuro) |
| Metodo 2 | — | `steps/methods/metod_2/` | target (futuro) |
| Metodo 3 | — | `steps/methods/metod_3/` | segmentacion (futuro) |
| Metodo 4 | 4.2 | `steps/methods/metod_4/` | estabilidad, fillrate, missings, psi |
| Metodo 4 | 4.3 | `steps/methods/metod_4/` | bivariado, **correlación**, gini |

Los sub-métodos organizan la selección en el UI y las carpetas de output (`reports/metod_4_2/`, `reports/metod_4_3/`).

Cada módulo-step es independiente: checa `&run_<modulo>`, lee `&ctx_scope`, crea CASLIBs, itera seg o unv, y limpia.

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

## 4) Módulo: Correlación

**Ruta**
- `src/modules/correlacion/`

**API pública**
- `%correlacion_run(...)`
- Parámetros de entrada:
  - `input_caslib=PROC` — CASLIB de entrada
  - `input_table=_active_input` — tabla promovida por `run_module`
  - `output_caslib=OUT` — CASLIB de salida
  - `troncal_id`, `split`, `scope`, `run_id` — contexto

**Estructura interna**
```
src/modules/correlacion/
  correlacion_run.sas          %correlacion_run — entry point público
  correlacion_contract.sas     %correlacion_contract — validaciones
  impl/
    correlacion_compute.sas    %_correlacion_compute — Pearson + Spearman
    correlacion_report.sas     %_correlacion_report — HTML + Excel con semáforo
```

**Inputs típicos**
- Dataset input (universo o segmento) con variables numéricas.
- Variables numéricas se resuelven según el **modo de ejecución**:

**Modos de ejecución (configurados en `steps/methods/metod_4/step_correlacion.sas`)**

| Modo | `corr_mode` | Variables | Output destino | Prefijo archivo |
|------|-------------|-----------|----------------|----------------|
| Automático | `AUTO` | `cfg_segmentos.num_list` → fallback `cfg_troncales.num_unv` | `reports/` + `tables/` | `correlacion_` |
| Personalizado | `CUSTOM` | `corr_custom_vars` (lista manual del usuario) | `experiments/` | `custom_correlacion_` |

- **AUTO** (por defecto): resuelve variables desde config. Segmento usa `cfg_segmentos.num_list` (si no vacío), fallback a `cfg_troncales.num_unv`. Universo usa `cfg_troncales.num_unv`.
- **CUSTOM**: el usuario especifica variables en `corr_custom_vars` (separadas por espacio). Si `corr_custom_vars` está vacío, se hace fallback automático a AUTO con WARNING.
- Los outputs CUSTOM van a `experiments/` para separar análisis exploratorio de resultados de validación estándar.
- Solo opera sobre variables numéricas (no categóricas).

**Validaciones (contract)**
- Tabla accesible y no vacía (nobs > 0) vía `proc sql count(*)`.
- Lista de variables numéricas no vacía.
- **No usar `table.tableExists`** (no confiable). Usar `proc sql` contra `dictionary.tables` o count directo.

**Cómputo**
- Correlación de **Pearson** (`proc corr outp=`).
- Correlación de **Spearman** (`proc corr spearman outs=`).
- Ambas matrices filtradas a `_type_='CORR'`.

**Reportes — semáforo por |r|**
- `|r| < 0.5` → lightgreen (débil)
- `0.5 ≤ |r| < 0.6` → yellow (moderada)
- `|r| ≥ 0.6` → red (fuerte)

Formato SAS `CorrSignif` aplicado vía `style(column)={backgroundcolor=CorrSignif.}` en ODS.

**Outputs esperados**

*Modo AUTO (validación estándar):*
- `outputs/runs/<run_id>/reports/correlacion_troncal_X_<split>_<scope>.html` — matrices coloreadas
- `outputs/runs/<run_id>/reports/correlacion_troncal_X_<split>_<scope>.xlsx` — hojas Pearson + Spearman
- `outputs/runs/<run_id>/tables/corr_tX_<spl>_<scope>_prsn.sas7bdat` — datos Pearson
- `outputs/runs/<run_id>/tables/corr_tX_<spl>_<scope>_sprm.sas7bdat` — datos Spearman

*Modo CUSTOM (análisis exploratorio):*
- `outputs/runs/<run_id>/experiments/custom_correlacion_troncal_X_<split>_<scope>.html`
- `outputs/runs/<run_id>/experiments/custom_correlacion_troncal_X_<split>_<scope>.xlsx`
- `outputs/runs/<run_id>/experiments/cx_corr_tX_<spl>_<scope>_prsn.sas7bdat`
- `outputs/runs/<run_id>/experiments/cx_corr_tX_<spl>_<scope>_sprm.sas7bdat`

*Naming compacto de tablas .sas7bdat (≤ 32 chars, límite SAS):*
- `<spl>` = `trn` | `oot`
- `<scope>` = `base` | `segNNN`
- Ejemplo: `corr_t1_trn_seg001_prsn` (24 chars)
- Reportes (.html/.xlsx) usan nombres descriptivos completos (sin límite de 32 chars).

**Compatibilidad de contexto**: segmento y universo.

**Cleanup**
- Tablas temporales en `work` (`_corr_pearson`, `_corr_spearman`) se eliminan al finalizar.
- No se usan tablas temporales CAS para outputs (se persisten directamente como `.sas7bdat` vía `libname`).

---

## 5) Reglas para agregar módulos

Para agregar un módulo nuevo:
1. Crear carpeta `src/modules/<modulo>/`.
2. Implementar:
   - `<modulo>_run.sas` (macro pública)
   - `<modulo>_contract.sas` (validaciones)
   - `impl/<modulo>_compute.sas`
   - `impl/<modulo>_report.sas` (si aplica)
3. Crear step de módulo en `steps/methods/metod_N/step_<modulo>.sas`:
   - Check de flag `&run_<modulo>` al inicio (→ skip si 0)
   - Sección de configuración propia (params editables)
   - Crea CASLIBs PROC + OUT
   - Lee `&ctx_scope` para decidir iteración:
     - SEGMENTO → itera via `ctx_segment_*`
     - UNIVERSO → itera via `ctx_universe_*`
   - Cleanup CASLIBs al final
4. Añadir flags en ambos `select_modules.sas` (segmento y universo).
   - Inputs esperados
   - Outputs generados
   - Validaciones
  - Compatibilidad de contexto (`segmento`, `universo`, o ambos)
5. Añadir `%include` en `runner/main.sas` (o como nodo en `.flw`).

Recomendación:
- **Nombres de tablas .sas7bdat** deben respetar el límite de **32 caracteres** de SAS:
  - Formato compacto: `<mod>_t<N>_<spl>_<scope>_<tipo>`
  - Ejemplo: `corr_t1_trn_seg001_prsn` (24 chars), `gini_t2_oot_base` (16 chars)
  - Usar abreviaturas: `trn`/`oot`, `prsn`/`sprm`, `cx_` para CUSTOM
- **Nombres de reportes (.html/.xlsx)** pueden ser descriptivos (sin límite):
  - `correlacion_troncal_1_train_seg001.html`
- Outputs tabulares se persisten como **`.sas7bdat`** vía `libname` + DATA step (no CAS `_save_into_caslib`).
- Rutas de salida separadas: `_report_path` para .html/.xlsx, `_tables_path` para .sas7bdat.
- **Validación de existencia**: usar `proc sql` contra `dictionary.tables` o count directo. **No usar `table.tableExists`**.
- Los outputs se persisten en `reports/` + `tables/` (modo AUTO) o `experiments/` (modo CUSTOM), siguiendo `docs/caslib_lifecycle.md`.
- El módulo debe declarar explícitamente si soporta ejecución en `segmento`, `universo` o ambos.

Ejemplo:
- `gini_troncal_1_train_base.xlsx`
- `gini_troncal_1_train_seg001.xlsx`
- `correlacion_troncal_1_train_base.html`
- `correlacion_troncal_1_oot_seg002.xlsx`
