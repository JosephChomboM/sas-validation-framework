# Framework SAS Viya (CAS) para Validación y Controles Automáticos

Este repositorio contiene un framework modular en **SAS Viya / CAS** para preparar data (train/oot), ejecutar controles de validación (por ejemplo **Gini, PSI**) y generar artefactos (reportes, tablas, logs) de forma **estandarizada y automatizable**.

El diseño prioriza:
- Convenciones determinísticas de rutas y nombres para facilitar loops y paralelización.
- Separación clara entre **data preparada** (`data/processed`) y **artefactos de ejecución** (`outputs/runs`).
- Módulos con API pública estable (`*_run.sas`) y validaciones explícitas (`*_contract.sas`).
- Orquestación centralizada en un **runner** SAS (reemplazo de `.flw/.step`).

---

## 1) Qué hace el proyecto

### 1.1 Flujo funcional (alto nivel)

El usuario configura el framework editando los **steps** (`steps/*.sas`) y luego ejecuta `runner/main.sas`:

1. **Setup del proyecto**: definir ruta raíz.
2. **Carga de configuración + creación de dirs del run**: leer `config.sas`, generar `run_id`, crear `outputs/runs/<run_id>/...`.
3. **Creación de carpetas de data**: `data/raw/`, `data/processed/`, subcarpetas `troncal_X/train/oot/`.
4. **Importación de datos desde ADLS** (opcional, una vez por proyecto): generar raw `.sashdat`.
5. **Partición de data**: por troncal + split (`train/oot`) + scope (`universo/segmento`).

> **Steps 03–05 se ejecutan una sola vez por proyecto** (o cuando se quiera regenerar data). En corridas posteriores, setear `data_prep_enabled=0` en `runner/main.sas` para saltar de Step 02 directo al contexto.

6. **Contexto + módulos** (`steps/context_and_modules.sas`):
   a. **Selección de scope**: elegir UNIVERSO (troncal completa) o SEGMENTO.
   b. **Selección de troncal, split y segmento** (si scope=SEGMENTO).
   c. **Selección de módulos**: habilitar qué controles correr (fillrate, correlacion, gini, etc.).
7. **Ejecución**: cada módulo habilitado lee `ctx_scope` e itera los segmentos o base según corresponda.

Reglas clave:
- La selección de contexto (qué data correr) ocurre **antes** de seleccionar módulos.
- Los métodos (`Metodo 1..N`) son **independientes** entre sí.
- Un solo step de contexto unificado define scope + troncal + split + segmento + módulos.
- Los steps de módulos leen `&ctx_scope` para decidir si iterar segmentos o base.

---

## 2) Estructura del repositorio

Estructura recomendada (resumen):

```
project_root/
  data/
    raw/
      mydataset.sashdat
    processed/
      troncal_1/
        train/
          base.sashdat
          seg001.sashdat
          seg002.sashdat
        oot/
          base.sashdat
          seg001.sashdat
          seg002.sashdat
      troncal_2/
        train/
          base.sashdat
          seg001.sashdat
        oot/
          base.sashdat
          seg001.sashdat

  src/
    common/
      common_public.sas
      fw_paths.sas
      cas_utils.sas
      preparation/
        fw_import_adls_to_cas.sas
        fw_prepare_processed.sas
    dispatch/
      run_module.sas
    modules/
      correlacion/
        correlacion_run.sas
        correlacion_contract.sas
        impl/
          correlacion_compute.sas
          correlacion_report.sas
      gini/...
      psi/...

  runner/
    main.sas                        # entrypoint - incluye steps y orquesta pipeline

  steps/                             # FRONTEND - configuración previa a ejecutar controles
    01_setup_project.sas             # rutas del proyecto
    02_load_config.sas               # carga/validación de config.sas + promote config
    03_create_folders.sas            # creación de carpetas base + troncal_X/train/oot
    04_import_raw_data.sas           # importación ADLS (una vez por proyecto)
    05_partition_data.sas            # particiones troncal/train/oot + universo/segmento
    context_and_modules.sas          # contexto unificado: scope + troncal + split + seg + módulos
    methods/                         # Steps de módulos organizados por método
      metod_1/                       # Método 1: universe (futuro)
      metod_2/                       # Método 2: target (futuro)
      metod_3/                       # Método 3: segmentación (futuro)
      metod_4/                       # Método 4: análisis de variables
        step_correlacion.sas         # correlación (4.3)
        step_gini.sas                # gini (4.3, futuro)
        step_bivariado.sas           # bivariado (4.3, futuro)
        step_estabilidad.sas         # estabilidad (4.2, futuro)
        step_fillrate.sas            # fillrate (4.2, futuro)
        step_missings.sas            # missings (4.2, futuro)
        step_psi.sas                 # psi (4.2, futuro)

  outputs/
    runs/
      <run_id>/
        logs/
        reports/
        images/
        tables/
        manifests/
        experiments/           # outputs de análisis exploratorio (modo CUSTOM)
```

Notas:
- `config.sas` define troncales/segmentos (DATA steps CAS). `casuser.cfg_troncales` y `casuser.cfg_segmentos` son las únicas tablas persistentes en `casuser`. Step 02 las promueve para compatibilidad con background submit.
- `steps/*.sas` modelan el frontend del flujo: un step de contexto unificado (scope + troncal + split + segmento + módulos) seguido de ejecución de módulos.
- Cada módulo tiene su propio step en `steps/methods/metod_N/` que lee `&ctx_scope` para saber si iterar segmentos o base.
- Los módulos se agrupan en sub-métodos: Método 4.2 (estabilidad, fillrate, missings, psi) y Método 4.3 (bivariado, correlacion, gini).
- Todo dato operativo (raw, processed, outputs) usa CASLIBs PATH-based (ver `docs/caslib_lifecycle.md`).
- Step 02 crea las carpetas de output del run (`outputs/runs/<run_id>/...` incluyendo `experiments/`) en cada corrida, independientemente de `data_prep_enabled`.
- Step 03 crea `data/raw/`, `data/processed/`, y subcarpetas `troncal_X/train/` y `troncal_X/oot/` por cada troncal. Solo se ejecuta durante data prep.
- Parámetros específicos de módulos de análisis (`threshold`, `corr_mode`, etc.) **no** viven en `config.sas`; se configuran en el step del módulo correspondiente.
- `def_cld` en `config.sas` define la fecha maxima (YYYYMM) para controles que usan target/PD/XB (ej. Gini). Controles que solo analizan variables (ej. correlacion, PSI) usan `oot_max_mes`.

### 3.0a Ciclo de vida de CASLIBs

Todo bloque que usa CASLIBs sigue estrictamente: **create → promote → work → drop**.
- Cada fase (data prep, ejecución segmento, ejecución universo) crea sus CASLIBs al inicio y los dropea al final.
- `run_module.sas` promueve el input específico (`_active_input`) desde CASLIB `PROC`, ejecuta el módulo, y dropea la tabla promovida.
- Ningún CASLIB sobrevive entre fases; los `.sashdat` en disco persisten.

---

## 3) Convenciones y estándares (para automatización)

### 3.0 CASLIBs y casuser
- **`casuser`** se usa **únicamente** para las tablas de configuración (`cfg_troncales`, `cfg_segmentos`) generadas por `config.sas`.
- Todo dato operativo (raw, processed, outputs) se accede mediante **PATH-based CASLIBs** (GLOBAL) mapeados a carpetas del filesystem, siguiendo `docs/caslib_lifecycle.md`.
- CASLIBs estándar del framework:
  - `RAW` → `data/raw/` (subdirs=0)
  - `PROC` → `data/processed/` (subdirs=1, para acceder subcarpetas troncal/split)
  - `OUT` → `outputs/runs/<run_id>/` (subdirs=1, creado por el runner)
- Los módulos pueden crear CASLIBs scoped adicionales (ej. `MOD_GINI_<run_id>`) y son responsables de su cleanup.

**Restricción SAS open code:** `%if`/`%do` no se permiten fuera de una macro. Todo archivo `.sas` que use lógica condicional debe encapsularla en `%macro ... %mend;`. Esto aplica a `runner/main.sas` (`%macro _main_pipeline`) y a steps individuales como `02`, `04`, `05`, `segmento/context`, `universo/context`.

**Independencia de steps:** cada step carga sus propias dependencias (`%include "&fw_root./src/common/common_public.sas";`) y gestiona su propio ciclo de vida de CASLIBs (create → promote → work → drop). Ningún CASLIB operativo sobrevive entre steps. `casuser` (config) es la única excepción.
- Se usa CASLIB/LIBNAME fijo `OUT` para outputs porque `LIBNAME` en SAS admite máximo 8 caracteres.
- La separación por corrida se mantiene vía path físico `outputs/runs/<run_id>/`.
- Convención estricta de naming operativo: usar solo `RAW` y `PROC` para capas de datos del framework (no usar `RAWDATA` ni `PROCESSED`).

### 3.1 Data preparada: naming determinístico
Dentro de cada `troncal_X/{train|oot}`:
- Universo (troncal completo): `base.sashdat`
- Segmentos numéricos: `seg001.sashdat`, `seg002.sashdat`, ..., `segNNN.sashdat`

Reglas:
- Padding de 3 dígitos para segmentos: `seg%sysfunc(putn(seg_id,z3.))`.
- El split (`train/oot`) y la troncal se expresan por carpeta, no por el nombre de archivo.
- Se accede vía CASLIB `PROC` con la subruta relativa (ej. `troncal_1/train/base.sashdat`).

### 3.2 Artefactos de ejecución por run
Todo output va en:
- `outputs/runs/<run_id>/logs`
- `outputs/runs/<run_id>/reports`
- `outputs/runs/<run_id>/images`
- `outputs/runs/<run_id>/tables`
- `outputs/runs/<run_id>/manifests`
- `outputs/runs/<run_id>/experiments` - outputs exploratorios (modo CUSTOM de módulos)

Reglas:
- Ningún módulo escribe en `data/processed` (solo en `outputs/...`).
- **Tablas .sas7bdat**: se persisten vía `libname` + DATA step directo en `tables/` (no vía CAS `_save_into_caslib`).
- **Reportes (.html/.xlsx)**: se generan con ODS en `reports/`.
- En modo CUSTOM, ambos tipos de output van a `experiments/`.
- **Nombres de datasets SAS** deben respetar el **límite de 32 caracteres**: usar formato compacto `<mod>_t<N>_<spl>_<scope>_<tipo>` (ej. `corr_t1_trn_seg001_prsn`). Reportes pueden usar nombres descriptivos completos.

### 3.3 API pública de módulos
Cada módulo implementa:
- `src/modules/<modulo>/<modulo>_run.sas`  (macro pública `%<modulo>_run(...)`)
- `src/modules/<modulo>/<modulo>_contract.sas` (validaciones)
- `src/modules/<modulo>/impl/*` (cómputo, reportes, plots, utilidades internas)

### 3.4 Validaciones obligatorias
Antes de ejecutar un módulo:
- existencia del input (tabla/archivo) - vía `proc sql` contra `dictionary.tables` o count directo (**no usar `table.tableExists`**)
- columnas requeridas para el módulo
- tipos/formatos mínimos (cuando aplique)

---

## 4) Orden de ejecución (segmentos primero)

Si una troncal tiene segmentación activa (variable segmentadora y N segmentos):
1. Ejecutar módulos en **cada segmento** (train y oot).
2. Ejecutar módulos en el **universo de la troncal** (train y oot).

Motivo:
- Permite detectar problemas segmentados temprano (calidad de data, drift, métricas) antes del agregado troncal.
- Favorece paralelización natural por segmento.

---

## 5) Steps como frontend del framework

Los archivos `steps/*.sas` actúan como el **frontend** del framework. El usuario edita estos archivos para configurar su run, y luego ejecuta `runner/main.sas`.

### 5.1 Flujo de steps

| Step | Archivo | Configura |
|------|---------|-----------|
| 01 | `steps/01_setup_project.sas` | Rutas del proyecto |
| 02 | `steps/02_load_config.sas` | Carga `config.sas` + promote config + dirs de output del run |
| 03 | `steps/03_create_folders.sas` | Carpetas de data + `troncal_X/train/oot/` (solo data prep) |
| 04 | `steps/04_import_raw_data.sas` | Importación ADLS (una vez por proyecto) |
| 05 | `steps/05_partition_data.sas` | Particiones por troncal/split/scope |
| - | `steps/context_and_modules.sas` | Contexto (scope + troncal + split + seg) + módulos |
| - | `steps/methods/metod_4/step_correlacion.sas` | Config + ejecución correlación |
| - | `steps/methods/metod_4/step_psi.sas` | Config + ejecución PSI |
| - | `steps/methods/metod_4/step_gini.sas` | (futuro) |
| - | `steps/methods/metod_4/step_*.sas` | estabilidad, fillrate, missings, psi, bivariado (futuro) |

### 5.2 Cómo usar
1. Configurar rutas/config (Steps 01–02). Siempre se ejecutan.
2. **Primera corrida**: `data_prep_enabled=1` → ejecutar Steps 03–05 (carpetas, ADLS, partición).
   **Corridas posteriores**: `data_prep_enabled=0` → saltar Steps 03–05.
3. **Contexto + módulos**: configurar scope, troncal, split, segmento y módulos a correr en `steps/context_and_modules.sas`.
4. Los steps de módulos se ejecutan leyendo `ctx_scope` para iterar segmentos (SEGMENTO) o base/troncal (UNIVERSO).

### 5.3 Convención de IDs `_id_*`
Cada step documenta variables `_id_*` que representan campos de un formulario de UI:
- Contexto (`context_and_modules.sas`): `_id_scope`, `_id_troncal_id`, `_id_split`, `_id_seg_id`, `_id_seg_num`
- Módulos (`context_and_modules.sas`): `_id_run_estabilidad`, `_id_run_fillrate`, `_id_run_missings`, `_id_run_psi`, `_id_run_bivariado`, `_id_run_correlacion`, `_id_run_gini`
- Módulos: params específicos dentro de cada step de módulo (ej. `corr_mode`, `corr_custom_vars`, `psi_mode`, `psi_n_buckets`)

Ver `design.md §5` para el contrato completo.

---

## 6) Cómo agregar un módulo nuevo

1. Crear carpeta `src/modules/<nuevo_modulo>/`.
2. Implementar:
   - `<nuevo_modulo>_run.sas`
   - `<nuevo_modulo>_contract.sas`
   - `impl/<nuevo_modulo>_compute.sas`
   - `impl/<nuevo_modulo>_report.sas`
3. Crear step del módulo en `steps/methods/metod_N/step_<nuevo_modulo>.sas`:
   - Check de flag `&run_<nuevo_modulo>` al inicio (→ skip si 0)
   - Sección de configuración propia del módulo (params editables)
   - Crea CASLIBs PROC + OUT
   - Lee `&ctx_scope` para iterar:
     - SEGMENTO → usa `ctx_troncal_id`, `ctx_n_segments`, `ctx_seg_id`
     - UNIVERSO → usa `ctx_troncal_id`
   - Cleanup CASLIBs al final
4. Añadir flag `run_<nuevo_modulo>` en `steps/context_and_modules.sas`.
5. Documentar inputs/outputs en `docs/module_catalog.md`.
6. Añadir `%include` en `runner/main.sas` o como nodo en `.flw`.

Ver `steps/methods/metod_4/step_correlacion.sas` y `src/modules/correlacion/` como implementación de referencia.

---

## 7) Documentación adicional

- Diseño y contratos: `docs/design.md`
- Catálogo de módulos: `docs/module_catalog.md`
