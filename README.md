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

1. **Setup del proyecto** (Step 01): define ruta raíz → crea carpetas automáticamente.
2. **Importación de datos** (Step 02): configura ADLS o indica que el raw ya existe.
3. **Config troncal/segmento** (Step 03 + `config.sas`): define troncales, ventanas train/oot, segmentación.
4. **Selección de métodos** (Step 04): elige qué módulos ejecutar (gini, psi, etc.).
5. **Ejecución automática** (backend del runner):
   - Sesión CAS + generación de `run_id`.
   - (Opcional) Importación parquet desde ADLS → `data/raw/`.
   - Preparación de data processed (train/oot/segmentos).
   - Ejecución de módulos: **segmentos primero**, luego troncal (universo).
   - Generación de artefactos en `outputs/runs/<run_id>/...`.
   - Cleanup de CASLIBs y cierre.

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
      run_method.sas
      run_module.sas
    modules/
      gini/...
      psi/...

  runner/
    main.sas                   # entrypoint (reemplaza .flw/.step)

  steps/
    import_raw_data.sas        # pseudo-step: config ADLS import (_id_*)
    select_troncal_segment.sas # pseudo-step: selección troncal/segmento (_id_*)
    select_methods.sas         # pseudo-step: selección de métodos (_id_*)

  outputs/
    runs/
      <run_id>/
        logs/
        reports/
        images/
        tables/
        manifests/
```

Notas:
- `config.sas` es generado desde HTML y define la configuración del run. Las tablas `casuser.cfg_troncales` y `casuser.cfg_segmentos` son las **únicas** que residen en `casuser`.
- Todo dato operativo (raw, processed, outputs) usa CASLIBs PATH-based (ver `docs/caslib_lifecycle.md`).
- `steps/*.sas` **no** son generados por HTML. Son archivos `.sas` que simulan el contrato de UI de `.step` mediante comentarios con variables `_id_*` (ver sección 5).

---

## 3) Convenciones y estándares (para automatización)

### 3.0 CASLIBs y casuser
- **`casuser`** se usa **únicamente** para las tablas de configuración (`cfg_troncales`, `cfg_segmentos`) generadas por `config.sas`.
- Todo dato operativo (raw, processed, outputs) se accede mediante **PATH-based CASLIBs** (GLOBAL) mapeados a carpetas del filesystem, siguiendo `docs/caslib_lifecycle.md`.
- CASLIBs estándar del framework:
  - `RAW` → `data/raw/` (subdirs=0)
  - `PROCESSED` → `data/processed/` (subdirs=1, para acceder subcarpetas troncal/split)
  - `OUT_<run_id>` → `outputs/runs/<run_id>/` (subdirs=1, creado por el runner)
- Los módulos pueden crear CASLIBs scoped adicionales (ej. `MOD_GINI_<run_id>`) y son responsables de su cleanup.

### 3.1 Data preparada: naming determinístico
Dentro de cada `troncal_X/{train|oot}`:
- Universo (troncal completo): `base.sashdat`
- Segmentos numéricos: `seg001.sashdat`, `seg002.sashdat`, ..., `segNNN.sashdat`

Reglas:
- Padding de 3 dígitos para segmentos: `seg%sysfunc(putn(seg_id,z3.))`.
- El split (`train/oot`) y la troncal se expresan por carpeta, no por el nombre de archivo.
- Se accede vía CASLIB `PROCESSED` con la subruta relativa (ej. `troncal_1/train/base.sashdat`).

### 3.2 Artefactos de ejecución por run
Todo output va en:
- `outputs/runs/<run_id>/logs`
- `outputs/runs/<run_id>/reports`
- `outputs/runs/<run_id>/images`
- `outputs/runs/<run_id>/tables`
- `outputs/runs/<run_id>/manifests`

Reglas:
- Ningún módulo escribe en `data/processed` (solo en `outputs/...`).
- Los outputs se persisten vía CASLIB `OUT_<run_id>` o un CASLIB scoped del módulo.

### 3.3 API pública de módulos
Cada módulo implementa:
- `src/modules/<modulo>/<modulo>_run.sas`  (macro pública `%<modulo>_run(...)`)
- `src/modules/<modulo>/<modulo>_contract.sas` (validaciones)
- `src/modules/<modulo>/impl/*` (cómputo, reportes, plots, utilidades internas)

### 3.4 Validaciones obligatorias
Antes de ejecutar un módulo:
- existencia del input (tabla/archivo)
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

## 5) “Pseudo-steps” en SAS (simulación de .step)

Como no se utilizarán archivos `.step`, se crean scripts `.sas` que actúan como “plantillas” de step y documentan el **contrato de UI** mediante comentarios.

Importante:
- Estas variables `_id_*` **no** provienen del HTML que genera `configs/config.sas`.
- Se usan para mantener un estándar de naming y para permitir que futuras herramientas (o desarrollos internos) mapeen entradas de UI hacia parámetros SAS de forma consistente.

### 5.1 Convención de IDs `_id_*`
Ejemplos (referenciales):
- `_id_adls_storage`, `_id_adls_container`, `_id_adls_parquet_path` (ADLS import)
- `_id_import_enabled`, `_id_raw_table_name` (activación y tabla destino)
- `_id_troncal_selector`
- `_id_troncal_list`
- `_id_segment_var`
- `_id_n_segments`
- `_id_methods_selected`

Los archivos `steps/*.sas` documentan qué IDs existirían en un `.step` y cómo deberían mapearse.

---

## 6) Cómo agregar un módulo nuevo

1. Crear carpeta `src/modules/<nuevo_modulo>/`.
2. Implementar:
   - `<nuevo_modulo>_run.sas`
   - `<nuevo_modulo>_contract.sas`
   - `impl/<nuevo_modulo>_compute.sas`
   - `impl/<nuevo_modulo>_report.sas`
3. Registrar el módulo en `configs/registry/modules_registry.sas`.
4. Documentar inputs/outputs en `docs/module_catalog.md`.

---

## 7) Documentación adicional

- Diseño y contratos: `docs/design.md`
- Catálogo de módulos: `docs/module_catalog.md`
