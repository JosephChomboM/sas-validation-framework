# Diseño del Framework (SAS Viya / CAS)

## 1) Alcance

Este documento describe:
- Componentes del framework y responsabilidades.
- Steps como frontend de configuración del usuario.
- Contratos de rutas y naming.
- Contrato de configuración vía `config.sas` (tablas CAS) y `steps/*.sas` (parámetros).
- Orden de ejecución: **segmentos primero**, luego troncal.

---

## 2) Arquitectura lógica

### 2.1 Capas

1) **Steps (Frontend)**
- Archivos `steps/*.sas` que actúan como formularios de configuración.
- El usuario edita estos archivos para definir parámetros del run.
- Se ejecutan secuencialmente al inicio de `runner/main.sas`.
- Flujo de steps:
  - `01_setup_project.sas` → ruta raíz del proyecto + creación de carpetas
  - `02_import_raw_data.sas` → parámetros ADLS + nombre de tabla raw
  - `03_select_troncal_segment.sas` → referencia a config.sas para troncales/segmentos
  - `04_select_methods.sas` → módulos a ejecutar + label del run

2) **Configuración**
- Fuente: `config.sas` (generado desde HTML).
- Contiene DATA steps que crean `casuser.cfg_troncales` y `casuser.cfg_segmentos`.
- Los parámetros de usuario (rutas, ADLS, métodos) viven en `steps/*.sas`, no en config.

3) **Common**
- Utilidades reutilizables:
  - paths
  - logging
  - validaciones genéricas
  - utilidades CAS (existence, nobs, load/save)
  - preparación de data raw → processed

4) **Dispatch**
- Orquestación de ejecución:
  - `run_method.sas`: ejecuta un método (conjunto de módulos)
  - `run_module.sas`: ejecuta un módulo en un contexto dado (troncal/split/segmento)

5) **Modules**
- Implementación por control:
  - API pública (`*_run.sas`)
  - Validaciones (`*_contract.sas`)
  - Implementación interna (`impl/`)

6) **Runner**
- `runner/main.sas`: entrypoint único, reemplaza `.flw`.
- Ejecuta:
  - **Frontend**: incluye steps 01–04 (setea macro vars, crea dirs).
  - **Backend**: CAS init → config.sas → (ADLS import) → prepare_processed → run_methods → cleanup.

---

## 3) Contratos de rutas y naming

### 3.1 Raw
- `data/raw/mydataset.sashdat` (dataset maestro)

### 3.2 Processed (inputs de controles)
Cada troncal se materializa por split:

- Universo:
  - `data/processed/troncal_X/train/base.sashdat`
  - `data/processed/troncal_X/oot/base.sashdat`

- Segmentos (numéricos):
  - `data/processed/troncal_X/train/segNNN.sashdat`
  - `data/processed/troncal_X/oot/segNNN.sashdat`

Reglas:
- `base.sashdat` es siempre el universo.
- `segNNN.sashdat` usa padding de 3 dígitos (001..999).
- No se incluyen “train/oot” ni “troncal” en el nombre del archivo (ya están en la ruta).

### 3.3 Outputs por run
- `outputs/runs/<run_id>/logs`
- `outputs/runs/<run_id>/reports`
- `outputs/runs/<run_id>/images`
- `outputs/runs/<run_id>/tables`
- `outputs/runs/<run_id>/manifests`

---

## 4) Contrato de configuración: `config.sas`

### 4.1 Principio
El `config.sas` declara parámetros; el framework ejecuta. Se evita lógica de orquestación en el config.

**Las tablas de configuración (`casuser.cfg_troncales`, `casuser.cfg_segmentos`) son las únicas tablas que residen en `casuser`.** Todo dato operativo (raw, processed, outputs) usa CASLIBs PATH-based dedicados.

### 4.2 Parámetros por troncal
Se recomienda declarar, por troncal:
- Identificadores:
  - `troncal_id` (ej. 1, 2)
- Variables:
  - target
  - pd / xb (según aplique)
  - monto
  - mes_var (variable de corte temporal, por ejemplo YYYYMM)
- Rango train/oot:
  - train_min_mes, train_max_mes
  - oot_min_mes, oot_max_mes
- Listas de variables:
  - var_num_list, var_cat_list
  - drv_num_list, drv_cat_list
- Segmentación (opcional):
  - var_seg (variable segmentadora)
  - n_segments (N)

Nota:
- Estas “listas” se almacenan como strings (separadas por espacio) para permitir iteración fácil.

### 4.3 Parámetros por segmento (opcional)
Si el usuario define overrides por segmento, se recomienda declarar por (troncal, seg_id):
- var_num_list, var_cat_list
- drv_num_list, drv_cat_list

Regla:
- Si no hay override, el segmento hereda las listas del troncal.

---

## 5) Steps como frontend (reemplazo de .step)

### 5.1 Concepto
Los archivos `steps/*.sas` actúan como el **frontend** del framework: son el punto de entrada donde el usuario configura todos los parámetros antes de ejecutar el pipeline.

En SAS Viya Studio, un `.step` ofrece un formulario gráfico. Como no se utilizan `.step`, los archivos `steps/*.sas` simulan esa experiencia:
- Cada archivo es un **formulario editable** con variables `_id_*` documentadas.
- El usuario modifica los valores `%let` según su caso de uso.
- Al ejecutar `runner/main.sas`, los steps se incluyen secuencialmente y setean las macro variables.
- Algunos steps ejecutan acciones automáticas (ej. Step 01 crea carpetas).

### 5.2 Flujo de steps

| Step | Archivo | Qué configura | Macro vars que setea |
|------|---------|---------------|---------------------|
| 01 | `steps/01_setup_project.sas` | Ruta raíz del proyecto | `&fw_root` |
| 02 | `steps/02_import_raw_data.sas` | Importación ADLS | `&adls_import_enabled`, `&adls_storage`, `&adls_container`, `&adls_parquet_path`, `&raw_table` |
| 03 | `steps/03_select_troncal_segment.sas` | Troncales y segmentos | (referencia a `config.sas`) |
| 04 | `steps/04_select_methods.sas` | Módulos a ejecutar | `&methods_list`, `&run_label` |

**Step 01** es especial: además de setear `&fw_root`, crea automáticamente la estructura de carpetas (`data/raw`, `data/processed`, `outputs/runs`) si no existe.

**Step 03** no setea macro variables directamente. La configuración de troncales/segmentos requiere DATA steps (tablas CAS), por lo que se define en `config.sas`. Step 03 documenta el contrato `_id_*` y referencia a `config.sas`.

### 5.3 Convención de IDs `_id_*`
Los IDs de UI se nombran como: `_id_<entidad>_<campo>`

Ejemplos:
- `_id_project_root` (Step 01)
- `_id_import_enabled`, `_id_adls_storage`, `_id_adls_container`, `_id_adls_parquet_path`, `_id_raw_table_name` (Step 02)
- `_id_troncal_id`, `_id_var_seg`, `_id_n_segments`, `_id_train_min_mes`, `_id_oot_max_mes` (Step 03 / config.sas)
- `_id_methods_select`, `_id_run_label` (Step 04)

### 5.4 Relación entre steps y config.sas
- **Steps**: parámetros simples (rutas, flags, listas). El usuario los edita como un formulario.
- **config.sas**: configuración compleja (DATA steps que generan tablas CAS por troncal/segmento). Generado desde HTML o editado manualmente.
- Ambos se cargan por `runner/main.sas`: primero steps (frontend), luego config (backend).

---

## 6) Orden de ejecución

Regla:
- Si existe segmentación en una troncal:
  1. Ejecutar todos los módulos para cada segmento (train y oot).
  2. Ejecutar los módulos para el troncal (universo: base) en train y oot.

Motivo:
- Permite detectar issues por subpoblación de forma temprana.
- Habilita paralelización natural por segmento.

---

## 7) Patrones de implementación recomendados

### 7.1 Resolver único de paths
Implementar `src/common/fw_paths.sas` con una macro pública que construya rutas de processed, evitando hardcode:
- `%fw_path_processed(outvar=, troncal_id=, split=, seg_id=)`
  - si `seg_id` vacío: devuelve `troncal_X/<split>/base.sashdat`
  - si `seg_id` presente: devuelve `troncal_X/<split>/segNNN.sashdat`
- Estas rutas son **relativas al CASLIB `PROCESSED`** (con subdirs habilitado), no a `casuser`.

### 7.2 CAS utility macros
Implementar `src/common/cas_utils.sas` con las macros baseline definidas en `docs/caslib_lifecycle.md`:
- `%_create_caslib(...)` — crea CASLIB PATH-based
- `%_drop_caslib(...)` — dropea CASLIB y opcionalmente sus tablas
- `%_load_cas_data(...)` — carga .sashdat desde CASLIB
- `%_save_into_caslib(...)` — guarda tabla CAS como .sashdat
- `%_promote_castable(...)` — promueve tabla (temporal; el caller debe limpiar)

Estas macros se incluyen vía `src/common/common_public.sas`.

### 7.3 Importación opcional desde ADLS
`fw_import_adls_to_cas` (en `src/common/preparation/`) permite:
- Crear CASLIB temporal `LAKEHOUSE` apuntando a Azure Data Lake Storage (parquet).
- Crear CASLIB `RAW` (PATH→`data/raw/`).
- Cargar tabla parquet → CAS → persistir como `.sashdat` en `data/raw/`.
- Cleanup: dropear CASLIB `LAKEHOUSE` al finalizar.
- Controlado por `&adls_import_enabled` (seteado en `steps/02_import_raw_data.sas`); si vale `0` se salta completamente.

### 7.4 Preparación idempotente
`fw_prepare_processed` debe:
- Crear CASLIB `RAW` (PATH→`data/raw/`) y CASLIB `PROCESSED` (PATH→`data/processed/`, subdirs=1)
- Leer raw desde CASLIB `RAW`, filtrar por ventanas mes, guardar en CASLIB `PROCESSED`
- Sobrescribir outputs processed de manera controlada
- Limpiar tablas temporales CAS (en `casuser`)
- Loggear conteos (nobs) para auditoría mínima
- **No dejar tablas operativas en `casuser`**; solo temporales que se dropean al final

### 7.5 Contratos y validaciones
Cada módulo debe fallar temprano con mensajes claros si:
- faltan columnas
- el input está vacío
- el split/segmento no existe

---

## 8) Decisiones explícitas del proyecto

- No se usa JSON como fuente de configuración; solo `config.sas` + `steps/*.sas`.
- No se usan `.flw` ni `.step` como artefactos ejecutables; se reemplaza con `runner/main.sas` + `steps/*.sas` como frontend.
- Los parámetros de usuario (rutas, ADLS, métodos) viven en `steps/*.sas`; la configuración de troncales/segmentos en `config.sas`.
- Se mantienen archivos al mismo nivel en `train/` y `oot/` (no carpetas por segmento).
- **`casuser` es exclusivo para tablas de configuración** (`cfg_troncales`, `cfg_segmentos`). Todo dato operativo usa CASLIBs PATH-based (ver `docs/caslib_lifecycle.md`).
- Cada paso que crea un CASLIB o promueve tablas es responsable de su cleanup.
