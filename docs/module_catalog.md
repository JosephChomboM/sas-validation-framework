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

**Acceso a datos:** los inputs se leen desde el CASLIB `PROC` (PATH-based, con subdirs habilitado, mapeado a `data/processed/`). `casuser` se usa para tablas de configuración (`cfg_troncales`, `cfg_segmentos`) y para tablas temporales/intermedias de módulos (reemplazando `work`). Los módulos limpian sus tablas temporales en `casuser` al finalizar.

Convención de naming operativo:
- `RAW` para `data/raw/`
- `PROC` para `data/processed/`
- Evitar aliases alternos como `RAWDATA` o `PROCESSED`.

**Regla de orquestación (contexto primero):**
1. Se selecciona/promueve el contexto de datos (`troncal_id`, `scope`, `split`, `seg_id` si aplica).
2. Luego se seleccionan módulos por Método (`Metodo 1..N`).
3. Se ejecuta el subflow de módulos para ese contexto.

**Selección de módulos por Método:**
- `context_and_modules.sas` es el único step donde se habilitan/deshabilitan módulos.
- Los flags `%let run_<modulo> = 1|0;` controlan qué módulos se ejecutan.
- Cada módulo tiene su propio step file independiente (`step_<modulo>.sas`).
- En el `.flw`, cada step de módulo es un nodo que puede ejecutarse vía background submit.
- Cada step de módulo checa su flag, lee `&ctx_scope`, crea CASLIBs PROC/OUT, itera, y limpia.

**Contexto unificado:**
- `steps/context_and_modules.sas` ? seleccionar scope (UNIVERSO|SEGMENTO), troncal, split, segmento + módulos habilitados
- `steps/methods/metod_N/step_<modulo>.sas` ? cada step lee `ctx_scope` para iterar base o segmentos

El runner pasa el contexto (`troncal_id`, `split`, `seg_id` opcional, `run_id`) a cada modulo via `%run_module`.

**Modos de `run_module`:**
- `dual_input=0` (default): promueve un solo input como `_active_input`. Para modulos single-input (correlacion, gini, etc.).
- `dual_input=1`: promueve train + oot como `_train_input` y `_oot_input`. Para modulos que comparan ambos splits (PSI, etc.). El parametro `split=` se ignora.

**Ciclo de vida de CASLIBs en ejecución:**
- Cada step de módulo (`step_<modulo>.sas`) crea CASLIBs `PROC` y `OUT` al inicio.
- Lee `&ctx_scope` para determinar si itera segmentos (SEGMENTO) o base/troncal (UNIVERSO).
- Por cada contexto, `run_module.sas` promueve el input específico desde `PROC` como tabla `_active_input` (vía `%_promote_castable`), ejecuta el módulo, y dropea la tabla promovida.
- Al final del step se dropean `PROC` y `OUT` (archivos en disco persisten).
- Patrón obligatorio: **create ? promote ? work ? drop**.

**Restricciones SAS**: ver `design.md §7` y `design.md §8`.

**Parámetros específicos de módulos:**
- Parámetros como `threshold`, `num_rounds`, `num_bins`, `corr_mode` y similares **no** se declaran en `config.sas`.
- Se configuran en el step del módulo correspondiente (`steps/methods/metod_N/step_<modulo>.sas`).
- `config.sas` solo contiene parámetros estructurales de troncales/segmentos.

**Flag de habilitación:**
- Cada step de módulo checa `&run_<modulo>` al inicio. Si vale 0, se salta la ejecución.
- Estos flags se setean en `steps/context_and_modules.sas`.

Orden de ejecución recomendado:
- Segmentos primero (si existen), luego universo.

### 1.1 Contrato mínimo de contexto para módulos

Todo módulo debe aceptar o derivar estos campos de contexto:
- `troncal_id`
- `scope` (`segmento` | `universo`)
- `split` (`train` | `oot`)
- `seg_id` (obligatorio solo si `scope=segmento`)
- `run_id`

**Fecha de corte (`def_cld` vs `oot_max_mes`):**
- Controles que usan target, PD o XB (ej. Gini) deben filtrar datos hasta `def_cld` (fecha maxima de cierre de default, YYYYMM).
- Controles que solo analizan variables sin target (ej. correlacion, PSI) usan `oot_max_mes` como fecha maxima.
- Ambos campos se declaran en `config.sas` por troncal.

Si el contexto no está completo, el módulo debe fallar temprano con mensaje claro.

### 1.2 Matriz Método ? módulos (contrato funcional)

Cada Método agrupa módulos lógicamente. Los steps de módulos están en `steps/methods/metod_N/`.

| Método   | Sub-método | Carpeta                  | Módulos (steps)                          |
| -------- | ---------- | ------------------------ | ---------------------------------------- |
| Metodo 1 | 1.1        | `steps/methods/metod_1/` | **universe**                             |
| Metodo 2 | 2.1        | `steps/methods/metod_2/` | **target**                               |
| Metodo 3 | -          | `steps/methods/metod_3/` | segmentacion (futuro)                    |
| Metodo 4 | 4.2        | `steps/methods/metod_4/` | estabilidad, fillrate, missings, **psi** |
| Metodo 4 | 4.3        | `steps/methods/metod_4/` | bivariado, **correlación**, gini         |

Los sub-métodos organizan la selección en el UI y las carpetas de output (`reports/METOD1.1/`, `reports/METOD2.1/`, `reports/METOD4.2/`, `reports/METOD4.3/`).

Cada módulo-step es independiente: checa `&run_<modulo>`, lee `&ctx_scope`, crea CASLIBs, itera seg o unv, y limpia.

---

## 2) Módulo: Universe (Método 1.1)

**Fecha de corte:** Universe analiza la composición del datos (cuentas, montos) sin usar target/PD/XB, por lo que la fecha maxima de análisis es `oot_max_mes`.

**Ruta**
- `src/modules/universe/`

**API pública**
- `%universe_run(...)`
- Parámetros de entrada:
  - `input_caslib=PROC` - CASLIB de entrada
  - `train_table=_train_input` - tabla TRAIN promovida por `run_module`
  - `oot_table=_oot_input` - tabla OOT promovida por `run_module`
  - `output_caslib=OUT` - CASLIB de salida
  - `troncal_id`, `scope`, `run_id` - contexto

**Nota arquitectónica:** Universe compara TRAIN vs OOT. Usa `run_module.sas` con `dual_input=1`.

**Estructura interna**
```
src/modules/universe/
  universe_run.sas              %universe_run - entry point público
  universe_contract.sas         %universe_contract - validaciones
  impl/
    universe_compute.sas        %_univ_describe_id - evolutivo cuentas + duplicados
                                %_univ_bandas_cuentas - bandas ±2s (TRAIN ? OOT)
                                %_univ_evolutivo_monto - suma monto por periodo
                                %_univ_describe_monto - media monto por periodo
    universe_report.sas         %_universe_report - HTML + Excel + JPEG
```

**Inputs típicos**
- Dos datasets: TRAIN y OOT, promovidos como `_train_input` y `_oot_input`.
- Variables resueltas desde `casuser.cfg_troncales`:
  - `byvar` (variable temporal, ej. YYYYMM) - requerida
  - `id_var_id` (identificador de cuenta) - requerido
  - `monto` (variable de monto) - opcional (WARNING si ausente)

**Validaciones (contract)**
- Tabla TRAIN accesible y no vacía (nobs > 0).
- Tabla OOT accesible y no vacía (nobs > 0).
- `byvar` presente en ambas tablas.
- `id_var` presente en ambas tablas.
- `monto_var` presente (solo WARNING si falta; análisis de monto se omite).

**Cómputo**
- Evolutivo de cuentas: PROC FREQ por periodo.
- Detección de duplicados: count por `byvar` + `id_var` having N > 1.
- Bandas ±2s: mean/std se calculan desde TRAIN y se aplican a OOT via macrovars globales.
- Evolutivo monto: suma por periodo (PROC SQL).
- Media monto: PROC MEANS por periodo.

**Tablas temporales (casuser)** - se eliminan al finalizar:
- `_univ_train`, `_univ_oot` - copias de trabajo
- `_univ_evolut_cuenta`, `_univ_dup`, `_univ_sindup`, `_univ_freq_cuentas`
- `_univ_sum_monto`, `_univ_evolut_monto`, `_univ_evolut_monto2`

**No persiste tablas .sas7bdat** (análisis visual solamente).

**Reportes**
- `outputs/runs/<run_id>/reports/METOD1.1/<prefix>_train.html` - gráficos TRAIN
- `outputs/runs/<run_id>/reports/METOD1.1/<prefix>_oot.html` - gráficos OOT
- `outputs/runs/<run_id>/reports/METOD1.1/<prefix>.xlsx` - Excel multi-hoja (TRAIN + OOT)
- `outputs/runs/<run_id>/images/METOD1.1/<prefix>_*.jpeg` - gráficos JPEG independientes

Formato de imagen: JPEG. HTML usa `bitmap_mode=inline`.

**Compatibilidad de contexto**: segmento y universo.

---

## 2.5) Modulo: Target (Metodo 2.1)

**Fecha de corte:** Target analiza la variable target (ratio de default), por lo que la fecha maxima de analisis es `def_cld` (fecha de cierre de default). El parametro `timedefault` filtra datos donde `byvar <= def_cld` (default cerrado).

**Ruta**
- `src/modules/target/`

**API publica**
- `%target_run(...)`
- Parametros de entrada:
  - `input_caslib=PROC` - CASLIB de entrada
  - `train_table=_train_input` - tabla TRAIN promovida por `run_module`
  - `oot_table=_oot_input` - tabla OOT promovida por `run_module`
  - `output_caslib=OUT` - CASLIB de salida
  - `troncal_id`, `scope`, `run_id` - contexto

**Nota arquitectonica:** Target compara TRAIN vs OOT. Usa `run_module.sas` con `dual_input=1`.

**Estructura interna**
```
src/modules/target/
  target_run.sas              %target_run - entry point publico
  target_contract.sas         %target_contract - validaciones
  impl/
    target_compute.sas        %_target_describe - evolutivo RD + materialidad
                              %_target_bandas - bandas +/-2s (TRAIN -> OOT)
                              %_target_ponderado_promedio - RD ponderado por monto (promedio)
                              %_target_ponderado_suma - RD ponderado por monto (suma) + ratio
    target_report.sas         %_target_report - HTML + Excel + JPEG
```

**Inputs tipicos**
- Dos datasets: TRAIN y OOT, promovidos como `_train_input` y `_oot_input`.
- Variables resueltas desde `casuser.cfg_troncales`:
  - `byvar` (variable temporal, ej. YYYYMM) - requerida
  - `target` (variable target binaria) - requerida
  - `monto` (variable de monto) - opcional (WARNING si ausente)
  - `def_cld` (fecha maxima de cierre default, YYYYMM) - requerida

**Validaciones (contract)**
- Tabla TRAIN accesible y no vacia (nobs > 0).
- Tabla OOT accesible y no vacia (nobs > 0).
- `byvar` presente en ambas tablas.
- `target` presente en ambas tablas.
- `monto` presente (solo WARNING si falta; analisis ponderados se omiten).
- `def_cld` definido y no vacio.

**Computo**
- **Evolutivo RD**: mean(target) por periodo, con filtro `byvar <= def_cld`.
- **Materialidad**: PROC FREQ cruzando byvar * target.
- **Diferencia relativa**: compara promedio de primeros 3 meses vs ultimos 3 meses (o primer/ultimo mes si < 6 meses).
- **Bandas +/-2s**: mean/std se calculan desde TRAIN y se aplican a OOT via macrovars globales (`_tgt_global_avg`, `_tgt_std_monthly`).
- **Target ponderado promedio**: sum(target*monto)/sum(monto) por periodo, con bandas.
- **Target ponderado suma**: sum(target*monto) por periodo + ratio normalizado sobre monto total.

**Tablas temporales (casuser)** - se eliminan al finalizar:
- `_tgt_train`, `_tgt_oot` - copias de trabajo
- `_tgt_evolut_target`, `_tgt_monthly`, `_tgt_monthly_pond`, `_tgt_monthly_sum_pond`, `_tgt_monthly_norm`
- `_tgt_first_months`, `_tgt_last_months`, `_tgt_first_mean`, `_tgt_last_mean`, `_tgt_results`

**No persiste tablas .sas7bdat** (analisis visual solamente).

**Reportes**
- `outputs/runs/<run_id>/reports/METOD2.1/<prefix>_train.html` - graficos TRAIN
- `outputs/runs/<run_id>/reports/METOD2.1/<prefix>_oot.html` - graficos OOT
- `outputs/runs/<run_id>/reports/METOD2.1/<prefix>.xlsx` - Excel multi-hoja (TRAIN + OOT)
- `outputs/runs/<run_id>/images/METOD2.1/<prefix>_*.jpeg` - graficos JPEG independientes

Formato de imagen: JPEG. HTML usa `bitmap_mode=inline`.

**Compatibilidad de contexto**: segmento y universo.

---

## 3) Modulo: Gini

**Fecha de corte:** Gini usa target/PD/XB, por lo que la fecha maxima de análisis es `def_cld`.

**Ruta**
- `src/modules/gini/`

**API pública**
- `%gini_run(...)`
- Parámetros de entrada incluyen:
  - `input_caslib=PROC` - CASLIB de entrada
  - `input_table=_active_input` - tabla promovida por `run_module`
  - `output_caslib=OUT` - CASLIB de salida
  - `troncal_id`, `split`, `scope`, `run_id` - contexto

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

## 3) Módulo: PSI (Population Stability Index)

**Fecha de corte:** PSI compara distribuciones de variables (no usa target/PD/XB), por lo que la fecha maxima de análisis es `oot_max_mes`.

**Ruta**
- `src/modules/psi/`

**API pública**
- `%psi_run(...)`
- Parámetros de entrada:
  - `input_caslib=PROC` - CASLIB de entrada
  - `train_table=_train_input` - tabla TRAIN promovida por `run_module`
  - `oot_table=_oot_input` - tabla OOT promovida por `run_module`
  - `output_caslib=OUT` - CASLIB de salida
  - `troncal_id`, `scope`, `run_id` - contexto

**Nota arquitectónica:** PSI compara TRAIN vs OOT - necesita DOS tablas promovidas simultaneamente. Usa `run_module.sas` con `dual_input=1`, que promueve ambas tablas (_train_input, _oot_input) automaticamente. El split del contexto se ignora (PSI siempre usa train+oot).

**Estructura interna**
```
src/modules/psi/
  psi_run.sas              %psi_run - entry point público
  psi_contract.sas         %psi_contract - validaciones
  impl/
    psi_compute.sas        %_psi_calc - PSI para una variable (core)
                           %_psi_compute - orquestador: variables × periodos
    psi_report.sas         %_psi_report - HTML + Excel (con Graficos) + JPEG
                           %_psi_plot_tendencia - serie temporal por variable
```

**Inputs típicos**
- Dos datasets: TRAIN (development) y OOT (out-of-time), promovidos como `_psi_train` y `_psi_oot`.
- Variables numéricas definidas por configuración (`num_list` / `num_unv`).
- Variables categóricas definidas por configuración (`cat_list` / `cat_unv`).
- Variable temporal (`mes_var`) para breakdown mensual del PSI.

**Modos de ejecución (configurados en `steps/methods/metod_4/step_psi.sas`)**

| Modo          | `psi_mode` | Variables                                                     | Output destino                 | Prefijo archivo |
| ------------- | ---------- | ------------------------------------------------------------- | ------------------------------ | --------------- |
| Automático    | `AUTO`     | config ? `num_list`/`cat_list` + fallback `num_unv`/`cat_unv` | `reports/`+`tables/`+`images/` | `psi_`          |
| Personalizado | `CUSTOM`   | `psi_custom_vars_num/cat` + `psi_custom_byvar`                | `experiments/`                 | `custom_psi_`   |

Parámetros adicionales del step:
- `psi_n_buckets` - número de bins para PROC RANK (default 10)
- `psi_mensual` - 1 = breakdown mensual, 0 = solo PSI total
- `psi_custom_vars_num`, `psi_custom_vars_cat`, `psi_custom_byvar` (solo CUSTOM)

**Validaciones (contract)**
- Tabla TRAIN accesible y no vacía (nobs > 0) vía `proc sql count(*)`.
- Tabla OOT accesible y no vacía (nobs > 0) vía `proc sql count(*)`.
- Al menos una lista de variables (num o cat) no vacía.
- Variable temporal (`byvar`) existe en ambas tablas si se proporcionó.
- **No usar `table.tableExists`** (no confiable). Usar `proc sql` contra `dictionary.tables` o count directo.

**Cómputo**
- Discretización: PROC RANK (variables continuas, buckets definidos por TRAIN) o valores directos (categóricas).
- Heurística: variables numéricas con = 10 valores distintos se tratan como categóricas.
- Suavizado Laplace para evitar log(0): `(n + 0.5) / (total + 0.5 * n_buckets)`.
- PSI Mensual: cada periodo en OOT vs TRAIN completo.
- PSI Total: OOT completo vs TRAIN completo.
- Tablas temporales usan sufijo aleatorio (`&rnd.`) para evitar colisiones.

**Tablas intermedias (casuser)** - se eliminan al finalizar:
- `casuser._psi_cubo` - detalle: Variable × Periodo × PSI × Tipo.
- `casuser._psi_cubo_wide` - pivot: Variable × mes_1 … mes_N × PSI_Total.
- `casuser._psi_resumen` - resumen con estadísticas, semáforo y alertas de tendencia.

**Reportes - semáforo por PSI**
- `PSI < 0.10` ? lightgreen (estable)
- `0.10 = PSI < 0.25` ? yellow (alerta)
- `PSI = 0.25` ? red (crítico)

Formato SAS `PsiSignif` aplicado vía `style(column)={backgroundcolor=PsiSignif.}` en ODS.

Excel multi-hoja: PSI Detalle | PSI Cubo Wide | Resumen | Graficos (tendencia temporal embebida).
HTML con cubo + resumen para vista rápida.
Imágenes JPEG: tendencia temporal por variable (archivos independientes).

**Convención ODS**: JPEG, `bitmap_mode=inline`, imágenes en Excel + JPEG simultáneo vía dual ODS, `reset=all` (ver `design.md §7.9`).

**Outputs esperados**

*Modo AUTO (validación estándar):*
- `outputs/runs/<run_id>/reports/METOD4.2/psi_troncal_X_<scope>.html` - cubo + resumen coloreado
- `outputs/runs/<run_id>/reports/METOD4.2/psi_troncal_X_<scope>.xlsx` - 3 hojas con semáforo
- `outputs/runs/<run_id>/tables/METOD4.2/psi_tX_<scope>_cubo.sas7bdat` - detalle Variable × Periodo
- `outputs/runs/<run_id>/tables/METOD4.2/psi_tX_<scope>_wide.sas7bdat` - pivot Variable × meses
- `outputs/runs/<run_id>/tables/METOD4.2/psi_tX_<scope>_rsmn.sas7bdat` - resumen con alertas
- `outputs/runs/<run_id>/images/METOD4.2/psi_troncal_X_<scope>_tend_*.jpeg` - tendencia temporal

*Modo CUSTOM (análisis exploratorio):*
- `outputs/runs/<run_id>/experiments/custom_psi_troncal_X_<scope>.*` (mismos tipos)
- Tablas con prefijo `cx_psi_tX_<scope>_*`

*Naming compacto de tablas .sas7bdat (= 32 chars, límite SAS):*
- `<scope>` = `base` | `segNNN`
- Ejemplo: `psi_t1_base_cubo` (16 chars), `psi_t1_seg001_rsmn` (19 chars)
- CUSTOM: `cx_psi_t1_base_cubo` (20 chars)

**Compatibilidad de contexto**: segmento y universo.

**Cleanup**
- Tablas temporales en `casuser` (`_psi_cubo`, `_psi_cubo_wide`, `_psi_resumen`, `_psi_dev`, `_psi_oot`) se eliminan al finalizar.
- Tablas promovidas (`_train_input`, `_oot_input`) se dropean por `run_module.sas` despues de cada invocacion.

---

## 4) Módulo: Correlación

**Fecha de corte:** Correlación solo analiza variables numéricas (no usa target/PD/XB), por lo que la fecha maxima de análisis es `oot_max_mes`.

**Ruta**
- `src/modules/correlacion/`

**API pública**
- `%correlacion_run(...)`
- Parámetros de entrada:
  - `input_caslib=PROC` - CASLIB de entrada
  - `input_table=_active_input` - tabla promovida por `run_module`
  - `output_caslib=OUT` - CASLIB de salida
  - `troncal_id`, `split`, `scope`, `run_id` - contexto

**Estructura interna**
```
src/modules/correlacion/
  correlacion_run.sas          %correlacion_run - entry point público
  correlacion_contract.sas     %correlacion_contract - validaciones
  impl/
    correlacion_compute.sas    %_correlacion_compute - Pearson + Spearman
    correlacion_report.sas     %_correlacion_report - HTML + Excel con semáforo
```

**Inputs típicos**
- Dataset input (universo o segmento) con variables numéricas.
- Variables numéricas se resuelven según el **modo de ejecución**:

**Modos de ejecución (configurados en `steps/methods/metod_4/step_correlacion.sas`)**

| Modo          | `corr_mode` | Variables                                                   | Output destino         | Prefijo archivo       |
| ------------- | ----------- | ----------------------------------------------------------- | ---------------------- | --------------------- |
| Automático    | `AUTO`      | `cfg_segmentos.num_list` ? fallback `cfg_troncales.num_unv` | `reports/` + `tables/` | `correlacion_`        |
| Personalizado | `CUSTOM`    | `corr_custom_vars` (lista manual del usuario)               | `experiments/`         | `custom_correlacion_` |

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

**Reportes - semáforo por |r|**
- `|r| < 0.5` ? lightgreen (débil)
- `0.5 = |r| < 0.6` ? yellow (moderada)
- `|r| = 0.6` ? red (fuerte)

Formato SAS `CorrSignif` aplicado vía `style(column)={backgroundcolor=CorrSignif.}` en ODS.

**Outputs esperados**

*Modo AUTO (validación estándar):*
- `outputs/runs/<run_id>/reports/METOD4.3/correlacion_troncal_X_<split>_<scope>.html` - matrices coloreadas
- `outputs/runs/<run_id>/reports/METOD4.3/correlacion_troncal_X_<split>_<scope>.xlsx` - hojas Pearson + Spearman
- `outputs/runs/<run_id>/tables/METOD4.3/corr_tX_<spl>_<scope>_prsn.sas7bdat` - datos Pearson
- `outputs/runs/<run_id>/tables/METOD4.3/corr_tX_<spl>_<scope>_sprm.sas7bdat` - datos Spearman

*Modo CUSTOM (análisis exploratorio):*
- `outputs/runs/<run_id>/experiments/custom_correlacion_troncal_X_<split>_<scope>.html`
- `outputs/runs/<run_id>/experiments/custom_correlacion_troncal_X_<split>_<scope>.xlsx`
- `outputs/runs/<run_id>/experiments/cx_corr_tX_<spl>_<scope>_prsn.sas7bdat`
- `outputs/runs/<run_id>/experiments/cx_corr_tX_<spl>_<scope>_sprm.sas7bdat`

*Naming compacto de tablas .sas7bdat (= 32 chars, límite SAS):*
- `<spl>` = `trn` | `oot`
- `<scope>` = `base` | `segNNN`
- Ejemplo: `corr_t1_trn_seg001_prsn` (24 chars)
- Reportes (.html/.xlsx) usan nombres descriptivos completos (sin límite de 32 chars).

**Compatibilidad de contexto**: segmento y universo.

**Cleanup**
- Tablas temporales en `casuser` (`_corr_pearson`, `_corr_spearman`) se eliminan al finalizar.

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
   - Check de flag `&run_<modulo>` al inicio (? skip si 0)
   - Sección de configuración propia (params editables)
   - Crea CASLIBs PROC + OUT
   - Lee `&ctx_scope` para decidir iteración:
     - SEGMENTO ? itera via `ctx_n_segments`, `ctx_seg_id`
     - UNIVERSO ? ejecuta base/troncal via `ctx_troncal_id`
   - Cleanup CASLIBs al final
4. Añadir flag `run_<nuevo_modulo>` en `steps/context_and_modules.sas`.
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
