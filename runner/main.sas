/* =========================================================================
   runner/main.sas — Entrypoint único del framework
   Reemplaza .flw/.step.

   Flujo:
     1) Inicialización (sesión CAS, rutas, run_id)
     2) Carga de config (config.sas → casuser.cfg_troncales / cfg_segmentos
        + macro vars ADLS)
     3) Carga de utilidades comunes (common_public.sas incl. cas_utils)
     4) Carga de dispatch (run_module.sas, run_method.sas)
     5) Creación de CASLIB OUT_<run_id> para outputs
     6) (Opcional) Importación de data ADLS → data/raw/
     7) Preparación de data processed (fw_prepare_processed)
     8) Ejecución de módulos — orden: segmentos primero, luego base
     9) Cleanup de CASLIBs y cierre

   CASLIB policy (caslib_lifecycle.md):
     - casuser: SOLO tablas de configuración (cfg_troncales, cfg_segmentos).
     - RAW:          PATH → data/raw/           (subdirs=0)
     - PROCESSED:    PATH → data/processed/     (subdirs=1)
     - LAKEHOUSE:    ADLS temporal (creado/limpiado por fw_import)
     - OUT_<run_id>: PATH → outputs/runs/<run_id>/ (subdirs=1)

   Requisitos previos:
     - Filesystem accesible con las carpetas data/ y outputs/
     - Si adls_import_enabled=0, dataset raw ya debe existir en data/raw/

   Ref: design.md §2.1 capa 5 (Runner), README.md §4
   ========================================================================= */

/* =====================================================================
   1) INICIALIZACIÓN — sesión CAS + casuser
   ===================================================================== */

/* Raíz del proyecto — ajustar si main.sas se ejecuta desde otro CWD */
%let fw_root = /path/to/framework_validacion;

/* Módulos a ejecutar (space-separated). Extensible: gini psi ... */
%let methods_list = gini psi;

/* Raw table override (default: mydataset) */
%let raw_table = mydataset;

/* Sesión CAS + casuser (caslib_lifecycle.md baseline) */
cas conn;
libname casuser cas caslib=casuser;
options casdatalimit=ALL;

/* ---- Run ID basado en timestamp ------------------------------------ */
data _null_;
  _ts = put(datetime(), E8601DT19.);
  _ts = translate(_ts, "-", ":");
  _ts = compress(_ts, "T");
  call symputx("run_id", cats("run_", _ts));
run;

%put NOTE: ======================================================;
%put NOTE: [main] run_id = &run_id.;
%put NOTE: ======================================================;

/* ---- Crear carpetas de output por run ------------------------------ */
%macro _create_output_dirs;
  %let _base = &fw_root./outputs/runs/&run_id.;
  %let _dirs = logs reports images tables manifests;
  %let _nd   = %sysfunc(countw(&_dirs., %str( )));

  %do _d = 1 %to &_nd.;
    %let _dir = %scan(&_dirs., &_d., %str( ));
    %let _rc  = %sysfunc(dcreate(&_dir., &_base.));
    %if &_rc. ne %then
      %put NOTE: [main] Creado &_base./&_dir.;
    %else
      %put WARNING: [main] No se pudo crear &_base./&_dir. (puede ya existir).;
  %end;
%mend _create_output_dirs;

data _null_;
  _base = cats("&fw_root./outputs/runs");
  rc1 = dcreate("&run_id.", _base);
run;
%_create_output_dirs;

/* =====================================================================
   2) CARGA DE CONFIGURACIÓN (→ casuser.cfg_troncales / cfg_segmentos)
   ===================================================================== */
%include "&fw_root./config.sas";

/* =====================================================================
   3) CARGA DE UTILIDADES COMUNES (cas_utils + fw_paths + fw_prepare)
   ===================================================================== */
%include "&fw_root./src/common/common_public.sas";

/* =====================================================================
   4) CARGA DE DISPATCH
   ===================================================================== */
%include "&fw_root./src/dispatch/run_module.sas";
%include "&fw_root./src/dispatch/run_method.sas";

/* =====================================================================
   5) CREACIÓN DE CASLIB OUT_<run_id> para outputs
   (RAW y PROCESSED se crean dentro de fw_prepare_processed)
   ===================================================================== */
%_create_caslib(
  cas_path     = &fw_root./outputs/runs/&run_id.,
  caslib_name  = OUT_&run_id.,
  lib_caslib   = OUT_&run_id.,
  global       = Y,
  cas_sess_name= conn,
  term_global_sess = 0,
  subdirs_flg  = 1
);

/* =====================================================================
   6) (OPCIONAL) IMPORTACIÓN DE DATA ADLS → data/raw/
   Controlado por &adls_import_enabled. (seteado en config.sas)
   Si =1, importa el parquet desde ADLS y lo persiste como .sashdat.
   Si =0, asume que data/raw/&raw_table..sashdat ya existe.
   ===================================================================== */
%macro _run_adls_import;
  %if &adls_import_enabled. = 1 %then %do;
    %put NOTE: [main] Importando data desde ADLS...;
    %fw_import_adls_to_cas(
      raw_path          = &fw_root./data/raw,
      adls_storage      = &adls_storage.,
      adls_container    = &adls_container.,
      adls_parquet_path = &adls_parquet_path.,
      output_table      = &raw_table.,
      save_to_disk      = 1
    );
  %end;
  %else %do;
    %put NOTE: [main] adls_import_enabled=0 — saltando importación ADLS.;
    %put NOTE: [main] Se asume que data/raw/&raw_table..sashdat ya existe.;
  %end;
%mend _run_adls_import;
%_run_adls_import;

/* =====================================================================
   7) PREPARACIÓN DE DATA PROCESSED
   (crea CASLIBs RAW y PROCESSED internamente)
   ===================================================================== */
%fw_prepare_processed(raw_table=&raw_table.);

/* =====================================================================
   8) EJECUCIÓN DE MÓDULOS — SEGMENTOS PRIMERO, LUEGO BASE
   Regla (design.md §6, README.md §4):
     Si la troncal tiene segmentación:
       1. Ejecutar módulos en cada segmento (train y oot).
       2. Ejecutar módulos en el universo/base (train y oot).
   ===================================================================== */

%macro _run_all_methods;

  /* Leer número de troncales (config en casuser) */
  proc sql noprint;
    select count(*) into :_n_tr trimmed from casuser.cfg_troncales;
  quit;

  data _null_;
    set casuser.cfg_troncales;
    call symputx(cats("_rtr_id_",   _n_), troncal_id);
    call symputx(cats("_rtr_vseg_", _n_), strip(var_seg));
    call symputx(cats("_rtr_nseg_", _n_), n_segments);
  run;

  %do _i = 1 %to &_n_tr.;

    %let _tid  = &&_rtr_id_&_i.;
    %let _vseg = &&_rtr_vseg_&_i.;
    %let _nseg = &&_rtr_nseg_&_i.;

    %put NOTE: ====================================================;
    %put NOTE: [main] Troncal &_tid. (var_seg=&_vseg. n_seg=&_nseg.);
    %put NOTE: ====================================================;

    /* -----------------------------------------------------------
       8a) SEGMENTOS PRIMERO (si hay segmentación)
       ----------------------------------------------------------- */
    %if %superq(_vseg) ne and &_nseg. > 0 %then %do;
      %do _sg = 1 %to &_nseg.;
        %do _sp = 1 %to 2;
          %if &_sp. = 1 %then %let _split = train;
          %else %let _split = oot;

          %run_method(
            method_modules = &methods_list.,
            troncal_id     = &_tid.,
            split          = &_split.,
            seg_id         = &_sg.,
            run_id         = &run_id.
          );
        %end; /* splits */
      %end; /* segmentos */
    %end;

    /* -----------------------------------------------------------
       8b) UNIVERSO / BASE (después de todos los segmentos)
       ----------------------------------------------------------- */
    %do _sp = 1 %to 2;
      %if &_sp. = 1 %then %let _split = train;
      %else %let _split = oot;

      %run_method(
        method_modules = &methods_list.,
        troncal_id     = &_tid.,
        split          = &_split.,
        seg_id         = ,
        run_id         = &run_id.
      );
    %end; /* splits base */

  %end; /* troncales */

  /* Limpieza macrovars */
  %do _i = 1 %to &_n_tr.;
    %symdel _rtr_id_&_i. _rtr_vseg_&_i. _rtr_nseg_&_i. / nowarn;
  %end;
  %symdel _n_tr / nowarn;

%mend _run_all_methods;

%_run_all_methods;

/* =====================================================================
   9) CLEANUP DE CASLIBs Y CIERRE
   Regla (caslib_lifecycle.md): el runner crea y limpia CASLIBs globales.
   Los módulos limpian sus propios CASLIBs scoped.
   ===================================================================== */

/* Drop CASLIB de outputs del run */
%_drop_caslib(caslib_name=OUT_&run_id., cas_sess_name=conn, del_prom_tables=1);

/* Drop CASLIBs operativos (RAW, PROCESSED creados por fw_prepare) */
%_drop_caslib(caslib_name=RAW,       cas_sess_name=conn, del_prom_tables=1);
%_drop_caslib(caslib_name=PROCESSED, cas_sess_name=conn, del_prom_tables=1);

/* Config tables en casuser se mantienen (deliberado) */

%put NOTE: ======================================================;
%put NOTE: [main] Run &run_id. completado.;
%put NOTE: Outputs en: &fw_root./outputs/runs/&run_id./;
%put NOTE: CASLIBs operativos limpiados. casuser.cfg_* preservados.;
%put NOTE: ======================================================;

/* Terminar sesión CAS */
cas conn terminate;
