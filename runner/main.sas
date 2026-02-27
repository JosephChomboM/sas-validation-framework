/* =========================================================================
   runner/main.sas — Orquestador del framework

   Flujo de ejecución:
     FRONTEND (steps — configuración del usuario):
       Step 1) Setup proyecto: ruta raíz + creación de carpetas
       Step 2) Import raw data: parámetros ADLS (o skip)
       Step 3) Select troncal/segmento: referencia a config.sas
       Step 4) Select métodos: módulos a ejecutar

     BACKEND (ejecución automática):
       5) Sesión CAS + run_id
       6) Carga de config.sas (→ casuser.cfg_troncales / cfg_segmentos)
       7) Carga de utilidades comunes + dispatch
       8) Creación de CASLIBs de output
       9) (Opcional) Importación ADLS → data/raw/
      10) Preparación data processed (train/oot/segmentos)
      11) Ejecución de módulos (segmentos primero, luego base)
      12) Cleanup de CASLIBs y cierre

   CASLIB policy (caslib_lifecycle.md):
     - casuser: SOLO tablas de configuración (cfg_troncales, cfg_segmentos)
     - RAW:          PATH → data/raw/           (subdirs=0)
     - PROCESSED:    PATH → data/processed/     (subdirs=1)
     - LAKEHOUSE:    ADLS temporal (creado/limpiado por fw_import)
     - OUT_<run_id>: PATH → outputs/runs/<run_id>/ (subdirs=1)

   Ref: design.md §2 / §5, README.md §1.1
   ========================================================================= */

/* =====================================================================
   FRONTEND — Steps (configuración del usuario)
   El usuario edita los archivos steps/*.sas ANTES de ejecutar main.sas.
   Al incluirlos aquí se setean las macro variables que el framework
   necesita y se ejecutan acciones automáticas (ej. creación de carpetas).
   ===================================================================== */

/* ---- Step 1: Setup del proyecto ------------------------------------ */
/* Setea &fw_root y crea estructura de carpetas (data/raw, processed…)   */
/* NOTA: Este %include usa ruta relativa porque &fw_root aún no existe.  */
/*       Ajustar si main.sas se ejecuta desde otro directorio.           */
%include "./steps/01_setup_project.sas";

/* ---- Step 2: Importación ADLS ------------------------------------- */
/* Setea &adls_import_enabled, &adls_storage, &adls_container,           */
/* &adls_parquet_path, &raw_table                                         */
%include "&fw_root./steps/02_import_raw_data.sas";

/* ---- Step 3: Troncal/segmento (referencia a config.sas) ----------- */
/* Documenta el contrato _id_*. La config real está en config.sas.       */
%include "&fw_root./steps/03_select_troncal_segment.sas";

/* ---- Step 4: Selección de métodos --------------------------------- */
/* Setea &methods_list, &run_label                                        */
%include "&fw_root./steps/04_select_methods.sas";

%put NOTE: ======================================================;
%put NOTE: [main] FRONTEND completado — Steps 1-4 cargados.;
%put NOTE:   fw_root              = &fw_root.;
%put NOTE:   adls_import_enabled  = &adls_import_enabled.;
%put NOTE:   raw_table            = &raw_table.;
%put NOTE:   methods_list         = &methods_list.;
%put NOTE: ======================================================;

/* =====================================================================
   BACKEND — Ejecución automática del framework
   ===================================================================== */

/* =====================================================================
   5) SESIÓN CAS + RUN_ID
   ===================================================================== */
cas conn;
libname casuser cas caslib=casuser;
options casdatalimit=ALL;

data _null_;
  _ts = put(datetime(), E8601DT19.);
  _ts = translate(_ts, "-", ":");
  _ts = compress(_ts, "T");
  call symputx("run_id", cats("run_", _ts));
run;

%put NOTE: ======================================================;
%put NOTE: [main] run_id = &run_id.;
%put NOTE: ======================================================;

/* ---- Crear carpetas de output para este run ------------------------ */
%macro _create_run_dirs;
  %let _base = &fw_root./outputs/runs/&run_id.;
  %let _dirs = logs reports images tables manifests;
  %let _nd   = %sysfunc(countw(&_dirs., %str( )));

  options dlcreatedir;
  libname _mkrun "&fw_root./outputs/runs/&run_id.";
  libname _mkrun clear;

  %do _d = 1 %to &_nd.;
    %let _dir = %scan(&_dirs., &_d., %str( ));
    libname _mksub "&_base./&_dir.";
    libname _mksub clear;
    %put NOTE: [main] Carpeta: &_base./&_dir.;
  %end;
%mend _create_run_dirs;
%_create_run_dirs;

/* =====================================================================
   6) CARGA DE CONFIGURACIÓN (→ casuser.cfg_troncales / cfg_segmentos)
   Requiere sesión CAS activa.
   ===================================================================== */
%include "&fw_root./config.sas";

/* =====================================================================
   7) CARGA DE UTILIDADES COMUNES + DISPATCH
   ===================================================================== */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";
%include "&fw_root./src/dispatch/run_method.sas";

/* =====================================================================
   8) CREACIÓN DE CASLIB OUT_<run_id> para outputs
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
   9) (OPCIONAL) IMPORTACIÓN DE DATA ADLS → data/raw/
   Controlado por &adls_import_enabled (seteado en step 02).
   Si =1, importa parquet desde ADLS y lo persiste como .sashdat.
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
   10) PREPARACIÓN DE DATA PROCESSED
   Crea CASLIBs RAW y PROCESSED internamente.
   Particiona train/oot y segmentos según casuser.cfg_troncales.
   ===================================================================== */
%fw_prepare_processed(raw_table=&raw_table.);

/* =====================================================================
   11) EJECUCIÓN DE MÓDULOS — SEGMENTOS PRIMERO, LUEGO BASE
   Regla (design.md §6):
     Si la troncal tiene segmentación:
       1. Ejecutar módulos en cada segmento (train y oot).
       2. Ejecutar módulos en el universo/base (train y oot).
   ===================================================================== */

%macro _run_all_methods;

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
       11a) SEGMENTOS PRIMERO (si hay segmentación)
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
       11b) UNIVERSO / BASE (después de todos los segmentos)
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

  /* Limpieza macrovars temporales */
  %do _i = 1 %to &_n_tr.;
    %symdel _rtr_id_&_i. _rtr_vseg_&_i. _rtr_nseg_&_i. / nowarn;
  %end;
  %symdel _n_tr / nowarn;

%mend _run_all_methods;

%_run_all_methods;

/* =====================================================================
   12) CLEANUP DE CASLIBs Y CIERRE
   El runner crea y limpia CASLIBs globales.
   Los módulos limpian sus propios CASLIBs scoped.
   ===================================================================== */
%_drop_caslib(caslib_name=OUT_&run_id., cas_sess_name=conn, del_prom_tables=1);
%_drop_caslib(caslib_name=RAW,       cas_sess_name=conn, del_prom_tables=1);
%_drop_caslib(caslib_name=PROCESSED, cas_sess_name=conn, del_prom_tables=1);

%put NOTE: ======================================================;
%put NOTE: [main] Run &run_id. completado.;
%put NOTE: Outputs en: &fw_root./outputs/runs/&run_id./;
%put NOTE: CASLIBs operativos limpiados. casuser.cfg_* preservados.;
%put NOTE: ======================================================;

cas conn terminate;
