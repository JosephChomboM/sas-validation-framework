/* =========================================================================
   runner/main.sas — Orquestador del framework (MVP Steps 01..11)

   Frontend (steps):
     01 setup rutas
     02 load config path
     03 create folders
     04 import ADLS config
     05 partition config
     06 promote segment context
     07 config methods segment
     08 macro run methods segment
     09 promote universe context
     10 config methods universe
     11 macro run methods universe

   Backend:
     A) CAS init + run_id
     B) include config.sas
     C) load common + dispatch
     D) create OUT_<run_id>
     E) optional ADLS import
     F) optional partition
     G) run segment subflow
     H) run universe subflow
     I) cleanup
   ========================================================================= */

/* =====================================================================
   FRONTEND — configuración
   ===================================================================== */
%include "./steps/01_setup_project.sas";
%include "&fw_root./steps/02_load_config.sas";
%include "&fw_root./steps/03_create_folders.sas";
%include "&fw_root./steps/04_import_raw_data.sas";
%include "&fw_root./steps/05_partition_data.sas";
%include "&fw_root./steps/06_promote_segment_context.sas";
%include "&fw_root./steps/07_config_methods_segment.sas";
%include "&fw_root./steps/08_run_methods_segment.sas";
%include "&fw_root./steps/09_promote_universe_context.sas";
%include "&fw_root./steps/10_config_methods_universe.sas";
%include "&fw_root./steps/11_run_methods_universe.sas";

%put NOTE: ======================================================;
%put NOTE: [main] FRONTEND listo (steps 01..11 cargados).;
%put NOTE: ======================================================;

/* =====================================================================
   BACKEND — ejecución
   ===================================================================== */

/* A) CAS init + run_id */
cas conn;
libname casuser cas caslib=casuser;
options casdatalimit=ALL;

data _null_;
  _ts = put(datetime(), E8601DT19.);
  _ts = translate(_ts, "-", ":");
  _ts = compress(_ts, "T");
  call symputx("run_id", cats("run_", _ts));
run;

/* Crear folders del run */
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
  %end;
%mend _create_run_dirs;
%_create_run_dirs;

/* B) include config.sas */
%include "&config_file.";

/* C) load common + dispatch */
%include "&fw_root./src/common/common_public.sas";
%include "&fw_root./src/dispatch/run_module.sas";
%include "&fw_root./src/dispatch/run_method.sas";

/* D) create OUT_<run_id> */
%_create_caslib(
  cas_path         = &fw_root./outputs/runs/&run_id.,
  caslib_name      = OUT_&run_id.,
  lib_caslib       = OUT_&run_id.,
  global           = Y,
  cas_sess_name    = conn,
  term_global_sess = 0,
  subdirs_flg      = 1
);

/* E) optional ADLS import */
%macro _run_adls_import;
  %if &adls_import_enabled. = 1 %then %do;
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
    %put NOTE: [main] adls_import_enabled=0 — skip import ADLS.;
  %end;
%mend _run_adls_import;
%_run_adls_import;

/* F) optional partition */
%if &partition_enabled. = 1 %then %do;
  %fw_prepare_processed(raw_table=&raw_table.);
%end;
%else %do;
  %put NOTE: [main] partition_enabled=0 — skip fw_prepare_processed.;
%end;

/* G) run segment subflow */
%run_methods_segment_context(run_id=&run_id.);

/* H) run universe subflow */
%run_methods_universe_context(run_id=&run_id.);

/* I) cleanup */
%_drop_caslib(caslib_name=OUT_&run_id., cas_sess_name=conn, del_prom_tables=1);
%_drop_caslib(caslib_name=RAW,       cas_sess_name=conn, del_prom_tables=1);
%_drop_caslib(caslib_name=PROCESSED, cas_sess_name=conn, del_prom_tables=1);

%put NOTE: ======================================================;
%put NOTE: [main] Run &run_id. completado.;
%put NOTE: [main] Outputs en: &fw_root./outputs/runs/&run_id./;
%put NOTE: ======================================================;

cas conn terminate;
