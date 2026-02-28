/* =========================================================================
   steps/04_import_raw_data.sas — Step 4: Config import ADLS (one-time)

   Backend del step:
     - Carga utilidades comunes
     - Crea CASLIB OUT_<run_id>
     - Ejecuta importación ADLS (si está habilitada)
   ========================================================================= */

%let adls_import_enabled = 1;
%let adls_storage        = adlscu1cemmbackp05;
%let adls_container      = bcp-exp-mrm-vime-01;
%let adls_parquet_path   = data/modelo/dataset_v1.parquet;
%let raw_table           = mydataset;

%put NOTE: [step-04] adls_import_enabled=&adls_import_enabled. raw_table=&raw_table.;

/* Cargar utilidades comunes (cas_utils + import + prepare + paths) */
%include "&fw_root./src/common/common_public.sas";

/* Crear CASLIB de outputs del run */
%_create_caslib(
   cas_path         = &fw_root./outputs/runs/&run_id.,
   caslib_name      = OUT_&run_id.,
   lib_caslib       = OUT_&run_id.,
   global           = Y,
   cas_sess_name    = conn,
   term_global_sess = 0,
   subdirs_flg      = 1
);

/* Importación opcional desde ADLS */
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
   %put NOTE: [step-04] adls_import_enabled=0 — skip import ADLS.;
%end;
