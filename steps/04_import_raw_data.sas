/* =========================================================================
   steps/04_import_raw_data.sas - Step 4: Config import ADLS (one-time)

   Backend del step:
     - Ejecuta importación ADLS (si está habilitada)
     - fw_import_adls_to_cas crea/dropea sus CASLIBs internamente
       (LAKEHOUSE + RAW), siguiendo el patrón create → use → drop.
   ========================================================================= */

/* Dependencias (cada step es independiente) */
%include "&fw_root./src/common/common_public.sas";

%let adls_import_enabled = 1;
%let adls_storage        = &_id_storage.;
%let adls_container      = &id_container.;
%let adls_parquet_path   = &id_container_path.;
%let raw_table           = &fw_sas_dataset_name.;

%put NOTE: [step-04] adls_import_enabled=&adls_import_enabled. raw_table=&raw_table.;

/* Importación opcional desde ADLS */
%macro _step04_import;
   %local _step_rc _step_status;
   %let _step_rc=0;
   %let _step_status=OK;

   %fw_log_start(step_name=step-04_import_raw_data, run_id=&run_id.,
      fw_root=&fw_root., log_stem=04_import_raw_data);

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
      %let _step_status=SKIP;
      %put NOTE: [step-04] adls_import_enabled=0 - skip import ADLS.;
   %end;

%_step04_exit:
   %fw_log_stop(step_name=step-04_import_raw_data, step_rc=&_step_rc.,
      step_status=&_step_status.);
%mend _step04_import;
%_step04_import;
