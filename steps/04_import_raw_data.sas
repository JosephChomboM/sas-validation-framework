/* =========================================================================
   steps/04_import_raw_data.sas — Step 4: Config import ADLS (one-time)

   Backend del step:
     - Ejecuta importación ADLS (si está habilitada)
     - fw_import_adls_to_cas crea/dropea sus CASLIBs internamente
       (LAKEHOUSE + RAW), siguiendo el patrón create → use → drop.
   ========================================================================= */

%let adls_import_enabled = 1;
%let adls_storage        = adlscu1cemmbackp05;
%let adls_container      = bcp-exp-mrm-vime-01;
%let adls_parquet_path   = data/modelo/dataset_v1.parquet;
%let raw_table           = mydataset;

%put NOTE: [step-04] adls_import_enabled=&adls_import_enabled. raw_table=&raw_table.;

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
