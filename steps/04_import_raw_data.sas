/* =========================================================================
   steps/04_import_raw_data.sas — Step 4: Config import ADLS (one-time)
   ========================================================================= */

%let adls_import_enabled = 1;
%let adls_storage        = adlscu1cemmbackp05;
%let adls_container      = mi-container;
%let adls_parquet_path   = data/modelo/dataset_v1.parquet;
%let raw_table           = mydataset;

%put NOTE: [step-04] adls_import_enabled=&adls_import_enabled. raw_table=&raw_table.;
