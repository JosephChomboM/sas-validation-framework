/* =========================================================================
   steps/05_partition_data.sas - Step 5: Configuración de particiones

   Backend del step:
     - Ejecuta partición processed (universo + segmentos)
   ========================================================================= */

/* Dependencias (cada step es independiente) */
%include "&fw_root./src/common/common_public.sas";

%let raw_table = &fw_sas_dataset_name.;
%let partition_enabled = 1;

%put NOTE: [step-05] partition_enabled=&partition_enabled.;

%macro _step05_partition;
   %local _step_rc _step_status;
   %let _step_rc=0;
   %let _step_status=OK;

   %fw_log_start(step_name=step-05_partition_data, run_id=&run_id.,
      fw_root=&fw_root., log_stem=05_partition_data);

   %if &partition_enabled. = 1 %then %do;
      %fw_prepare_processed(raw_table=&raw_table.);
   %end;
   %else %do;
      %let _step_status=SKIP;
      %put NOTE: [step-05] skip fw_prepare_processed.;
   %end;

%_step05_exit:
   %fw_log_stop(step_name=step-05_partition_data, step_rc=&_step_rc.,
      step_status=&_step_status.);
%mend _step05_partition;
%_step05_partition;
