/* =========================================================================
   steps/05_partition_data.sas — Step 5: Configuración de particiones

   Backend del step:
     - Ejecuta partición processed (universo + segmentos)
   ========================================================================= */

/* Dependencias (cada step es independiente) */
%include "&fw_root./src/common/common_public.sas";

%let partition_enabled = 1;

%put NOTE: [step-05] partition_enabled=&partition_enabled.;

%macro _step05_partition;
   %if &partition_enabled. = 1 %then %do;
      %fw_prepare_processed(raw_table=&raw_table.);
   %end;
   %else %do;
      %put NOTE: [step-05] skip fw_prepare_processed.;
   %end;
%mend _step05_partition;
%_step05_partition;
