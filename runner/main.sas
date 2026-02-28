/* =========================================================================
   runner/main.sas — Orquestador liviano

   Responsabilidad:
     - Incluir secuencialmente los steps 01..11.
     - Cada step contiene su parte frontend + backend.

   data_prep_enabled (0|1):
     - 1 = ejecutar steps 03-05 (crear carpetas de data, importar ADLS, particionar).
           Usar en la primera corrida o al re-generar data.
     - 0 = saltar steps 03-05 (data/carpetas ya existen en disco).
           Permite ir directo de config (step 02) a ejecución (step 06+).

   Nota SAS: toda lógica condicional (%if/%do) debe vivir dentro de una
   macro. Por eso el pipeline completo se define en %_main_pipeline.
   ========================================================================= */

/* Flag de preparación de data — cambiar a 0 para saltar Steps 03-05 */
%let data_prep_enabled = 1;

%macro _main_pipeline;

   /* =====================================================================
      SETUP + CONFIG (siempre)
      ===================================================================== */
   %include "./steps/01_setup_project.sas";
   %include "&fw_root./steps/02_load_config.sas";

   /* Cargar macros comunes (cas_utils, fw_paths, fw_import, fw_prepare)
      Se cargan siempre — son definiciones de macros, no ejecutan nada. */
   %include "&fw_root./src/common/common_public.sas";

   /* =====================================================================
      DATA PREP (una vez por proyecto, controlado por data_prep_enabled)
      ===================================================================== */
   %if &data_prep_enabled. = 1 %then %do;
      %include "&fw_root./steps/03_create_folders.sas";
      %include "&fw_root./steps/04_import_raw_data.sas";
      %include "&fw_root./steps/05_partition_data.sas";
      %put NOTE: [main] Steps 03-05 (data prep) ejecutados.;
   %end;
   %else %do;
      %put NOTE: [main] data_prep_enabled=0 — salto steps 03-05 (data ya existe en disco).;
   %end;

   /* =====================================================================
      EXECUTION (cada corrida)
      ===================================================================== */
   %include "&fw_root./steps/06_promote_segment_context.sas";
   %include "&fw_root./steps/07_config_methods_segment.sas";
   %include "&fw_root./steps/08_run_methods_segment.sas";
   %include "&fw_root./steps/09_promote_universe_context.sas";
   %include "&fw_root./steps/10_config_methods_universe.sas";
   %include "&fw_root./steps/11_run_methods_universe.sas";

   %put NOTE: ======================================================;
   %put NOTE: [main] Run &run_id. completado.;
   %put NOTE: [main] Outputs en: &fw_root./outputs/runs/&run_id./;
   %put NOTE: ======================================================;

%mend _main_pipeline;
%_main_pipeline;
