/* =========================================================================
   runner/main.sas — Orquestador liviano

   Responsabilidad:
     - Incluir secuencialmente los steps compartidos (01–05).
     - Ejecutar dos swimlanes independientes:
       SEGMENTO: contexto seg → selección módulos → módulos (solo seg)
       UNIVERSO: contexto unv → selección módulos → módulos (solo unv)

   data_prep_enabled (0|1):
     - 1 = ejecutar steps 03-05 (crear carpetas de data, importar ADLS, particionar).
           Usar en la primera corrida o al re-generar data.
     - 0 = saltar steps 03-05 (data/carpetas ya existen en disco).

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
      Cada step también incluye sus dependencias para ser independiente.
      Esta carga es redundante (idempotente) pero documenta las dependencias
      del pipeline completo. */
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
      EXECUTION — SWIMLANE SEGMENTO
      Contexto → selección de módulos → ejecución (solo segmentos)
      ===================================================================== */
   %include "&fw_root./steps/segmento/context.sas";
   %include "&fw_root./steps/segmento/select_modules.sas";

   /* Módulos: cada step checa su flag run_<modulo>, crea CASLIBs,
      itera segmentos según ctx_segment_*, y limpia */
   /* -- Método 4.2 --------------------------------------------------- */
   /* %include "&fw_root./steps/methods/metod_4/step_estabilidad.sas"; */
   /* %include "&fw_root./steps/methods/metod_4/step_fillrate.sas";    */
   /* %include "&fw_root./steps/methods/metod_4/step_missings.sas";    */
   /* %include "&fw_root./steps/methods/metod_4/step_psi.sas";         */
   /* -- Método 4.3 --------------------------------------------------- */
   /* %include "&fw_root./steps/methods/metod_4/step_bivariado.sas";   */
   %include "&fw_root./steps/methods/metod_4/step_correlacion.sas";
   /* %include "&fw_root./steps/methods/metod_4/step_gini.sas";        */

   /* -- Métodos 1-3 (futuro) ----------------------------------------- */
   /* %include "&fw_root./steps/methods/metod_1/step_universe.sas";    */
   /* %include "&fw_root./steps/methods/metod_2/step_target.sas";      */
   /* %include "&fw_root./steps/methods/metod_3/step_segmentacion.sas"; */

   /* =====================================================================
      EXECUTION — SWIMLANE UNIVERSO
      Contexto → selección de módulos → ejecución (solo base/troncal)
      ===================================================================== */
   %include "&fw_root./steps/universo/context.sas";
   %include "&fw_root./steps/universo/select_modules.sas";

   /* Mismos steps de módulo: ahora ctx_scope=UNIVERSO → itera base */
   /* -- Método 4.2 --------------------------------------------------- */
   /* %include "&fw_root./steps/methods/metod_4/step_estabilidad.sas"; */
   /* %include "&fw_root./steps/methods/metod_4/step_fillrate.sas";    */
   /* %include "&fw_root./steps/methods/metod_4/step_missings.sas";    */
   /* %include "&fw_root./steps/methods/metod_4/step_psi.sas";         */
   /* -- Método 4.3 --------------------------------------------------- */
   /* %include "&fw_root./steps/methods/metod_4/step_bivariado.sas";   */
   %include "&fw_root./steps/methods/metod_4/step_correlacion.sas";
   /* %include "&fw_root./steps/methods/metod_4/step_gini.sas";        */

   %put NOTE: ======================================================;
   %put NOTE: [main] Run &run_id. completado.;
   %put NOTE: [main] Outputs en: &fw_root./outputs/runs/&run_id./;
   %put NOTE: ======================================================;

%mend _main_pipeline;
%_main_pipeline;
