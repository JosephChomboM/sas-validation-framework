/* =========================================================================
   runner/main.sas — Orquestador liviano

   Responsabilidad:
     - Incluir secuencialmente los steps 01..11.
     - Cada step contiene su parte frontend + backend.
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

%put NOTE: [main] Steps 01..11 ejecutados (la lógica backend vive en cada step).;
