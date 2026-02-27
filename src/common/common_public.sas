/* =========================================================================
   common_public.sas — Agregador de utilidades comunes del framework
   Se incluye una sola vez desde runner/main.sas.

   Orden de %include determinado por dependencias:
     1) fw_paths       (sin dependencias)
     2) fw_prepare     (depende de fw_paths)
   ========================================================================= */

/* --- Resolver la raíz del proyecto (asumida 1 nivel arriba de src/) ----- */
/* El runner debe haber definido &fw_root. antes de incluir este archivo.    */

%include "&fw_root./src/common/fw_paths.sas";
%include "&fw_root./src/common/preparation/fw_prepare_processed.sas";

%put NOTE: [common_public] Utilidades comunes cargadas.;
