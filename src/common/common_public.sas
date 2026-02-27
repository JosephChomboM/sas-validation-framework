/* =========================================================================
   common_public.sas — Agregador de utilidades comunes del framework
   Se incluye una sola vez desde runner/main.sas.

   Orden de %include determinado por dependencias:
     1) cas_utils         (sin dependencias; macros baseline CAS)
     2) fw_paths          (sin dependencias)
     3) fw_import_adls    (depende de cas_utils)
     4) fw_prepare        (depende de cas_utils + fw_paths)
   ========================================================================= */

/* --- Resolver la raíz del proyecto (asumida 1 nivel arriba de src/) ----- */
/* El runner debe haber definido &fw_root. antes de incluir este archivo.    */

%include "&fw_root./src/common/cas_utils.sas";
%include "&fw_root./src/common/fw_paths.sas";
%include "&fw_root./src/common/preparation/fw_import_adls_to_cas.sas";
%include "&fw_root./src/common/preparation/fw_prepare_processed.sas";

%put NOTE: [common_public] Utilidades comunes cargadas (cas_utils + fw_paths + fw_import + fw_prepare).;
