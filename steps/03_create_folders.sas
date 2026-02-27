/* =========================================================================
   steps/03_create_folders.sas — Step 3: Crear estructura de carpetas
   ========================================================================= */

options dlcreatedir;

libname _mkdir01 "&fw_root./data";
libname _mkdir01 clear;
libname _mkdir02 "&fw_root./data/raw";
libname _mkdir02 clear;
libname _mkdir03 "&fw_root./data/processed";
libname _mkdir03 clear;
libname _mkdir04 "&fw_root./outputs";
libname _mkdir04 clear;
libname _mkdir05 "&fw_root./outputs/runs";
libname _mkdir05 clear;

%put NOTE: [step-03] Carpetas base verificadas/creadas.;
