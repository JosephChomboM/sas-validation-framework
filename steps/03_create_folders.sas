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

/* Crear carpetas del run actual */
%macro _create_run_dirs;
   %let _base = &fw_root./outputs/runs/&run_id.;
   %let _dirs = logs reports images tables manifests;
   %let _nd   = %sysfunc(countw(&_dirs., %str( )));

   libname _mkrun "&fw_root./outputs/runs/&run_id.";
   libname _mkrun clear;

   %do _d = 1 %to &_nd.;
      %let _dir = %scan(&_dirs., &_d., %str( ));
      libname _mksub "&_base./&_dir.";
      libname _mksub clear;
   %end;
%mend _create_run_dirs;
%_create_run_dirs;

%put NOTE: [step-03] Carpetas base y run (&run_id.) verificadas/creadas.;
