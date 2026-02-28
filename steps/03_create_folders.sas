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

/* ---- Crear carpetas del run actual ---- */
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

/* ---- Crear estructura troncal_X/train/ y troncal_X/oot/ bajo data/processed/ ----
   Lee casuser.cfg_troncales (disponible tras step 02) para obtener los IDs.
   Sigue la estructura del repositorio:
     data/processed/troncal_1/train/
     data/processed/troncal_1/oot/
     data/processed/troncal_2/train/
     ...
   ---- */
%macro _create_troncal_dirs;
   proc sql noprint;
      select distinct troncal_id
         into :_tdir_list separated by ' '
         from casuser.cfg_troncales;
      %let _n_tdir = &sqlobs.;
   quit;

   %put NOTE: [step-03] Creando carpetas processed para &_n_tdir. troncal(es).;

   %do _i = 1 %to &_n_tdir.;
      %let _tid = %scan(&_tdir_list., &_i., %str( ));

      libname _mktr1 "&fw_root./data/processed/troncal_&_tid.";
      libname _mktr1 clear;
      libname _mktr2 "&fw_root./data/processed/troncal_&_tid./train";
      libname _mktr2 clear;
      libname _mktr3 "&fw_root./data/processed/troncal_&_tid./oot";
      libname _mktr3 clear;

      %put NOTE: [step-03]   troncal_&_tid. => train/ oot/ creados.;
   %end;
%mend _create_troncal_dirs;
%_create_troncal_dirs;

options nodlcreatedir;
%put NOTE: [step-03] Carpetas base, run (&run_id.) y troncales verificadas/creadas.;
