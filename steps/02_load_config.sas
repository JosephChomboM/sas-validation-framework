/* =========================================================================
   steps/02_load_config.sas — Step 2: Carga/validación de config.sas
   Backend del step:
     - Inicializa sesión CAS
     - Genera run_id
     - Carga config.sas (cfg_troncales/cfg_segmentos)
     - Crea carpetas de output del run actual (siempre, cada corrida)
   ========================================================================= */

%let config_file = &fw_root./config.sas;

%macro _step02_load;

  %if %sysfunc(fileexist(&config_file.)) = 0 %then %do;
    %put ERROR: [step-02] No existe config_file=&config_file.;
    %return;
  %end;
  %else %do;
    %put NOTE: [step-02] config_file=&config_file.;
  %end;

  /* --- Backend: CAS init + run_id + carga config ----------------------- */
  cas conn;
  libname casuser cas caslib=casuser;
  options casdatalimit=ALL;

  data _null_;
    _ts = put(datetime(), E8601DT19.);
    _ts = translate(_ts, "-", ":");
    _ts = compress(_ts, "T");
    call symputx("run_id", cats("run_", _ts));
  run;

  %include "&config_file.";

  %put NOTE: [step-02] CAS inicializado y config cargado. run_id=&run_id.;

%mend _step02_load;
%_step02_load;

/* --- Crear carpetas de output del run (cada corrida) ----------------- */
%macro _create_run_dirs;
   options dlcreatedir;

   libname _mkout1 "&fw_root./outputs";
   libname _mkout1 clear;
   libname _mkout2 "&fw_root./outputs/runs";
   libname _mkout2 clear;

   %let _base = &fw_root./outputs/runs/&run_id.;
   %let _dirs = logs reports images tables manifests experiments;
   %let _nd   = %sysfunc(countw(&_dirs., %str( )));

   libname _mkrun "&_base.";
   libname _mkrun clear;

   %do _d = 1 %to &_nd.;
      %let _dir = %scan(&_dirs., &_d., %str( ));
      libname _mksub "&_base./&_dir.";
      libname _mksub clear;
   %end;

   options nodlcreatedir;
%mend _create_run_dirs;
%_create_run_dirs;

%put NOTE: [step-02] Carpetas de output del run (&run_id.) creadas.;
