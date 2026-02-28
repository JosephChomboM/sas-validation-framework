/* =========================================================================
   steps/02_load_config.sas — Step 2: Carga/validación de config.sas
   Backend del step:
     - Inicializa sesión CAS
     - Genera run_id
     - Carga config.sas (cfg_troncales/cfg_segmentos)
   ========================================================================= */

%let config_file = &fw_root./config.sas;

%if %sysfunc(fileexist(&config_file.)) = 0 %then %do;
  %put ERROR: [step-02] No existe config_file=&config_file.;
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
