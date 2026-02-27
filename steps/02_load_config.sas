/* =========================================================================
   steps/02_load_config.sas — Step 2: Carga/validación de config.sas
   Define la ubicación del archivo de configuración.
   ========================================================================= */

%let config_file = &fw_root./config.sas;

%if %sysfunc(fileexist(&config_file.)) = 0 %then %do;
  %put ERROR: [step-02] No existe config_file=&config_file.;
%end;
%else %do;
  %put NOTE: [step-02] config_file=&config_file.;
%end;
