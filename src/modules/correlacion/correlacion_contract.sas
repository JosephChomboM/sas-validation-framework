/* =========================================================================
   correlacion_contract.sas — Validaciones pre-ejecución del módulo

   Verifica:
     1) Tabla input accesible y con observaciones (nobs > 0)
     2) Lista de variables numéricas no está vacía

   Setea macro variable &_corr_rc (declarada %global por correlacion_run):
     0 = OK, 1 = fallo (el módulo no debe ejecutarse)
   ========================================================================= */

%macro correlacion_contract(input_caslib=, input_table=, variables=);

  /* _corr_rc is declared %global by correlacion_run (the caller).
     Do NOT re-declare %global here. */
  %let _corr_rc = 0;

  /* ---- 1) Validar variables numéricas --------------------------------- */
  %if %length(%superq(variables)) = 0 %then %do;
    %put ERROR: [correlacion_contract] No se proporcionaron variables numericas.;
    %let _corr_rc = 1;
    %return;
  %end;

  /* ---- 2) Validar tabla accesible y nobs > 0 -------------------------- */
  %local _corr_nobs;
  %let _corr_nobs = 0;

  proc sql noprint;
    select count(*) into :_corr_nobs trimmed
    from &input_caslib..&input_table.;
  quit;

  %if &_corr_nobs. = 0 %then %do;
    %put ERROR: [correlacion_contract] Tabla &input_caslib..&input_table. no accesible o tiene 0 obs.;
    %let _corr_rc = 1;
    %return;
  %end;

  %put NOTE: [correlacion_contract] OK — &_corr_nobs. obs, variables=&variables.;

%mend correlacion_contract;
