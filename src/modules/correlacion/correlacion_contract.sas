/* =========================================================================
   correlacion_contract.sas - Validaciones pre-ejecución del módulo

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

  %put NOTE: [correlacion_contract] OK - &_corr_nobs. obs, variables=&variables.;

%mend correlacion_contract;

/* -------------------------------------------------------------------------
   Refactor override: scope-input validation with internal split derivation.
   The later macro definition takes precedence over the legacy one above.
   ------------------------------------------------------------------------- */
%macro correlacion_contract(input_caslib=, input_table=, variables=, byvar=,
  split=, train_min_mes=, train_max_mes=, oot_min_mes=, oot_max_mes=);

  %local _corr_tbl_exists _corr_nobs _corr_byvar_exists _corr_byvar_type
    _corr_var_count _corr_missing_vars _corr_i _corr_var _corr_var_exists
    _corr_split_min _corr_split_max _corr_split_nobs;

  %let _corr_rc = 0;

  %if %length(%superq(variables)) = 0 %then %do;
    %put ERROR: [correlacion_contract] No se proporcionaron variables numericas.;
    %let _corr_rc = 1;
    %return;
  %end;

  proc sql noprint;
    select count(*)
      into :_corr_tbl_exists trimmed
    from dictionary.tables
    where upcase(libname)=upcase("&input_caslib.")
      and upcase(memname)=upcase("&input_table.");
  quit;

  %if &_corr_tbl_exists. = 0 %then %do;
    %put ERROR: [correlacion_contract] Tabla &input_caslib..&input_table. no existe.;
    %let _corr_rc = 1;
    %return;
  %end;

  proc sql noprint;
    select count(*)
      into :_corr_nobs trimmed
    from &input_caslib..&input_table.;
  quit;

  %if &_corr_nobs. = 0 %then %do;
    %put ERROR: [correlacion_contract] Tabla &input_caslib..&input_table. tiene 0 obs.;
    %let _corr_rc = 1;
    %return;
  %end;

  %if %length(%superq(byvar)) = 0 %then %do;
    %put ERROR: [correlacion_contract] No se proporciono byvar.;
    %let _corr_rc = 1;
    %return;
  %end;

  proc sql noprint;
    select count(*),
           max(type)
      into :_corr_byvar_exists trimmed,
           :_corr_byvar_type trimmed
    from dictionary.columns
    where upcase(libname)=upcase("&input_caslib.")
      and upcase(memname)=upcase("&input_table.")
      and upcase(name)=upcase("&byvar.");
  quit;

  %if &_corr_byvar_exists. = 0 %then %do;
    %put ERROR: [correlacion_contract] byvar=&byvar. no existe en &input_caslib..&input_table..;
    %let _corr_rc = 1;
    %return;
  %end;

  %if %upcase(&_corr_byvar_type.) ne NUM %then %do;
    %put ERROR: [correlacion_contract] byvar=&byvar. debe ser numerica para inferir TRAIN/OOT.;
    %let _corr_rc = 1;
    %return;
  %end;

  %let _corr_var_count = 0;
  %let _corr_missing_vars =;
  %let _corr_i = 1;
  %let _corr_var = %scan(%superq(variables), &_corr_i., %str( ));

  %do %while(%length(%superq(_corr_var)) > 0);
    proc sql noprint;
      select count(*)
        into :_corr_var_exists trimmed
      from dictionary.columns
      where upcase(libname)=upcase("&input_caslib.")
        and upcase(memname)=upcase("&input_table.")
        and upcase(name)=upcase("&_corr_var.")
        and upcase(type)='NUM';
    quit;

    %if &_corr_var_exists. = 0 %then %do;
      %let _corr_missing_vars=&_corr_missing_vars. &_corr_var.;
    %end;
    %else %do;
      %let _corr_var_count=%eval(&_corr_var_count. + 1);
    %end;

    %let _corr_i=%eval(&_corr_i. + 1);
    %let _corr_var=%scan(%superq(variables), &_corr_i., %str( ));
  %end;

  %if %length(%superq(_corr_missing_vars)) > 0 %then %do;
    %put ERROR: [correlacion_contract] Variables faltantes o no numericas:&_corr_missing_vars.;
    %let _corr_rc = 1;
    %return;
  %end;

  %if &_corr_var_count. < 2 %then %do;
    %put ERROR: [correlacion_contract] Se requieren al menos 2 variables numericas para correlacion.;
    %let _corr_rc = 1;
    %return;
  %end;

  %if %upcase(&split.) = TRAIN %then %do;
    %let _corr_split_min = &train_min_mes.;
    %let _corr_split_max = &train_max_mes.;
  %end;
  %else %if %upcase(&split.) = OOT %then %do;
    %let _corr_split_min = &oot_min_mes.;
    %let _corr_split_max = &oot_max_mes.;
  %end;
  %else %do;
    %put ERROR: [correlacion_contract] split=&split. no reconocido.;
    %let _corr_rc = 1;
    %return;
  %end;

  proc sql noprint;
    select count(*)
      into :_corr_split_nobs trimmed
    from &input_caslib..&input_table.
    where &byvar. between &_corr_split_min. and &_corr_split_max.;
  quit;

  %if &_corr_split_nobs. = 0 %then %do;
    %put ERROR: [correlacion_contract] Split &split. no tiene observaciones entre &_corr_split_min. y &_corr_split_max..;
    %let _corr_rc = 1;
    %return;
  %end;

  %put NOTE: [correlacion_contract] OK - &_corr_nobs. obs en scope,
    &_corr_split_nobs. obs para split=&split., variables=&variables.;

%mend correlacion_contract;
