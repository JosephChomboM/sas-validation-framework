/* =========================================================================
   correlacion_contract.sas — Validaciones pre-ejecución del módulo

   Verifica:
     1) Tabla input existe en CASLIB
     2) Tabla tiene observaciones (nobs > 0)
     3) Lista de variables numéricas no está vacía

   Setea macro variable global &_corr_rc:
     0 = OK, 1 = fallo (el módulo no debe ejecutarse)
   ========================================================================= */

%macro correlacion_contract(input_caslib=, input_table=, variables=);

  %global _corr_rc;
  %let _corr_rc = 0;

  /* ---- 1) Validar existencia de la tabla en CASLIB -------------------- */
  %local _tbl_exists;
  %let _tbl_exists = 0;

  proc cas;
    session conn;
    table.tableExists result=_r / caslib="&input_caslib." name="&input_table.";
    call symputx('_tbl_exists', _r.exists);
  quit;

  %if &_tbl_exists. = 0 %then %do;
    %put ERROR: [correlacion_contract] Tabla &input_caslib..&input_table. no existe.;
    %let _corr_rc = 1;
    %return;
  %end;

  /* ---- 2) Validar nobs > 0 ------------------------------------------- */
  %local _corr_nobs;
  proc sql noprint;
    select count(*) into :_corr_nobs trimmed
    from &input_caslib..&input_table.;
  quit;

  %if &_corr_nobs. = 0 %then %do;
    %put ERROR: [correlacion_contract] Tabla &input_caslib..&input_table. tiene 0 observaciones.;
    %let _corr_rc = 1;
    %return;
  %end;

  /* ---- 3) Validar que haya variables numéricas ------------------------ */
  %if %length(%superq(variables)) = 0 %then %do;
    %put ERROR: [correlacion_contract] No se proporcionaron variables numericas.;
    %let _corr_rc = 1;
    %return;
  %end;

  %put NOTE: [correlacion_contract] OK — &_corr_nobs. obs, variables=&variables.;

%mend correlacion_contract;
