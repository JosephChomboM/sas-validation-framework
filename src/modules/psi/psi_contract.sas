/* =========================================================================
   psi_contract.sas - Validaciones pre-ejecución del módulo PSI

   Verifica:
     1) Tabla TRAIN accesible y con nobs > 0
     2) Tabla OOT   accesible y con nobs > 0
     3) Al menos una lista de variables (num o cat) no vacía
     4) Variable temporal (byvar) existe en ambas tablas (si se proporcionó)

   Convención de retorno:
     _psi_rc = 0 → OK  |  _psi_rc = 1 → falla (caller debe abortar)
     _psi_rc es %global, declarada por psi_run.sas (el caller).

   Método de validación: proc sql count(*) / dictionary.columns.
   NO usar table.tableExists (no confiable en SAS Viya).
   ========================================================================= */

%macro psi_contract(
    input_caslib =,
    train_table  =,
    oot_table    =,
    byvar        =,
    var_num      =,
    var_cat      =
);

  %local _n_train _n_oot;

  %put NOTE: [psi_contract] Validando inputs...;
  %put NOTE: [psi_contract]   TRAIN = &input_caslib..&train_table.;
  %put NOTE: [psi_contract]   OOT   = &input_caslib..&oot_table.;

  /* ================================================================
     1) Tabla TRAIN accesible y no vacía
     ================================================================ */
  %let _n_train = 0;
  proc sql noprint;
    select count(*) into :_n_train trimmed
    from &input_caslib..&train_table.;
  quit;

  %if &_n_train. = 0 %then %do;
    %put ERROR: [psi_contract] Tabla TRAIN &input_caslib..&train_table. no existe o está vacía (nobs=0).;
    %let _psi_rc = 1;
    %return;
  %end;

  /* ================================================================
     2) Tabla OOT accesible y no vacía
     ================================================================ */
  %let _n_oot = 0;
  proc sql noprint;
    select count(*) into :_n_oot trimmed
    from &input_caslib..&oot_table.;
  quit;

  %if &_n_oot. = 0 %then %do;
    %put ERROR: [psi_contract] Tabla OOT &input_caslib..&oot_table. no existe o está vacía (nobs=0).;
    %let _psi_rc = 1;
    %return;
  %end;

  /* ================================================================
     3) Al menos una lista de variables no vacía
     ================================================================ */
  %if %length(%superq(var_num)) = 0 and %length(%superq(var_cat)) = 0 %then %do;
    %put ERROR: [psi_contract] No hay variables numéricas ni categóricas para PSI.;
    %let _psi_rc = 1;
    %return;
  %end;

  /* ================================================================
     4) Variable temporal (byvar) existe en ambas tablas si se proporcionó
     ================================================================ */
  %if %length(%superq(byvar)) > 0 %then %do;
    %local _byvar_ok_tr _byvar_ok_ot;
    %let _byvar_ok_tr = 0;
    %let _byvar_ok_ot = 0;

    proc sql noprint;
      select count(*) into :_byvar_ok_tr trimmed
      from dictionary.columns
      where upcase(libname) = upcase("&input_caslib.")
        and upcase(memname) = upcase("&train_table.")
        and upcase(name)    = upcase("&byvar.");

      select count(*) into :_byvar_ok_ot trimmed
      from dictionary.columns
      where upcase(libname) = upcase("&input_caslib.")
        and upcase(memname) = upcase("&oot_table.")
        and upcase(name)    = upcase("&byvar.");
    quit;

    %if &_byvar_ok_tr. = 0 or &_byvar_ok_ot. = 0 %then %do;
      %put ERROR: [psi_contract] Variable temporal "&byvar." no encontrada en ambas tablas.;
      %let _psi_rc = 1;
      %return;
    %end;
  %end;

  %put NOTE: [psi_contract] Validación OK - TRAIN=&_n_train. obs | OOT=&_n_oot. obs;

%mend psi_contract;
