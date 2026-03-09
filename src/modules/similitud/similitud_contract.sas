/* =========================================================================
similitud_contract.sas - Validaciones pre-ejecucion del modulo Similitud

Verifica:
1) Al menos una lista de variables (num o cat) no vacia
2) Variable target definida y no vacia
3) Variable temporal (byvar) definida y no vacia
4) Tabla TRAIN accesible y con observaciones (nobs > 0)
5) Tabla OOT accesible y con observaciones (nobs > 0)
6) byvar presente en ambas tablas
7) target presente en ambas tablas

Setea macro variable &_simil_rc (declarada %global por similitud_run):
0 = OK, 1 = fallo (el modulo no debe ejecutarse)

Validacion de existencia: usa proc sql count(*) directo.
NO usa table.tableExists (no confiable en SAS Viya).
========================================================================= */
%macro similitud_contract( input_caslib=, train_table=, oot_table=, vars_num=,
    vars_cat=, target=, byvar=);

    %let _simil_rc = 0;

    %local _simil_nobs_trn _simil_nobs_oot _simil_has_col;

    /* ---- 1) Validar al menos una lista de variables --------------------- */
    %if %length(%superq(vars_num)) = 0 and %length(%superq(vars_cat)) = 0
    %then %do;
        %put ERROR: [similitud_contract] No se proporcionaron variables
            numericas ni categoricas.;
        %let _simil_rc = 1;
        %return;
    %end;

    /* ---- 2) Validar target definido ------------------------------------- */
    %if %length(%superq(target)) = 0 %then %do;
        %put ERROR: [similitud_contract] Variable target no definida.;
        %let _simil_rc = 1;
        %return;
    %end;

    /* ---- 3) Validar byvar no vacia -------------------------------------- */
    %if %length(%superq(byvar)) = 0 %then %do;
        %put ERROR: [similitud_contract] Variable temporal (byvar) no
            definida.;
        %let _simil_rc = 1;
        %return;
    %end;

    /* ---- 4) Validar tabla TRAIN accesible y nobs > 0 -------------------- */
    %let _simil_nobs_trn = 0;

    proc sql noprint;
        select count(*) into :_simil_nobs_trn trimmed
        from &input_caslib..&train_table.;
    quit;

    %if &_simil_nobs_trn. = 0 %then %do;
        %put ERROR: [similitud_contract] TRAIN &input_caslib..&train_table.
            no accesible o 0 obs.;
        %let _simil_rc = 1;
        %return;
    %end;

    /* ---- 5) Validar tabla OOT accesible y nobs > 0 ---------------------- */
    %let _simil_nobs_oot = 0;

    proc sql noprint;
        select count(*) into :_simil_nobs_oot trimmed
        from &input_caslib..&oot_table.;
    quit;

    %if &_simil_nobs_oot. = 0 %then %do;
        %put ERROR: [similitud_contract] OOT &input_caslib..&oot_table.
            no accesible o 0 obs.;
        %let _simil_rc = 1;
        %return;
    %end;

    /* ---- 6) Validar byvar existe en TRAIN ------------------------------- */
    %let _simil_has_col = 0;

    proc sql noprint;
        select count(*) into :_simil_has_col trimmed
        from dictionary.columns
        where upcase(libname) = upcase("&input_caslib.")
          and upcase(memname) = upcase("&train_table.")
          and upcase(name) = upcase("&byvar.");
    quit;

    %if &_simil_has_col. = 0 %then %do;
        %put ERROR: [similitud_contract] byvar=&byvar. no encontrada en TRAIN.;
        %let _simil_rc = 1;
        %return;
    %end;

    /* ---- Validar byvar existe en OOT ------------------------------------ */
    %let _simil_has_col = 0;

    proc sql noprint;
        select count(*) into :_simil_has_col trimmed
        from dictionary.columns
        where upcase(libname) = upcase("&input_caslib.")
          and upcase(memname) = upcase("&oot_table.")
          and upcase(name) = upcase("&byvar.");
    quit;

    %if &_simil_has_col. = 0 %then %do;
        %put ERROR: [similitud_contract] byvar=&byvar. no encontrada en OOT.;
        %let _simil_rc = 1;
        %return;
    %end;

    /* ---- 7) Validar target existe en TRAIN ------------------------------ */
    %let _simil_has_col = 0;

    proc sql noprint;
        select count(*) into :_simil_has_col trimmed
        from dictionary.columns
        where upcase(libname) = upcase("&input_caslib.")
          and upcase(memname) = upcase("&train_table.")
          and upcase(name) = upcase("&target.");
    quit;

    %if &_simil_has_col. = 0 %then %do;
        %put ERROR: [similitud_contract] target=&target. no encontrada en
            TRAIN.;
        %let _simil_rc = 1;
        %return;
    %end;

    /* ---- Validar target existe en OOT ----------------------------------- */
    %let _simil_has_col = 0;

    proc sql noprint;
        select count(*) into :_simil_has_col trimmed
        from dictionary.columns
        where upcase(libname) = upcase("&input_caslib.")
          and upcase(memname) = upcase("&oot_table.")
          and upcase(name) = upcase("&target.");
    quit;

    %if &_simil_has_col. = 0 %then %do;
        %put ERROR: [similitud_contract] target=&target. no encontrada en
            OOT.;
        %let _simil_rc = 1;
        %return;
    %end;

    %put NOTE: [similitud_contract] OK - TRAIN=&_simil_nobs_trn. obs,
        OOT=&_simil_nobs_oot. obs;
    %if %length(%superq(vars_num)) > 0 %then
        %put NOTE: [similitud_contract] vars_num=&vars_num.;
    %if %length(%superq(vars_cat)) > 0 %then
        %put NOTE: [similitud_contract] vars_cat=&vars_cat.;
    %put NOTE: [similitud_contract] target=&target. byvar=&byvar.;

%mend similitud_contract;
