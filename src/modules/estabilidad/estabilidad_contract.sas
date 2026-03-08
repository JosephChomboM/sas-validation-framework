/* =========================================================================
estabilidad_contract.sas - Validaciones pre-ejecucion del modulo Estabilidad

Verifica:
1) Al menos una lista de variables (num o cat) no vacia
2) Variable temporal (byvar) no vacia
3) Tabla TRAIN accesible y con observaciones (nobs > 0)
4) Tabla OOT accesible y con observaciones (nobs > 0)
5) byvar presente en ambas tablas

Setea macro variable &_estab_rc (declarada %global por estabilidad_run):
0 = OK, 1 = fallo (el modulo no debe ejecutarse)

Validacion de existencia: usa proc sql count(*) directo.
NO usa table.tableExists (no confiable en SAS Viya).
========================================================================= */
%macro estabilidad_contract( input_caslib=, train_table=, oot_table=, vars_num=,
    vars_cat=, byvar=);

    %let _estab_rc=0;

    %local _estab_nobs_trn _estab_nobs_oot _estab_has_col;

    /* ---- 1) Validar al menos una lista de variables --------------------- */
    %if %length(%superq(vars_num))=0 and %length(%superq(vars_cat))=0 %then %do;
        %put ERROR: [estabilidad_contract] No se proporcionaron variables
            numericas ni categoricas.;
        %let _estab_rc=1;
        %return;
    %end;

    /* ---- 2) Validar byvar no vacia -------------------------------------- */
    %if %length(%superq(byvar))=0 %then %do;
        %put ERROR: [estabilidad_contract] Variable temporal (byvar) no
            definida.;
        %let _estab_rc=1;
        %return;
    %end;

    /* ---- 3) Validar tabla TRAIN accesible y nobs > 0 -------------------- */
    %let _estab_nobs_trn=0;

    proc sql noprint;
        select count(*) into :_estab_nobs_trn trimmed from
            &input_caslib..&train_table.;
    quit;

    %if &_estab_nobs_trn.=0 %then %do;
        %put ERROR: [estabilidad_contract] TRAIN &input_caslib..&train_table. no
            accesible o 0 obs.;
        %let _estab_rc=1;
        %return;
    %end;

    /* ---- 4) Validar tabla OOT accesible y nobs > 0 ---------------------- */
    %let _estab_nobs_oot=0;

    proc sql noprint;
        select count(*) into :_estab_nobs_oot trimmed from
            &input_caslib..&oot_table.;
    quit;

    %if &_estab_nobs_oot.=0 %then %do;
        %put ERROR: [estabilidad_contract] OOT &input_caslib..&oot_table. no
            accesible o 0 obs.;
        %let _estab_rc=1;
        %return;
    %end;

    /* ---- 5) Validar byvar existe en ambas tablas ------------------------ */
    %let _estab_has_col=0;

    proc sql noprint;
        select count(*) into :_estab_has_col trimmed from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.") and upcase(memname)=
            upcase("&train_table.") and upcase(name)=upcase("&byvar.");
    quit;

    %if &_estab_has_col.=0 %then %do;
        %put ERROR: [estabilidad_contract] byvar=&byvar. no encontrada en
            TRAIN.;
        %let _estab_rc=1;
        %return;
    %end;

    %let _estab_has_col=0;

    proc sql noprint;
        select count(*) into :_estab_has_col trimmed from dictionary.columns
            where upcase(libname)=upcase("&input_caslib.") and upcase(memname)=
            upcase("&oot_table.") and upcase(name)=upcase("&byvar.");
    quit;

    %if &_estab_has_col.=0 %then %do;
        %put ERROR: [estabilidad_contract] byvar=&byvar. no encontrada en OOT.;
        %let _estab_rc=1;
        %return;
    %end;

    %put NOTE: [estabilidad_contract] OK - TRAIN=&_estab_nobs_trn. obs,
        OOT=&_estab_nobs_oot. obs;
    %if %length(%superq(vars_num)) > 0 %then %put NOTE: [estabilidad_contract]
        vars_num=&vars_num.;
    %if %length(%superq(vars_cat)) > 0 %then %put NOTE: [estabilidad_contract]
        vars_cat=&vars_cat.;
    %put NOTE: [estabilidad_contract] byvar=&byvar.;

%mend estabilidad_contract;
